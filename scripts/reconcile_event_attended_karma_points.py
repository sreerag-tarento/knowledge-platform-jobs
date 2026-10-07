#!/usr/bin/env python3
"""
Manual reconciliation for EVENT_ATTENDED (webinar) karma points awarded by
karma-points-processor-v2 at the old 5-point rate, bumping them to the
current 10-point rate.

This script ONLY touches user_karma_points (the per-row points value).
user_karma_points_summary is no longer updated here - instead, for every row
this script updates, it writes a ready-to-publish KARMA_POINTS_ADJUSTMENT
event (one JSON line per row) that you feed directly onto the
karma.points.unified.v2.event Kafka topic. karma-points-processor-v2's
KarmaPointsAdjustmentHandler consumes that event and increments
user_karma_points_summary.total_points (+ mirrors Redis) by the event's
`data.points` delta - no idempotency check on the Flink side, so each event
must be fed onto the topic exactly once.

For each (userid, context_id) pair in the input CSV:
  - Reads user_karma_points filtered to context_type='EVENT',
    operation_type='EVENT_ATTENDED', context_id=<context_id> for that user
    (partition read on userid, since that's the table's partition key;
    credit_date/context_type/operation_type/context_id are clustering
    columns, so an exact row needs all of them - we get there by filtering
    the partition instead of guessing credit_date).
  - No matching row -> logged + appended to the "not found" CSV.
  - Row found but points != 5 -> NOT touched, appended to the "mismatch" CSV
    with full row details (someone/something already changed it, needs a
    human to look, not this script).
  - More than one row matches userid+context_id (same context_id reused
    under a different context_type/operation_type - shouldn't happen for
    this event type, but we don't assume) -> appended to an "ambiguous" CSV,
    never touched.
  - Row found with points == 5 -> updated in place to 10 (user_karma_points
    only) and a KARMA_POINTS_ADJUSTMENT event payload (delta +5) is written
    to the adjustment-events JSONL output. Logged to the "updated" CSV.

user_karma_points_credit_lookup is intentionally NOT touched by this script:
it only records that a credit happened (existence + credit_date) for the
idempotency gate, not the point value, so a point-value correction has
nothing to update there.

Dry run by default - only --execute --yes actually writes to Cassandra.

Usage:
    pip install cassandra-driver

    export CASSANDRA_HOST=localhost
    export CASSANDRA_PORT=9042
    export CASSANDRA_KEYSPACE=sunbird
    # export CASSANDRA_USERNAME=... CASSANDRA_PASSWORD=...   # if auth is enabled

    # Dry run (default) - just reports what would change, no writes
    python reconcile_event_attended_karma_points.py --input pairs.csv

    # Live run - actually updates user_karma_points, and writes the
    # adjustment events JSONL for you to feed onto the Kafka topic
    python reconcile_event_attended_karma_points.py --input pairs.csv --execute --yes

Input CSV must have a header row with columns: userid,context_id
"""

import argparse
import csv
import json
import logging
import os
import sys
from datetime import datetime, timezone

try:
    from cassandra.cluster import Cluster
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.policies import WhiteListRoundRobinPolicy
    from cassandra.query import SimpleStatement, ConsistencyLevel
except ImportError:
    sys.exit("Missing dependency. Run: pip install cassandra-driver")

CONTEXT_TYPE_EVENT = "EVENT"
OPERATION_TYPE_EVENT = "EVENT_ATTENDED"
EXPECTED_CURRENT_POINTS = 5
NEW_POINTS = 10
POINTS_DELTA = NEW_POINTS - EXPECTED_CURRENT_POINTS

EVENT_TYPE_KARMA_POINTS_ADJUSTMENT = "KARMA_POINTS_ADJUSTMENT"

KARMA_POINTS_TABLE = "user_karma_points"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("reconcile_event_attended_karma_points")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", required=True, help="CSV with columns userid,context_id")
    p.add_argument("--execute", action="store_true",
                    help="Actually write to Cassandra. Without this, dry run only.")
    p.add_argument("--yes", action="store_true",
                    help="Required together with --execute to confirm a live run.")
    p.add_argument("--output-dir", default=".", help="Directory to write report CSVs into")
    return p.parse_args()


def get_env(name, default=None, required=False):
    value = os.environ.get(name, default)
    if required and not value:
        sys.exit(f"Missing required environment variable: {name}")
    return value


def connect_cassandra():
    host = get_env("CASSANDRA_HOST", "localhost")
    port = int(get_env("CASSANDRA_PORT", "9042"))
    keyspace = get_env("CASSANDRA_KEYSPACE", "sunbird")
    username = get_env("CASSANDRA_USERNAME")
    password = get_env("CASSANDRA_PASSWORD")

    auth_provider = PlainTextAuthProvider(username=username, password=password) if username else None
    # A local/dev Cassandra is often a single-node ring behind a pool/proxy whose
    # system.peers lists other node IPs that aren't reachable from here - letting
    # the driver "discover" and dial those hangs the control connection
    # (NoHostAvailable/OperationTimedOut). Pinning the load-balancing policy to
    # only this one contact point avoids that entirely.
    cluster = Cluster([host], port=port, auth_provider=auth_provider,
                       load_balancing_policy=WhiteListRoundRobinPolicy([host]))
    session = cluster.connect(keyspace)
    logger.info(f"Connected to Cassandra {host}:{port} keyspace={keyspace}")
    return cluster, session


def read_pairs(path):
    pairs = []
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        if "userid" not in reader.fieldnames or "context_id" not in reader.fieldnames:
            sys.exit(f"Input CSV must have 'userid' and 'context_id' columns, found: {reader.fieldnames}")
        for row in reader:
            userid = row["userid"].strip()
            context_id = row["context_id"].strip()
            if userid and context_id:
                pairs.append((userid, context_id))
    return pairs


def find_matching_rows(session, userid, context_id):
    """Partition read on userid (the table's partition key), filtered in
    Python to this context_id + the EVENT/EVENT_ATTENDED event type - this
    repo's own code does the same partition-then-filter pattern
    (CassandraUtil.hasEarnedFirstEnrolmentPoints) rather than guessing the
    rest of the clustering key."""
    query = SimpleStatement(
        f"SELECT userid, credit_date, context_type, operation_type, context_id, addinfo, points "
        f"FROM {KARMA_POINTS_TABLE} WHERE userid = %s",
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    )
    rows = list(session.execute(query, (userid,)))
    return [
        row for row in rows
        if row.context_id == context_id
        and row.context_type == CONTEXT_TYPE_EVENT
        and row.operation_type == OPERATION_TYPE_EVENT
    ]


def update_points_row(session, row, new_points):
    query = SimpleStatement(
        f"UPDATE {KARMA_POINTS_TABLE} SET points = %s "
        f"WHERE userid = %s AND credit_date = %s AND context_type = %s "
        f"AND operation_type = %s AND context_id = %s",
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
    )
    session.execute(query, (new_points, row.userid, row.credit_date, row.context_type,
                             row.operation_type, row.context_id))


def build_adjustment_event(userid, delta, ets_millis):
    """KARMA_POINTS_ADJUSTMENT event for karma-points-processor-v2's
    KarmaPointsAdjustmentHandler - no idempotency check on the Flink side, so
    this must be fed onto the topic exactly once per row."""
    return {
        "eventType": EVENT_TYPE_KARMA_POINTS_ADJUSTMENT,
        "userId": userid,
        "ets": ets_millis,
        "data": {
            "user_id": userid,
            "points": delta,
        },
    }


def main():
    args = parse_args()
    if args.execute and not args.yes:
        sys.exit("--execute requires --yes to confirm a live write run.")

    mode = "LIVE" if args.execute else "DRY-RUN"
    logger.info(f"Mode: {mode}")

    pairs = read_pairs(args.input)
    logger.info(f"Loaded {len(pairs)} (userid, context_id) pairs from {args.input}")

    cluster, session = connect_cassandra()

    not_found, mismatched, ambiguous, updated, adjustment_events = [], [], [], [], []

    for userid, context_id in pairs:
        matches = find_matching_rows(session, userid, context_id)

        if len(matches) == 0:
            logger.warning(f"Not found: userid={userid} context_id={context_id}")
            not_found.append({"userid": userid, "context_id": context_id})
            continue

        if len(matches) > 1:
            logger.warning(f"Ambiguous: {len(matches)} rows match userid={userid} context_id={context_id}")
            for row in matches:
                ambiguous.append({
                    "userid": userid, "context_id": context_id,
                    "credit_date": row.credit_date, "context_type": row.context_type,
                    "operation_type": row.operation_type, "points": row.points, "addinfo": row.addinfo,
                })
            continue

        row = matches[0]
        if row.points != EXPECTED_CURRENT_POINTS:
            logger.info(f"Mismatch: userid={userid} context_id={context_id} points={row.points} (expected {EXPECTED_CURRENT_POINTS})")
            mismatched.append({
                "userid": userid, "context_id": context_id, "credit_date": row.credit_date,
                "context_type": row.context_type, "operation_type": row.operation_type,
                "points": row.points, "addinfo": row.addinfo,
            })
            continue

        logger.info(f"{'Updating' if args.execute else 'Would update'}: userid={userid} context_id={context_id} points {row.points} -> {NEW_POINTS}")
        ets_millis = int(datetime.now(timezone.utc).timestamp() * 1000)
        updated.append({
            "userid": userid, "context_id": context_id, "credit_date": row.credit_date,
            "old_points": row.points, "new_points": NEW_POINTS,
        })
        adjustment_events.append(build_adjustment_event(userid, POINTS_DELTA, ets_millis))

        if args.execute:
            update_points_row(session, row, NEW_POINTS)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = "live" if args.execute else "dryrun"

    def write_csv(name, rows, fieldnames):
        path = os.path.join(args.output_dir, f"{name}_{suffix}_{ts}.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"Wrote {len(rows)} rows to {path}")

    write_csv("not_found", not_found, ["userid", "context_id"])
    write_csv("mismatched", mismatched,
               ["userid", "context_id", "credit_date", "context_type", "operation_type", "points", "addinfo"])
    write_csv("ambiguous", ambiguous,
               ["userid", "context_id", "credit_date", "context_type", "operation_type", "points", "addinfo"])
    write_csv("updated", updated, ["userid", "context_id", "credit_date", "old_points", "new_points"])

    events_path = os.path.join(args.output_dir, f"adjustment_events_{suffix}_{ts}.jsonl")
    with open(events_path, "w") as f:
        for event in adjustment_events:
            f.write(json.dumps(event) + "\n")
    logger.info(f"Wrote {len(adjustment_events)} KARMA_POINTS_ADJUSTMENT event(s) to {events_path}")

    cluster.shutdown()

    print()
    print(f"Mode: {mode}")
    print(f"  total pairs processed  : {len(pairs)}")
    print(f"  not found              : {len(not_found)}")
    print(f"  mismatched (untouched) : {len(mismatched)}")
    print(f"  ambiguous (untouched)  : {len(ambiguous)}")
    print(f"  updated{'  ' if args.execute else ' (would be)'}        : {len(updated)}")
    if not args.execute:
        print("\nThis was a DRY RUN. Re-run with --execute --yes to actually write.")
    print(f"\n{len(adjustment_events)} KARMA_POINTS_ADJUSTMENT event(s) written to {events_path}")
    print("Feed these onto the karma.points.unified.v2.event Kafka topic to apply the "
          "corresponding user_karma_points_summary increments.")


if __name__ == "__main__":
    main()
