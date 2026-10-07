#!/usr/bin/env python3
"""
Scans the program-activity-aggregate-updater course-info Redis cache
(default: DB 7) for cached course entries whose "primaryCategory" is
Course/Standalone Assessment but whose "parentCollections" field is
missing or empty, and deletes them so the next lookup falls through to
content-service and re-populates with current data.

Root cause reference: ContentHelper.getCourseInfo serializes with
Jackson's NON_EMPTY inclusion, so a course cached before being linked
to its Program has "parentCollections" silently dropped from the
cached JSON and stays that way until the (very long, observed
150-300+ day) TTL expires.

Usage:
    pip install redis

    # Dry run (default) - writes the list of doIds that WOULD be deleted
    python find_and_clean_stale_parentcollections_cache.py \
        --host 10.175.5.166 --port 6379 --db 7

    # Live run - actually deletes them, writes the list of doIds that WERE deleted
    python find_and_clean_stale_parentcollections_cache.py \
        --host 10.175.5.166 --port 6379 --db 7 --execute --yes
"""

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone

try:
    import redis
except ImportError:
    sys.exit("Missing dependency. Run: pip install redis")


CATEGORIES_REQUIRING_PARENT_COLLECTIONS = {"course", "standalone assessment"}


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--host", required=True, help="Redis host, e.g. 10.175.5.166")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=7)
    p.add_argument("--password", default=None)
    p.add_argument("--pattern", default="do_*", help="SCAN MATCH pattern (default: do_*)")
    p.add_argument("--scan-count", type=int, default=500, help="SCAN COUNT hint per iteration")
    p.add_argument("--categories", default=",".join(sorted(CATEGORIES_REQUIRING_PARENT_COLLECTIONS)),
                    help="Comma-separated primaryCategory values to target (case-insensitive)")
    p.add_argument("--execute", action="store_true",
                    help="Actually delete matched keys. Without this flag, the script only reports (dry run).")
    p.add_argument("--yes", action="store_true",
                    help="Required together with --execute to confirm a live delete run.")
    p.add_argument("--limit", type=int, default=0,
                    help="Stop after deleting this many keys (0 = no limit). Safety valve for first live run.")
    p.add_argument("--delete-batch-size", type=int, default=200,
                    help="How many DELs to pipeline together at a time")
    p.add_argument("--get-batch-size", type=int, default=300,
                    help="How many GETs to pipeline together per round-trip (bigger = fewer round-trips, "
                         "important when connecting over a slow port-forward)")
    p.add_argument("--progress-every", type=int, default=1000,
                    help="Print a progress line every N scanned keys")
    p.add_argument("--heartbeat-seconds", type=float, default=5.0,
                    help="Also print a progress line at least this often, regardless of key count")
    p.add_argument("--output", default=None,
                    help="Output CSV path. Defaults to a timestamped file in the current directory.")
    return p.parse_args()


def is_target_entry(raw_value, categories):
    """Return (is_match, reason, primary_category) for a cached JSON string value."""
    try:
        data = json.loads(raw_value)
    except (json.JSONDecodeError, TypeError):
        return False, "malformed_json", None

    if not isinstance(data, dict):
        return False, "not_a_json_object", None

    primary_category = (data.get("primaryCategory") or data.get("primarycategory") or "").strip()
    if primary_category.lower() not in categories:
        return False, "category_not_targeted", primary_category

    parent_collections = data.get("parentCollections", data.get("parentcollections", None))
    if parent_collections is None:
        return True, "parentCollections_key_missing", primary_category
    if isinstance(parent_collections, list) and len(parent_collections) == 0:
        return True, "parentCollections_empty_list", primary_category

    return False, "has_parentCollections", primary_category


def main():
    args = parse_args()
    categories = {c.strip().lower() for c in args.categories.split(",") if c.strip()}

    if args.execute and not args.yes:
        sys.exit("Refusing to run live delete without --yes (safety guard). "
                  "Run with --execute --yes to actually delete matched keys.")

    mode = "LIVE-DELETE" if args.execute else "DRY-RUN"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = args.output or f"stale_parentcollections_{'deleted' if args.execute else 'dryrun'}_{ts}.csv"

    r = redis.Redis(host=args.host, port=args.port, db=args.db, password=args.password,
                     decode_responses=True, socket_timeout=10)
    r.ping()
    print(f"[{mode}] host={args.host} port={args.port} db={args.db} pattern={args.pattern} "
          f"categories={sorted(categories)}", flush=True)
    print("  connected, starting SCAN ...", flush=True)

    matched = []           # rows to write to CSV
    to_delete_batch = []   # keys queued for the current delete pipeline
    get_batch = []         # keys queued for the current pipelined GET round-trip
    scanned = 0
    deleted_count = 0
    stop_early = False
    start = time.time()
    last_progress_at = start

    def maybe_print_progress(force=False):
        nonlocal last_progress_at
        now = time.time()
        if force or (scanned % args.progress_every == 0) or (now - last_progress_at >= args.heartbeat_seconds):
            print(f"  scanned={scanned} matched_so_far={len(matched)} elapsed={now-start:.0f}s", flush=True)
            last_progress_at = now

    def flush_delete_batch():
        nonlocal deleted_count
        if not to_delete_batch:
            return
        if args.execute:
            pipe = r.pipeline()
            for k in to_delete_batch:
                pipe.delete(k)
            pipe.execute()
            deleted_count += len(to_delete_batch)
        to_delete_batch.clear()

    def process_get_batch():
        """One pipelined GET round-trip for a whole batch of keys instead of one round-trip per key."""
        nonlocal stop_early
        if not get_batch:
            return
        pipe = r.pipeline(transaction=False)
        for k in get_batch:
            pipe.get(k)
        results = pipe.execute(raise_on_error=False)

        for key, value in zip(get_batch, results):
            if isinstance(value, Exception):
                # e.g. WRONGTYPE - this cache is expected to hold plain strings only; skip defensively
                continue
            if value is None:
                continue

            is_match, reason, primary_category = is_target_entry(value, categories)
            if not is_match:
                continue

            try:
                ttl = r.ttl(key)
            except redis.exceptions.RedisError:
                ttl = ""

            matched.append({"doId": key, "reason": reason, "primaryCategory": primary_category, "ttl": ttl, "error": ""})

            if args.execute:
                to_delete_batch.append(key)
                if len(to_delete_batch) >= args.delete_batch_size:
                    flush_delete_batch()

            if args.limit and (deleted_count + len(to_delete_batch)) >= args.limit:
                stop_early = True

        get_batch.clear()

    for key in r.scan_iter(match=args.pattern, count=args.scan_count):
        scanned += 1
        maybe_print_progress()

        get_batch.append(key)
        if len(get_batch) >= args.get_batch_size:
            process_get_batch()

        if stop_early:
            print(f"  hit --limit={args.limit}, stopping scan early", flush=True)
            break

    if not stop_early:
        process_get_batch()  # flush any remainder shorter than a full batch
    flush_delete_batch()
    maybe_print_progress(force=True)

    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["doId", "reason", "primaryCategory", "ttl", "error"])
        writer.writeheader()
        writer.writerows(matched)

    elapsed = time.time() - start
    print(f"\n[{mode}] done in {elapsed:.0f}s", flush=True)
    print(f"  keys scanned:        {scanned}", flush=True)
    print(f"  keys matched:        {len(matched)}", flush=True)
    if args.execute:
        print(f"  keys deleted:        {deleted_count}", flush=True)
    else:
        print(f"  keys that WOULD be deleted (re-run with --execute --yes): {len(matched)}", flush=True)
    print(f"  report written to:   {output_path}", flush=True)


if __name__ == "__main__":
    main()
