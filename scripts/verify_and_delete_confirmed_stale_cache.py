#!/usr/bin/env python3
"""
Takes the CSV produced by find_and_clean_stale_parentcollections_cache.py
(dry-run output), and for every doId in it, calls the content-service
read API to check whether that content ACTUALLY has parentCollections
at the source of truth right now.

  - If the API response has a non-empty "parentCollections" -> this course
    IS genuinely linked to a program, but the Redis cache entry lacked it.
    Confirmed stale -> safe to delete from Redis.
  - If the API response has no/empty "parentCollections" -> this course
    genuinely has no parent program. NOT a bug, NOT deleted.
  - If the API call fails / response is malformed -> treated as "unknown",
    NEVER deleted (fail safe).

Always writes a full verification report CSV. Redis deletion only
happens for confirmed cases, and only when --execute --yes is passed
(same dry-run-by-default safety pattern as the scan script).

Usage:
    pip install requests redis

    # Dry run (default) - just verifies against the API and reports, no Redis changes
    python verify_and_delete_confirmed_stale_cache.py \
        --input stale_parentcollections_dryrun_20260915T000000Z.csv \
        --api-base https://portal.igotkarmayogi.gov.in/api/content/v1/read

    # Live run - verifies AND deletes confirmed-stale keys from Redis
    python verify_and_delete_confirmed_stale_cache.py \
        --input stale_parentcollections_dryrun_20260915T000000Z.csv \
        --api-base https://portal.igotkarmayogi.gov.in/api/content/v1/read \
        --redis-host 10.175.5.166 --redis-port 6379 --redis-db 7 \
        --execute --yes
"""

import argparse
import csv
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

try:
    import requests
except ImportError:
    sys.exit("Missing dependency. Run: pip install requests")


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", required=True, help="CSV from the scan script (must have a 'doId' column)")
    p.add_argument("--api-base", required=True,
                   help="Content read API base, e.g. https://portal.igotkarmayogi.gov.in/api/content/v1/read "
                        "(doId is appended as /<doId>)")
    p.add_argument("--header", action="append", default=[],
                   help='Extra HTTP header as "Name: Value". Repeatable, e.g. '
                        '--header "Authorization: Bearer xyz"')
    p.add_argument("--timeout", type=float, default=15.0, help="Per-request timeout in seconds")
    p.add_argument("--workers", type=int, default=10, help="Number of parallel API requests")
    p.add_argument("--retries", type=int, default=2, help="Retries per doId on network error/timeout")
    p.add_argument("--insecure", action="store_true", help="Skip TLS certificate verification")

    p.add_argument("--redis-host", default=None, help="Required if using --execute")
    p.add_argument("--redis-port", type=int, default=6379)
    p.add_argument("--redis-db", type=int, default=7)
    p.add_argument("--redis-password", default=None)

    p.add_argument("--execute", action="store_true",
                    help="Actually delete confirmed-stale keys from Redis. Without this, dry run only.")
    p.add_argument("--yes", action="store_true",
                    help="Required together with --execute to confirm a live delete run.")
    p.add_argument("--delete-batch-size", type=int, default=200)
    p.add_argument("--output", default=None, help="Output CSV path (defaults to a timestamped file)")
    return p.parse_args()


def parse_headers(header_args):
    headers = {}
    for h in header_args:
        if ":" not in h:
            sys.exit(f"Invalid --header value (expected 'Name: Value'): {h}")
        name, value = h.split(":", 1)
        headers[name.strip()] = value.strip()
    return headers


def read_doids(input_path):
    doids = []
    seen = set()
    with open(input_path, newline="") as f:
        reader = csv.DictReader(f)
        if "doId" not in (reader.fieldnames or []):
            sys.exit(f"Input CSV {input_path} has no 'doId' column. Columns found: {reader.fieldnames}")
        for row in reader:
            doid = (row.get("doId") or "").strip()
            if doid and doid not in seen:
                seen.add(doid)
                doids.append(doid)
    return doids


def check_one(session, api_base, doid, headers, timeout, verify_tls, retries):
    """Returns a dict describing what the API says about this doId's parentCollections."""
    url = f"{api_base.rstrip('/')}/{doid}"
    last_error = None

    for attempt in range(retries + 1):
        try:
            resp = session.get(url, headers=headers, timeout=timeout, verify=verify_tls)
        except requests.RequestException as e:
            last_error = str(e)
            continue

        if resp.status_code != 200:
            last_error = f"http_{resp.status_code}: {resp.text[:200]}"
            continue

        try:
            body = resp.json()
        except ValueError as e:
            last_error = f"invalid_json: {e}"
            continue

        content = (body or {}).get("result", {}).get("content", {})
        if not isinstance(content, dict):
            last_error = "missing_result.content_in_response"
            continue

        parent_collections = content.get("parentCollections") or []
        has_parent = isinstance(parent_collections, list) and len(parent_collections) > 0

        return {
            "doId": doid,
            "api_status": "ok",
            "primaryCategory_api": content.get("primaryCategory", ""),
            "parentCollections_api": ";".join(parent_collections) if has_parent else "",
            "has_parentCollections_at_source": has_parent,
            "decision": "delete_confirmed_stale" if has_parent else "skip_genuinely_no_parent",
            "error": "",
        }

    # exhausted retries without a usable response -> never delete on uncertain state
    return {
        "doId": doid,
        "api_status": "error",
        "primaryCategory_api": "",
        "parentCollections_api": "",
        "has_parentCollections_at_source": "",
        "decision": "skip_api_error",
        "error": last_error or "unknown_error",
    }


def main():
    args = parse_args()

    if args.execute and not args.yes:
        sys.exit("Refusing to run live delete without --yes (safety guard). "
                  "Run with --execute --yes to actually delete confirmed-stale keys.")
    if args.execute and not args.redis_host:
        sys.exit("--execute requires --redis-host")

    headers = parse_headers(args.header)
    verify_tls = not args.insecure
    doids = read_doids(args.input)
    if not doids:
        sys.exit(f"No doIds found in {args.input}")

    mode = "LIVE-DELETE" if args.execute else "DRY-RUN"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_path = args.output or f"verify_report_{'deleted' if args.execute else 'dryrun'}_{ts}.csv"

    print(f"[{mode}] verifying {len(doids)} doIds against {args.api_base} "
          f"with {args.workers} workers ...", flush=True)

    results = []
    start = time.time()
    session = requests.Session()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(check_one, session, args.api_base, doid, headers, args.timeout, verify_tls, args.retries): doid
            for doid in doids
        }
        done_count = 0
        for future in as_completed(futures):
            results.append(future.result())
            done_count += 1
            if done_count % 50 == 0 or done_count == len(doids):
                print(f"  verified {done_count}/{len(doids)} elapsed={time.time()-start:.0f}s", flush=True)

    # keep output order stable / matching the input order
    order = {doid: i for i, doid in enumerate(doids)}
    results.sort(key=lambda r: order[r["doId"]])

    confirmed = [r for r in results if r["decision"] == "delete_confirmed_stale"]
    skipped_no_parent = [r for r in results if r["decision"] == "skip_genuinely_no_parent"]
    skipped_errors = [r for r in results if r["decision"] == "skip_api_error"]

    deleted_count = 0
    if args.execute and confirmed:
        try:
            import redis
        except ImportError:
            sys.exit("Missing dependency. Run: pip install redis")

        r = redis.Redis(host=args.redis_host, port=args.redis_port, db=args.redis_db,
                         password=args.redis_password, decode_responses=True, socket_timeout=10)
        r.ping()

        keys = [row["doId"] for row in confirmed]
        for i in range(0, len(keys), args.delete_batch_size):
            batch = keys[i:i + args.delete_batch_size]
            pipe = r.pipeline()
            for k in batch:
                pipe.delete(k)
            pipe.execute()
            deleted_count += len(batch)
        for row in confirmed:
            row["redis_deleted"] = True
    else:
        for row in confirmed:
            row["redis_deleted"] = False
    for row in skipped_no_parent + skipped_errors:
        row["redis_deleted"] = False

    fieldnames = ["doId", "api_status", "primaryCategory_api", "parentCollections_api",
                  "has_parentCollections_at_source", "decision", "redis_deleted", "error"]
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    elapsed = time.time() - start
    print(f"\n[{mode}] done in {elapsed:.0f}s", flush=True)
    print(f"  total verified:                 {len(results)}", flush=True)
    print(f"  confirmed stale (has parent):   {len(confirmed)}", flush=True)
    print(f"  genuinely no parent (skipped):  {len(skipped_no_parent)}", flush=True)
    print(f"  api errors (skipped, unsure):   {len(skipped_errors)}", flush=True)
    if args.execute:
        print(f"  keys actually deleted:          {deleted_count}", flush=True)
    else:
        print(f"  keys that WOULD be deleted (re-run with --execute --yes --redis-host ...): {len(confirmed)}",
              flush=True)
    print(f"  report written to:              {output_path}", flush=True)


if __name__ == "__main__":
    main()
