"""
Statistics collector for MapReduce inverted index.

Connects to Redis and polls job progress, then collects final metrics.
Can run as a standalone pod or locally.

Usage:
  python stats.py                    # poll until done
  python stats.py --once             # print current state and exit
  python stats.py --final            # only print final report (wait for completion)
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta

import redis

from config import (
    REDIS_HOST, REDIS_PORT, MAP_QUEUE, REDUCE_QUEUE,
    MAP_DONE_COUNTER, REDUCE_DONE_COUNTER, TOTAL_MAP_TASKS,
    TOTAL_REDUCE_TASKS, STATS_KEY, PHASE_KEY, INTERMEDIATE_DIR, OUTPUT_DIR,
)


def get_phase(r: redis.Redis) -> str:
    return r.get(PHASE_KEY) or "waiting"


def get_progress(r: redis.Redis) -> dict:
    phase = get_phase(r)
    total_map = int(r.get(TOTAL_MAP_TASKS) or 0)
    total_reduce = int(r.get(TOTAL_REDUCE_TASKS) or 0)
    done_map = int(r.get(MAP_DONE_COUNTER) or 0)
    done_reduce = int(r.get(REDUCE_DONE_COUNTER) or 0)
    queue_map = r.llen(MAP_QUEUE)
    queue_reduce = r.llen(REDUCE_QUEUE)

    return {
        "phase": phase,
        "map": {"done": done_map, "total": total_map, "queued": queue_map},
        "reduce": {"done": done_reduce, "total": total_reduce, "queued": queue_reduce},
    }


def get_worker_stats(r: redis.Redis) -> dict:
    raw = r.hgetall(STATS_KEY)
    map_stats = []
    reduce_stats = []

    for key, value in raw.items():
        s = json.loads(value)
        if key.startswith("map_"):
            map_stats.append(s)
        elif key.startswith("reduce_"):
            reduce_stats.append(s)

    return {"map": map_stats, "reduce": reduce_stats}


def print_progress(progress: dict, elapsed: float):
    phase = progress["phase"]
    m = progress["map"]
    rd = progress["reduce"]

    print(f"\r[{elapsed:6.0f}s] phase={phase:8s} | "
          f"map: {m['done']}/{m['total']} done, {m['queued']} queued | "
          f"reduce: {rd['done']}/{rd['total']} done, {rd['queued']} queued",
          end="", flush=True)


def print_final_report(r: redis.Redis, total_elapsed: float):
    stats = get_worker_stats(r)

    # Map stats
    total_docs = sum(s.get("docs_processed", 0) for s in stats["map"])
    total_terms = sum(s.get("terms_emitted", 0) for s in stats["map"])
    map_times = [s.get("elapsed", 0) for s in stats["map"]]
    map_total_time = sum(map_times)
    map_max_time = max(map_times) if map_times else 0
    map_min_time = min(map_times) if map_times else 0
    map_avg_time = map_total_time / len(map_times) if map_times else 0

    # Reduce stats
    total_unique_terms = sum(s.get("terms", 0) for s in stats["reduce"])
    total_postings = sum(s.get("postings", 0) for s in stats["reduce"])
    total_index_size = sum(s.get("index_size_bytes", 0) for s in stats["reduce"])
    reduce_times = [s.get("elapsed", 0) for s in stats["reduce"]]
    reduce_total_time = sum(reduce_times)
    reduce_max_time = max(reduce_times) if reduce_times else 0
    reduce_min_time = min(reduce_times) if reduce_times else 0
    reduce_avg_time = reduce_total_time / len(reduce_times) if reduce_times else 0

    # Posting list size distribution
    postings_per_reducer = [s.get("postings", 0) for s in stats["reduce"]]
    terms_per_reducer = [s.get("terms", 0) for s in stats["reduce"]]

    print("\n")
    print("=" * 70)
    print("  MAPREDUCE INVERTED INDEX — FINAL REPORT")
    print("=" * 70)

    print("\n--- INPUT ---")
    print(f"  Documents processed:    {total_docs:>12,}")
    print(f"  Term occurrences:       {total_terms:>12,}")

    print("\n--- MAP PHASE ---")
    print(f"  Map tasks completed:    {len(stats['map']):>12}")
    print(f"  Avg task time:          {map_avg_time:>12.1f}s")
    print(f"  Min task time:          {map_min_time:>12.1f}s")
    print(f"  Max task time:          {map_max_time:>12.1f}s")
    print(f"  Total CPU time:         {map_total_time:>12.1f}s")

    print("\n--- SHUFFLE & SORT + REDUCE PHASE ---")
    print(f"  Reduce tasks completed: {len(stats['reduce']):>12}")
    print(f"  Avg task time:          {reduce_avg_time:>12.1f}s")
    print(f"  Min task time:          {reduce_min_time:>12.1f}s")
    print(f"  Max task time:          {reduce_max_time:>12.1f}s")
    print(f"  Total CPU time:         {reduce_total_time:>12.1f}s")

    print("\n--- INDEX ---")
    print(f"  Unique terms:           {total_unique_terms:>12,}")
    print(f"  Total postings:         {total_postings:>12,}")
    print(f"  Index size:             {total_index_size / (1024**2):>12.1f} MB")
    if total_unique_terms > 0:
        print(f"  Avg postings/term:      {total_postings / total_unique_terms:>12.1f}")
    if postings_per_reducer:
        print(f"  Postings/shard (min):   {min(postings_per_reducer):>12,}")
        print(f"  Postings/shard (max):   {max(postings_per_reducer):>12,}")

    print("\n--- TIMING ---")
    print(f"  Wall clock time:        {total_elapsed:>12.1f}s")
    print(f"  Map CPU time:           {map_total_time:>12.1f}s")
    print(f"  Reduce CPU time:        {reduce_total_time:>12.1f}s")
    if total_elapsed > 0:
        print(f"  Parallelism (map):      {map_total_time / max(map_max_time, 0.1):>12.1f}x")
        print(f"  Parallelism (reduce):   {reduce_total_time / max(reduce_max_time, 0.1):>12.1f}x")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(description="MapReduce stats collector")
    parser.add_argument("--once", action="store_true", help="Print current state and exit")
    parser.add_argument("--final", action="store_true", help="Wait for completion, print final report only")
    parser.add_argument("--interval", type=int, default=2, help="Poll interval in seconds")
    args = parser.parse_args()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    try:
        r.ping()
    except redis.ConnectionError:
        print(f"ERROR: Cannot connect to Redis at {REDIS_HOST}:{REDIS_PORT}")
        sys.exit(1)

    start_time = time.time()

    if args.once:
        progress = get_progress(r)
        elapsed = time.time() - start_time
        print_progress(progress, elapsed)
        print()
        if progress["phase"] == "done":
            print_final_report(r, elapsed)
        return

    # Poll until done
    print("Monitoring MapReduce progress...\n")
    while True:
        progress = get_progress(r)
        elapsed = time.time() - start_time

        if not args.final:
            print_progress(progress, elapsed)

        if progress["phase"] == "done":
            print_final_report(r, elapsed)
            break

        time.sleep(args.interval)


if __name__ == "__main__":
    main()
