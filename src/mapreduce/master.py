"""
MapReduce Master/Coordinator.

Responsibilities:
1. Scan input directory, split files into chunks
2. Push map tasks to Redis queue
3. Wait for all map tasks to complete
4. Push reduce tasks to Redis queue
5. Wait for all reduce tasks to complete
6. Collect and report statistics
"""
import glob
import json
import os
import sys
import time

import redis

from config import (
    REDIS_HOST, REDIS_PORT, INPUT_DIR, INTERMEDIATE_DIR, OUTPUT_DIR,
    CHUNK_SIZE_BYTES, NUM_REDUCERS, MAP_QUEUE, REDUCE_QUEUE,
    MAP_DONE_COUNTER, REDUCE_DONE_COUNTER, TOTAL_MAP_TASKS,
    TOTAL_REDUCE_TASKS, STATS_KEY, PHASE_KEY,
)


def scan_input_files(input_dir: str) -> list[dict]:
    """
    Scan input directory and create map tasks.
    Each task covers a CHUNK_SIZE_BYTES portion of a file.
    """
    tasks = []
    map_id = 0

    # Support multiple input files
    for file_path in sorted(glob.glob(os.path.join(input_dir, "*"))):
        if not os.path.isfile(file_path):
            continue

        file_size = os.path.getsize(file_path)
        offset = 0

        while offset < file_size:
            length = min(CHUNK_SIZE_BYTES, file_size - offset)
            tasks.append({
                "map_id": map_id,
                "file_path": file_path,
                "offset": offset,
                "length": length,
            })
            offset += length
            map_id += 1

    return tasks


def main():
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Clean up from previous runs
    for key in [MAP_QUEUE, REDUCE_QUEUE, MAP_DONE_COUNTER,
                REDUCE_DONE_COUNTER, TOTAL_MAP_TASKS, TOTAL_REDUCE_TASKS,
                STATS_KEY, PHASE_KEY]:
        r.delete(key)

    # Clean intermediate and output directories
    os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    total_start = time.time()

    # ========== PHASE 1: MAP ==========
    print("=" * 60)
    print("PHASE 1: MAP")
    print("=" * 60)

    map_tasks = scan_input_files(INPUT_DIR)
    total_maps = len(map_tasks)

    if total_maps == 0:
        print(f"ERROR: No input files found in {INPUT_DIR}")
        sys.exit(1)

    # Calculate total input size
    total_input_size = sum(t["length"] for t in map_tasks)
    print(f"Input: {total_input_size / (1024**3):.2f} GB")
    print(f"Chunk size: {CHUNK_SIZE_BYTES / (1024**2):.0f} MB")
    print(f"Map tasks: {total_maps}")
    print(f"Reduce tasks: {NUM_REDUCERS}")

    # Push map tasks to queue
    r.set(TOTAL_MAP_TASKS, total_maps)
    for task in map_tasks:
        r.rpush(MAP_QUEUE, json.dumps(task))

    # Signal map phase start
    r.set(PHASE_KEY, "map")
    map_start = time.time()

    # Wait for all map tasks to complete
    print("\nWaiting for map tasks...")
    while True:
        done = int(r.get(MAP_DONE_COUNTER) or 0)
        if done >= total_maps:
            break
        print(f"  Progress: {done}/{total_maps} ({done*100//total_maps}%)", end="\r")
        time.sleep(2)

    map_elapsed = time.time() - map_start
    print(f"\nMap phase complete: {map_elapsed:.1f}s")

    # ========== PHASE 2: REDUCE ==========
    print("\n" + "=" * 60)
    print("PHASE 2: SHUFFLE & SORT + REDUCE")
    print("=" * 60)

    # Push reduce tasks to queue
    r.set(TOTAL_REDUCE_TASKS, NUM_REDUCERS)
    for reduce_id in range(NUM_REDUCERS):
        r.rpush(REDUCE_QUEUE, json.dumps({"reduce_id": reduce_id}))

    # Signal reduce phase start
    r.set(PHASE_KEY, "reduce")
    reduce_start = time.time()

    # Wait for all reduce tasks
    print("Waiting for reduce tasks...")
    while True:
        done = int(r.get(REDUCE_DONE_COUNTER) or 0)
        if done >= NUM_REDUCERS:
            break
        print(f"  Progress: {done}/{NUM_REDUCERS} ({done*100//NUM_REDUCERS}%)", end="\r")
        time.sleep(2)

    reduce_elapsed = time.time() - reduce_start
    print(f"\nReduce phase complete: {reduce_elapsed:.1f}s")

    # ========== STATISTICS ==========
    total_elapsed = time.time() - total_start
    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    # Collect stats from all workers
    all_stats = r.hgetall(STATS_KEY)

    total_docs = 0
    total_terms_emitted = 0
    total_unique_terms = 0
    total_postings = 0
    total_index_size = 0

    for key, value in all_stats.items():
        stats = json.loads(value)
        if key.startswith("map_"):
            total_docs += stats.get("docs_processed", 0)
            total_terms_emitted += stats.get("terms_emitted", 0)
        elif key.startswith("reduce_"):
            total_unique_terms += stats.get("terms", 0)
            total_postings += stats.get("postings", 0)
            total_index_size += stats.get("index_size_bytes", 0)

    # Calculate intermediate data size
    intermediate_size = 0
    for root, dirs, files in os.walk(INTERMEDIATE_DIR):
        for f in files:
            intermediate_size += os.path.getsize(os.path.join(root, f))

    print(f"Input size:           {total_input_size / (1024**3):.2f} GB")
    print(f"Documents processed:  {total_docs:,}")
    print(f"Term occurrences:     {total_terms_emitted:,}")
    print(f"Unique terms:         {total_unique_terms:,}")
    print(f"Total postings:       {total_postings:,}")
    print(f"Intermediate data:    {intermediate_size / (1024**3):.2f} GB")
    print(f"Final index size:     {total_index_size / (1024**2):.1f} MB")
    print(f"Compression ratio:    {total_input_size / max(total_index_size, 1):.1f}x")
    print(f"")
    print(f"Map phase time:       {map_elapsed:.1f}s")
    print(f"Reduce phase time:    {reduce_elapsed:.1f}s")
    print(f"Total time:           {total_elapsed:.1f}s")
    print(f"Throughput:           {total_input_size / (1024**2) / max(total_elapsed, 1):.1f} MB/s")

    # Signal done
    r.set(PHASE_KEY, "done")


if __name__ == "__main__":
    main()
