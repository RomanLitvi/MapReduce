"""
MapReduce Mapper for inverted index.

Each mapper:
1. Reads a chunk of the input (offset + length in a large file)
2. Tokenizes documents
3. Uses in-mapper combining to aggregate (term, doc_id, tf) locally
4. Partitions output by hash(term) % NUM_REDUCERS
5. Sorts each partition by term before writing

Input format: one document per line, format "doc_id\ttext"
Output: intermediate files sorted by term, format "term\tdoc_id\ttf"
"""
import json
import os
import sys
import time
import hashlib
from collections import defaultdict

import redis

from config import (
    REDIS_HOST, REDIS_PORT, INPUT_DIR, INTERMEDIATE_DIR,
    NUM_REDUCERS, COMBINER_FLUSH_SIZE, MAP_QUEUE, MAP_DONE_COUNTER,
    STATS_KEY, PHASE_KEY,
)
from tokenizer import tokenize


def partition_key(term: str, num_reducers: int) -> int:
    """Deterministic partition: hash(term) % R."""
    h = hashlib.md5(term.encode("utf-8")).hexdigest()
    return int(h, 16) % num_reducers


def run_map_task(task: dict):
    """
    Execute a single map task.

    task = {
        "map_id": int,
        "file_path": str,      # path to input file
        "offset": int,         # byte offset to start reading
        "length": int,         # bytes to read
    }
    """
    map_id = task["map_id"]
    file_path = task["file_path"]
    offset = task["offset"]
    length = task["length"]

    t_start = time.time()
    docs_processed = 0
    terms_emitted = 0

    # --- In-mapper combiner ---
    # Instead of emitting (term, doc_id, 1) for every occurrence,
    # we aggregate locally: combiner[(term, doc_id)] += 1
    # This dramatically reduces intermediate data size.
    combiner: dict[tuple[str, str], int] = defaultdict(int)

    # Read our chunk
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        f.seek(offset)

        # If not at file start, skip partial first line
        if offset > 0:
            f.readline()

        bytes_read = f.tell() - offset
        while bytes_read < length:
            line = f.readline()
            if not line:
                break
            bytes_read = f.tell() - offset

            line = line.strip()
            if not line:
                continue

            # Parse document: "doc_id\ttext" or just treat line number as doc_id
            parts = line.split("\t", 1)
            if len(parts) == 2:
                doc_id, text = parts
            else:
                doc_id = f"doc_{offset}_{docs_processed}"
                text = line

            # MAP FUNCTION: tokenize and emit (term, doc_id, 1)
            # With in-mapper combining, we aggregate the 1s locally
            tokens = tokenize(text)
            for token in tokens:
                combiner[(token, doc_id)] += 1  # The "1" — counting occurrences
                terms_emitted += 1

            docs_processed += 1

            # Flush combiner if it gets too large (memory management)
            if len(combiner) >= COMBINER_FLUSH_SIZE:
                flush_combiner(combiner, map_id)
                combiner.clear()

    # Final flush
    if combiner:
        flush_combiner(combiner, map_id)

    elapsed = time.time() - t_start
    print(
        f"[Mapper {map_id}] Done: {docs_processed} docs, "
        f"{terms_emitted} term occurrences, {elapsed:.1f}s"
    )

    return {
        "map_id": map_id,
        "docs_processed": docs_processed,
        "terms_emitted": terms_emitted,
        "elapsed": elapsed,
    }


def flush_combiner(combiner: dict[tuple[str, str], int], map_id: int):
    """
    Write combiner contents to partitioned intermediate files.
    Each partition is sorted by term for efficient reducer merge.
    """
    # Group by partition
    partitions: dict[int, list[tuple[str, str, int]]] = defaultdict(list)
    for (term, doc_id), tf in combiner.items():
        p = partition_key(term, NUM_REDUCERS)
        partitions[p].append((term, doc_id, tf))

    # Write each partition (sorted by term)
    out_dir = os.path.join(INTERMEDIATE_DIR, f"map-{map_id}")
    os.makedirs(out_dir, exist_ok=True)

    for part_id, entries in partitions.items():
        entries.sort(key=lambda x: x[0])  # Sort by term
        out_path = os.path.join(out_dir, f"part-{part_id}")
        with open(out_path, "a", encoding="utf-8") as f:
            for term, doc_id, tf in entries:
                f.write(f"{term}\t{doc_id}\t{tf}\n")


def main():
    """Worker loop: pull map tasks from Redis queue until empty."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Wait for map phase
    while r.get(PHASE_KEY) != "map":
        time.sleep(1)

    print(f"[Mapper] Started, pulling tasks from queue...")

    while True:
        # BLPOP with timeout — blocks until task available or timeout
        result = r.blpop(MAP_QUEUE, timeout=5)
        if result is None:
            # No more tasks
            print("[Mapper] No more tasks, exiting.")
            break

        _, task_json = result
        task = json.loads(task_json)
        print(f"[Mapper] Got task: map_id={task['map_id']}")

        stats = run_map_task(task)

        # Report completion
        done_count = r.incr(MAP_DONE_COUNTER)
        r.hset(STATS_KEY, f"map_{task['map_id']}", json.dumps(stats))
        print(f"[Mapper] Total completed: {done_count}")


if __name__ == "__main__":
    main()
