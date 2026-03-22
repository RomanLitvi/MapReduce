"""
MapReduce Reducer for inverted index.

Each reducer:
1. Collects all intermediate files for its partition (from all mappers)
2. Performs k-way merge sort on pre-sorted files
3. Aggregates posting lists: term → [(doc_id, tf), ...]
4. Writes final index segment

SHUFFLE & SORT in detail:
- SHUFFLE: reducer reads intermediate/map-*/part-{reduce_id} from ALL mappers.
  Each mapper wrote entries destined for this reducer (same hash partition).
- SORT: Each mapper's file is already sorted by term. The reducer performs
  a k-way merge (like merge sort's merge step) using a heap, producing
  a globally sorted stream without loading everything into memory.

Output format: "term\tdoc_id1:tf1,doc_id2:tf2,..." (sorted by doc_id)
"""
import glob
import heapq
import json
import os
import sys
import time
from collections import defaultdict

import redis

from config import (
    REDIS_HOST, REDIS_PORT, INTERMEDIATE_DIR, OUTPUT_DIR,
    NUM_REDUCERS, MERGE_BUFFER_SIZE, REDUCE_QUEUE, REDUCE_DONE_COUNTER,
    STATS_KEY, PHASE_KEY,
)


def iter_sorted_file(path: str):
    """
    Yield (term, doc_id, tf) from a sorted intermediate file.
    Uses buffered reading for large files.
    """
    with open(path, "r", encoding="utf-8", buffering=MERGE_BUFFER_SIZE) as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 3:
                yield parts[0], parts[1], int(parts[2])


def k_way_merge(file_paths: list[str]):
    """
    K-way merge of pre-sorted files using a min-heap.

    This is the SORT part of "Shuffle & Sort":
    - Each file is already sorted by term (mapper did this)
    - We use heapq.merge to merge K sorted streams into one sorted stream
    - Memory usage: O(K) where K = number of files, NOT O(N) total entries

    This is how MapReduce handles data larger than RAM — we never load
    everything at once, just maintain K pointers.
    """
    iterators = []
    for path in file_paths:
        iterators.append(iter_sorted_file(path))

    # heapq.merge does k-way merge on sorted iterables
    # It compares by first element (term), which gives us global sort
    yield from heapq.merge(*iterators, key=lambda x: x[0])


def run_reduce_task(task: dict):
    """
    Execute a single reduce task.

    task = {"reduce_id": int}

    SHUFFLE: Collect all intermediate/map-*/part-{reduce_id} files
    SORT: K-way merge sort
    REDUCE: Aggregate posting lists per term
    """
    reduce_id = task["reduce_id"]
    t_start = time.time()

    # --- SHUFFLE PHASE ---
    # Gather all intermediate files for this partition from all mappers
    pattern = os.path.join(INTERMEDIATE_DIR, "map-*", f"part-{reduce_id}")
    file_paths = sorted(glob.glob(pattern))

    if not file_paths:
        print(f"[Reducer {reduce_id}] No intermediate files found.")
        return {"reduce_id": reduce_id, "terms": 0, "elapsed": 0}

    print(
        f"[Reducer {reduce_id}] SHUFFLE: collected {len(file_paths)} files "
        f"from {len(file_paths)} mappers"
    )

    # --- SORT & REDUCE PHASE ---
    # K-way merge produces globally sorted (term, doc_id, tf) stream.
    # We group consecutive entries by term and build posting lists.

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"index-{reduce_id:04d}")

    terms_written = 0
    total_postings = 0

    with open(out_path, "w", encoding="utf-8") as out:
        current_term = None
        posting_list: dict[str, int] = {}  # doc_id → tf

        for term, doc_id, tf in k_way_merge(file_paths):
            if term != current_term:
                # REDUCE: write previous term's posting list
                if current_term is not None:
                    write_posting(out, current_term, posting_list)
                    terms_written += 1
                    total_postings += len(posting_list)

                current_term = term
                posting_list = {}

            # Aggregate: same (term, doc_id) from multiple mapper flushes
            posting_list[doc_id] = posting_list.get(doc_id, 0) + tf

        # Write last term
        if current_term is not None:
            write_posting(out, current_term, posting_list)
            terms_written += 1
            total_postings += len(posting_list)

    elapsed = time.time() - t_start
    file_size = os.path.getsize(out_path)
    print(
        f"[Reducer {reduce_id}] Done: {terms_written} unique terms, "
        f"{total_postings} postings, index size {file_size / 1024 / 1024:.1f}MB, "
        f"{elapsed:.1f}s"
    )

    return {
        "reduce_id": reduce_id,
        "terms": terms_written,
        "postings": total_postings,
        "index_size_bytes": file_size,
        "elapsed": elapsed,
    }


def write_posting(f, term: str, posting_list: dict[str, int]):
    """
    Write a single posting list entry.
    Format: term\tdoc_id1:tf1,doc_id2:tf2,...
    Sorted by doc_id for efficient intersection in query processing.
    """
    # Sort postings by doc_id for deterministic output
    sorted_postings = sorted(posting_list.items())
    postings_str = ",".join(f"{doc_id}:{tf}" for doc_id, tf in sorted_postings)
    f.write(f"{term}\t{postings_str}\n")


def main():
    """Worker loop: pull reduce tasks from Redis queue until empty."""
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

    # Wait for reduce phase
    while r.get(PHASE_KEY) != "reduce":
        time.sleep(1)

    print(f"[Reducer] Started, pulling tasks from queue...")

    while True:
        result = r.blpop(REDUCE_QUEUE, timeout=10)
        if result is None:
            print("[Reducer] No more tasks, exiting.")
            break

        _, task_json = result
        task = json.loads(task_json)
        print(f"[Reducer] Got task: reduce_id={task['reduce_id']}")

        stats = run_reduce_task(task)

        done_count = r.incr(REDUCE_DONE_COUNTER)
        r.hset(STATS_KEY, f"reduce_{task['reduce_id']}", json.dumps(stats))
        print(f"[Reducer] Total completed: {done_count}")


if __name__ == "__main__":
    main()
