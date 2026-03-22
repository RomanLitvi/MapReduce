"""
Single-node runner for testing MapReduce without Kubernetes or Redis.
Runs master → mappers → reducers sequentially in one process.

Usage:
  python local_runner.py --input ./data/input --output ./data/output
  python local_runner.py --input ./data/input --output ./data/output --num-reducers 4
"""
import argparse
import glob
import json
import os
import shutil
import sys
import time

# Add parent to path for imports
sys.path.insert(0, os.path.dirname(__file__))

import config
from mapper import run_map_task
from reducer import run_reduce_task


def main():
    parser = argparse.ArgumentParser(description="Local MapReduce runner")
    parser.add_argument("--input", default="./data/input", help="Input directory")
    parser.add_argument("--output", default="./data/output", help="Output directory")
    parser.add_argument("--intermediate", default="./data/intermediate", help="Intermediate directory")
    parser.add_argument("--chunk-size-mb", type=int, default=64, help="Chunk size in MB")
    parser.add_argument("--num-reducers", type=int, default=4, help="Number of reduce partitions")
    args = parser.parse_args()

    # Override config
    config.INPUT_DIR = args.input
    config.OUTPUT_DIR = args.output
    config.INTERMEDIATE_DIR = args.intermediate
    config.CHUNK_SIZE_BYTES = args.chunk_size_mb * 1024 * 1024
    config.NUM_REDUCERS = args.num_reducers

    # Clean previous run
    if os.path.exists(args.intermediate):
        shutil.rmtree(args.intermediate)
    if os.path.exists(args.output):
        shutil.rmtree(args.output)

    total_start = time.time()

    # ========== SCAN INPUT ==========
    print("=" * 60)
    print("SCANNING INPUT")
    print("=" * 60)

    tasks = []
    map_id = 0
    total_input_size = 0

    for file_path in sorted(glob.glob(os.path.join(args.input, "*"))):
        if not os.path.isfile(file_path):
            continue

        file_size = os.path.getsize(file_path)
        total_input_size += file_size
        offset = 0

        while offset < file_size:
            length = min(config.CHUNK_SIZE_BYTES, file_size - offset)
            tasks.append({
                "map_id": map_id,
                "file_path": file_path,
                "offset": offset,
                "length": length,
            })
            offset += length
            map_id += 1

    print(f"Input size: {total_input_size / (1024**2):.1f} MB")
    print(f"Map tasks:  {len(tasks)}")
    print(f"Reducers:   {args.num_reducers}")

    if not tasks:
        print(f"ERROR: No input files in {args.input}")
        sys.exit(1)

    # ========== MAP PHASE ==========
    print("\n" + "=" * 60)
    print("MAP PHASE")
    print("=" * 60)

    map_start = time.time()
    total_docs = 0
    total_terms = 0

    for i, task in enumerate(tasks):
        stats = run_map_task(task)
        total_docs += stats["docs_processed"]
        total_terms += stats["terms_emitted"]
        print(f"  [{i+1}/{len(tasks)}] map_id={task['map_id']}: "
              f"{stats['docs_processed']} docs, {stats['elapsed']:.1f}s")

    map_elapsed = time.time() - map_start
    print(f"\nMap done: {total_docs:,} docs, {total_terms:,} terms, {map_elapsed:.1f}s")

    # ========== REDUCE PHASE ==========
    print("\n" + "=" * 60)
    print("SHUFFLE & SORT + REDUCE PHASE")
    print("=" * 60)

    reduce_start = time.time()
    total_unique_terms = 0
    total_postings = 0
    total_index_size = 0

    for reduce_id in range(args.num_reducers):
        task = {"reduce_id": reduce_id}
        stats = run_reduce_task(task)
        total_unique_terms += stats["terms"]
        total_postings += stats.get("postings", 0)
        total_index_size += stats.get("index_size_bytes", 0)

    reduce_elapsed = time.time() - reduce_start
    print(f"\nReduce done: {reduce_elapsed:.1f}s")

    # ========== RESULTS ==========
    total_elapsed = time.time() - total_start

    # Intermediate size
    intermediate_size = 0
    for root, dirs, files in os.walk(args.intermediate):
        for f in files:
            intermediate_size += os.path.getsize(os.path.join(root, f))

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"Input size:           {total_input_size / (1024**2):.1f} MB")
    print(f"Documents processed:  {total_docs:,}")
    print(f"Term occurrences:     {total_terms:,}")
    print(f"Unique terms:         {total_unique_terms:,}")
    print(f"Total postings:       {total_postings:,}")
    print(f"Intermediate data:    {intermediate_size / (1024**2):.1f} MB")
    print(f"Final index size:     {total_index_size / (1024**2):.1f} MB")
    print(f"Compression ratio:    {total_input_size / max(total_index_size, 1):.1f}x")
    print(f"")
    print(f"Map phase time:       {map_elapsed:.1f}s")
    print(f"Reduce phase time:    {reduce_elapsed:.1f}s")
    print(f"Total time:           {total_elapsed:.1f}s")
    print(f"Throughput:           {total_input_size / (1024**2) / max(total_elapsed, 1):.1f} MB/s")
    print(f"")
    print(f"Index files:          {args.output}/index-*")

    # Show sample from index
    print("\n" + "=" * 60)
    print("SAMPLE OUTPUT (first 5 lines of index-0000)")
    print("=" * 60)
    sample_path = os.path.join(args.output, "index-0000")
    if os.path.exists(sample_path):
        with open(sample_path, "r") as f:
            for i, line in enumerate(f):
                if i >= 5:
                    break
                print(f"  {line.rstrip()}")


if __name__ == "__main__":
    main()
