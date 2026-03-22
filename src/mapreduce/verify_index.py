"""
Verify inverted index correctness.

1. Reads original documents from input
2. Builds a naive (brute-force) inverted index in memory
3. Reads the MapReduce output index
4. Compares every entry — term, doc_ids, term frequencies

If all match — the MapReduce index is correct.

Usage:
  python verify_index.py --input ./data/input --index ./data/output
"""
import argparse
import glob
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))
from tokenizer import tokenize


def build_naive_index(input_dir: str) -> dict[str, dict[str, int]]:
    """
    Build inverted index by brute force — read all docs, count everything.
    Returns: {term: {doc_id: tf, ...}, ...}
    """
    index: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    doc_count = 0

    for file_path in sorted(glob.glob(os.path.join(input_dir, "*.txt"))):
        if not os.path.isfile(file_path):
            continue
        with open(file_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) == 2:
                    doc_id, text = parts
                else:
                    doc_id = f"doc_{doc_count}"
                    text = line

                for term in tokenize(text):
                    index[term][doc_id] += 1

                doc_count += 1

    print(f"Naive index: {doc_count:,} docs, {len(index):,} unique terms")
    return dict(index)


def read_mapreduce_index(index_dir: str) -> dict[str, dict[str, int]]:
    """
    Read the MapReduce output index.
    Format: term\tdoc_id:tf,doc_id:tf,...
    Returns: {term: {doc_id: tf, ...}, ...}
    """
    index: dict[str, dict[str, int]] = {}

    for file_path in sorted(glob.glob(os.path.join(index_dir, "index-*"))):
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if not line:
                    continue

                parts = line.split("\t", 1)
                if len(parts) != 2:
                    continue

                term, postings_str = parts
                postings = {}
                for entry in postings_str.split(","):
                    doc_id, tf = entry.rsplit(":", 1)
                    postings[doc_id] = int(tf)

                if term in index:
                    # Same term in multiple shards — should not happen
                    print(f"  ERROR: duplicate term '{term}' across shards!")
                    index[term].update(postings)
                else:
                    index[term] = postings

    print(f"MR index:    {len(index):,} unique terms")
    return index


def compare(naive: dict, mr: dict) -> bool:
    errors = 0
    max_errors = 20  # stop after 20 to avoid flooding

    # Check terms
    naive_terms = set(naive.keys())
    mr_terms = set(mr.keys())

    missing = naive_terms - mr_terms
    extra = mr_terms - naive_terms

    if missing:
        print(f"\n  MISSING from MR index: {len(missing)} terms")
        for t in list(missing)[:5]:
            print(f"    '{t}' → expected in {len(naive[t])} docs")
        errors += len(missing)

    if extra:
        print(f"\n  EXTRA in MR index: {len(extra)} terms")
        for t in list(extra)[:5]:
            print(f"    '{t}' → found in {len(mr[t])} docs")
        errors += len(extra)

    # Check postings for common terms
    common_terms = naive_terms & mr_terms
    posting_errors = 0

    for term in common_terms:
        naive_postings = naive[term]
        mr_postings = mr[term]

        if naive_postings != mr_postings:
            posting_errors += 1
            if posting_errors <= 5:
                # Find specific difference
                naive_docs = set(naive_postings.keys())
                mr_docs = set(mr_postings.keys())
                missing_docs = naive_docs - mr_docs
                extra_docs = mr_docs - naive_docs
                wrong_tf = {d for d in naive_docs & mr_docs
                            if naive_postings[d] != mr_postings[d]}

                print(f"\n  MISMATCH for '{term}':")
                if missing_docs:
                    print(f"    Missing docs: {list(missing_docs)[:3]}")
                if extra_docs:
                    print(f"    Extra docs:   {list(extra_docs)[:3]}")
                if wrong_tf:
                    for d in list(wrong_tf)[:3]:
                        print(f"    Wrong TF for {d}: expected {naive_postings[d]}, got {mr_postings[d]}")

    if posting_errors:
        print(f"\n  Total posting mismatches: {posting_errors}/{len(common_terms)} terms")
        errors += posting_errors

    return errors == 0


def main():
    parser = argparse.ArgumentParser(description="Verify MapReduce index correctness")
    parser.add_argument("--input", default="./data/input", help="Input directory")
    parser.add_argument("--index", default="./data/output", help="Index directory")
    args = parser.parse_args()

    print("=" * 60)
    print("VERIFYING INVERTED INDEX CORRECTNESS")
    print("=" * 60)

    print("\nStep 1: Building naive (brute-force) index...")
    naive = build_naive_index(args.input)

    print("\nStep 2: Reading MapReduce index...")
    mr = read_mapreduce_index(args.index)

    print("\nStep 3: Comparing...")
    ok = compare(naive, mr)

    print("\n" + "=" * 60)
    if ok:
        print("  PASSED — MapReduce index is CORRECT")
        print(f"  Verified {len(naive):,} terms across all documents")
    else:
        print("  FAILED — index has errors (see above)")
    print("=" * 60)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
