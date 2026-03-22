"""
Generate test data for MapReduce inverted index.
Creates a large file with synthetic documents.

Usage:
  python generate_test_data.py --size-gb 80 --output /data/input/corpus.txt
  python generate_test_data.py --size-gb 1 --output /data/input/corpus.txt  # for testing
"""
import argparse
import os
import random
import string
import hashlib


# Sample vocabulary — in real scenario, use actual text corpus
VOCABULARY = [
    "information", "retrieval", "search", "engine", "index", "document",
    "query", "term", "frequency", "inverse", "ranking", "relevance",
    "boolean", "model", "vector", "space", "cosine", "similarity",
    "precision", "recall", "algorithm", "data", "structure", "tree",
    "hash", "table", "linked", "list", "array", "graph", "node",
    "edge", "weight", "path", "shortest", "network", "database",
    "storage", "memory", "disk", "cache", "buffer", "page", "block",
    "sector", "cluster", "file", "system", "operating", "process",
    "thread", "mutex", "semaphore", "deadlock", "starvation", "priority",
    "scheduling", "round", "robin", "first", "come", "served",
    "machine", "learning", "neural", "network", "deep", "layer",
    "activation", "function", "gradient", "descent", "backpropagation",
    "training", "testing", "validation", "accuracy", "loss", "epoch",
    "batch", "size", "rate", "optimizer", "regularization", "dropout",
    "convolution", "pooling", "recurrent", "attention", "transformer",
    "embedding", "token", "vocabulary", "sequence", "classification",
    "regression", "clustering", "dimensionality", "reduction", "feature",
    "selection", "extraction", "engineering", "pipeline", "preprocessing",
    "normalization", "standardization", "encoding", "decoding",
    "compression", "decompression", "encryption", "decryption",
    "authentication", "authorization", "session", "cookie", "request",
    "response", "server", "client", "protocol", "tcp", "udp", "http",
    "distributed", "computing", "parallel", "concurrent", "synchronous",
    "asynchronous", "message", "queue", "broker", "producer", "consumer",
    "partition", "replication", "consistency", "availability", "tolerance",
    "kubernetes", "container", "docker", "image", "registry", "pod",
    "service", "deployment", "replica", "controller", "namespace",
    "volume", "persistent", "claim", "config", "map", "secret",
    "ingress", "load", "balancer", "proxy", "reverse", "forward",
    "university", "student", "professor", "lecture", "exam", "grade",
    "semester", "course", "program", "degree", "bachelor", "master",
    "research", "paper", "journal", "conference", "publication",
    "python", "java", "javascript", "typescript", "golang", "rust",
    "compiler", "interpreter", "runtime", "virtual", "bytecode",
    "garbage", "collection", "reference", "counting", "mark", "sweep",
]


def generate_document(doc_id: int, min_words: int = 50, max_words: int = 500) -> str:
    """Generate a synthetic document with random words from vocabulary."""
    num_words = random.randint(min_words, max_words)
    words = random.choices(VOCABULARY, k=num_words)
    text = " ".join(words)
    return f"doc_{doc_id}\t{text}"


def main():
    parser = argparse.ArgumentParser(description="Generate test data for MapReduce")
    parser.add_argument("--size-gb", type=float, default=1.0, help="Target size in GB")
    parser.add_argument("--output", type=str, default="/data/input/corpus.txt")
    args = parser.parse_args()

    target_bytes = int(args.size_gb * 1024 * 1024 * 1024)
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    print(f"Generating {args.size_gb} GB of test data...")
    print(f"Output: {args.output}")

    doc_id = 0
    bytes_written = 0

    with open(args.output, "w", encoding="utf-8") as f:
        while bytes_written < target_bytes:
            line = generate_document(doc_id) + "\n"
            f.write(line)
            bytes_written += len(line.encode("utf-8"))
            doc_id += 1

            if doc_id % 100_000 == 0:
                pct = bytes_written * 100 / target_bytes
                print(f"  {pct:.1f}% — {doc_id:,} docs, {bytes_written / (1024**3):.2f} GB")

    print(f"Done: {doc_id:,} documents, {bytes_written / (1024**3):.2f} GB")


if __name__ == "__main__":
    main()
