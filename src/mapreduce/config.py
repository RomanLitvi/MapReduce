"""
MapReduce configuration for inverted index building.
"""
import os

# Redis connection
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# Storage paths (shared NFS volume in k8s)
INPUT_DIR = os.getenv("INPUT_DIR", "/data/input")
INTERMEDIATE_DIR = os.getenv("INTERMEDIATE_DIR", "/data/intermediate")
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/data/output")

# MapReduce parameters
CHUNK_SIZE_BYTES = int(os.getenv("CHUNK_SIZE_BYTES", str(64 * 1024 * 1024)))  # 64MB
NUM_REDUCERS = int(os.getenv("NUM_REDUCERS", "32"))

# Performance tuning
COMBINER_FLUSH_SIZE = int(os.getenv("COMBINER_FLUSH_SIZE", str(500_000)))  # flush after N entries
MERGE_BUFFER_SIZE = int(os.getenv("MERGE_BUFFER_SIZE", str(8 * 1024 * 1024)))  # 8MB read buffer

# Redis keys
MAP_QUEUE = "mapreduce:map_tasks"
REDUCE_QUEUE = "mapreduce:reduce_tasks"
MAP_DONE_COUNTER = "mapreduce:map_done"
REDUCE_DONE_COUNTER = "mapreduce:reduce_done"
TOTAL_MAP_TASKS = "mapreduce:total_map"
TOTAL_REDUCE_TASKS = "mapreduce:total_reduce"
STATS_KEY = "mapreduce:stats"
PHASE_KEY = "mapreduce:phase"
