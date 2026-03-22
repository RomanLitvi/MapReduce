# MapReduce Inverted Index

A distributed inverted index builder using the MapReduce paradigm, orchestrated on Kubernetes.

Processes large text collections (tested up to 80GB) by splitting work across parallel mapper and reducer pods, coordinated through Redis.

## Architecture

```
                         ┌──────────────────────────┐
                         │     MASTER (1 pod)        │
                         │                           │
                         │  1. Download/prepare data  │
                         │  2. Split into 64MB chunks │
                         │  3. Push tasks to Redis    │
                         │  4. Signal phases          │
                         │  5. Collect statistics      │
                         └─────────┬────────────────┘
                                   │
                            Redis Queue
                         ┌─────────┴────────────────┐
                         │                           │
              ┌──────────▼──────────┐    ┌───────────▼──────────┐
              │    MAP PHASE         │    │    REDUCE PHASE       │
              │                      │    │                       │
              │  Mapper-0            │    │  Reducer-0            │
              │  Mapper-1            │    │  Reducer-1            │
              │  Mapper-2            │    │  Reducer-2            │
              │  ...                 │    │  ...                  │
              │                      │    │                       │
              │  Each reads a 64MB   │    │  Each collects its    │
              │  chunk, tokenizes,   │    │  partition from ALL   │
              │  emits (term,doc,tf) │    │  mappers, merges,     │
              │  to partitioned      │    │  writes final index   │
              │  intermediate files  │    │  segment              │
              └──────────────────────┘    └───────────────────────┘
```

### Kubernetes Deployment

```
┌─────────────────── Kubernetes Cluster ───────────────────┐
│                                                           │
│  Redis (Deployment + Service)     Shared PVC (200Gi)     │
│  ┌─────────┐                      ┌──────────────────┐   │
│  │  :6379   │◄── all pods ──────► │ /data/input/     │   │
│  │  queue   │    connect here     │ /data/inter.../  │   │
│  └─────────┘                      │ /data/output/    │   │
│                                    └──────────────────┘   │
│  Master Job (1 pod, with init container for data download)│
│  Mapper Job (N parallel pods, pull tasks from Redis)      │
│  Reducer Job (N parallel pods, pull tasks from Redis)     │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

## How MapReduce Works

### Phase 1: Map

Each mapper receives a task: *"read file X from byte offset Y, length 64MB"*.

```python
# Conceptual map function
def map(doc_id, text):
    for term in tokenize(text):
        emit(term, doc_id, 1)     # "1" = I saw this term once
```

**The "ones" explained.** Instead of counting term frequency upfront, each mapper emits a simple atomic fact — *"I saw term T in document D one time"*. The mapper doesn't know the full picture (it only sees its 64MB chunk), so it reports the smallest unit of information. The reducer aggregates all the ones later:

```
Mapper sees: "hello world hello" in doc_1

Emits:  ("hello", doc_1, 1)
        ("world", doc_1, 1)
        ("hello", doc_1, 1)    ← another 1

Reducer: ("hello", doc_1) → 1 + 1 = 2  (term frequency)
```

**In-mapper combiner** (optimization): instead of emitting every `(term, doc_id, 1)` individually, the mapper aggregates locally in a hash map before writing to disk. This reduces intermediate data by 10-100x:

```python
# What actually happens in the mapper
combiner = defaultdict(int)
for term in tokenize(text):
    combiner[(term, doc_id)] += 1    # aggregate the ones locally

# Flush: ("hello", doc_1, 2) instead of two separate ("hello", doc_1, 1)
```

Each mapper writes its output to **partitioned files**, sorted by term:
```
intermediate/map-0/part-0    ← all terms where hash(term) % R == 0
intermediate/map-0/part-1    ← all terms where hash(term) % R == 1
...
intermediate/map-0/part-31   ← all terms where hash(term) % R == 31
```

### Phase 2: Shuffle & Sort

This is the critical phase that bridges Map and Reduce. It has two parts:

**Shuffle** — each reducer collects its partition from ALL mappers. Reducer-5 reads:
```
intermediate/map-0/part-5
intermediate/map-1/part-5
intermediate/map-2/part-5
...
intermediate/map-N/part-5
```

The partitioning (`hash(term) % R`) guarantees that all occurrences of the same term end up at the same reducer.

**Sort** — each mapper's file is already sorted by term. The reducer performs a **k-way merge sort** using a min-heap (`heapq.merge`), merging N sorted streams into one globally sorted stream:

```
File 0: algorithm, database, search, ...
File 1: algorithm, index, vector, ...
File 2: boolean, index, retrieval, ...
        ↓ k-way merge
Output: algorithm, algorithm, boolean, database, index, index, retrieval, search, vector, ...
```

Memory usage: **O(K)** where K = number of files, not O(N) total entries. This is how MapReduce handles data larger than RAM — it never loads everything at once.

### Phase 3: Reduce

The reducer reads the globally sorted stream and groups consecutive entries by term:

```python
# Conceptual reduce function
def reduce(term, values):         # values = [(doc_1, 2), (doc_42, 5), ...]
    posting_list = sorted(values)
    emit(term, posting_list)
```

Output (one file per reducer, the **sharded inverted index**):
```
algorithm    doc_42:3,doc_100:1,doc_9999:7
retrieval    doc_1:2,doc_42:1,doc_500:4
search       doc_1:5,doc_100:2
```

Format: `term\tdoc_id:tf,doc_id:tf,...` — sorted by doc_id for efficient query-time intersection.

### Sharded Index

The 32 output files (`index-0000` ... `index-0031`) form a **sharded index**. Each shard contains a disjoint subset of terms, determined by `hash(term) % 32`.

To look up a term at query time:
```
hash("algorithm") % 32 = 17  →  open only index-0017, find "algorithm"
```

For a boolean query `"algorithm AND retrieval"`:
```
hash("algorithm") % 32 = 17  →  index-0017 → {doc_42, doc_100, doc_9999}
hash("retrieval") % 32 = 5   →  index-0005 → {doc_1, doc_42, doc_500}
Intersection: {doc_42}
```

Two file reads instead of scanning the entire index.

## Performance Optimizations

| Optimization | Effect |
|---|---|
| In-mapper combiner | Reduces intermediate data 10-100x |
| Partitioning `hash(term) % R` | Even distribution across reducers |
| Sort in mapper | Enables k-way merge instead of full sort in reducer |
| `heapq.merge` | O(K) memory instead of O(N) |
| Buffered I/O (8MB) | Reduces syscall overhead |
| Streaming reduce | Never holds the full index in memory |

## Project Structure

```
.
├── src/mapreduce/
│   ├── config.py              # Configuration (Redis, paths, tuning)
│   ├── tokenizer.py           # Text tokenization and normalization
│   ├── mapper.py              # Map worker: tokenize → combine → partition → sort
│   ├── reducer.py             # Reduce worker: shuffle → k-way merge → aggregate
│   ├── master.py              # Coordinator: split input, manage phases via Redis
│   ├── local_runner.py        # Single-node runner for testing without k8s
│   ├── download_data.py       # Download Wikipedia / 20 Newsgroups
│   └── generate_test_data.py  # Generate synthetic test corpus
├── k8s/
│   ├── namespace.yaml         # mapreduce namespace
│   ├── storage.yaml           # PersistentVolumeClaim (ReadWriteMany)
│   ├── redis.yaml             # Redis deployment + service
│   ├── master.yaml            # Master job (with init container for data download)
│   ├── mappers.yaml           # Mapper job (parallel pods)
│   └── reducers.yaml          # Reducer job (parallel pods)
├── .github/workflows/
│   └── ci.yaml                # CI: lint + multi-platform Docker build (amd64/arm64)
├── Dockerfile                 # Multi-role image (master/mapper/reducer)
├── deploy.sh                  # Deployment helper script
└── requirements.txt           # Python dependencies (redis)
```

## Quick Start

### Option A: Local (no Kubernetes)

```bash
# Setup
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Download test data (20 Newsgroups, ~18MB)
python src/mapreduce/download_data.py --dataset small --output ./data/input

# Run MapReduce locally
python src/mapreduce/local_runner.py \
  --input ./data/input \
  --output ./data/output \
  --num-reducers 4

# View the index
head -5 ./data/output/index-0000
```

### Option B: Kubernetes (minikube)

```bash
# Start minikube (adjust memory to your machine)
minikube start --memory=12288 --cpus=4 --disk-size=50g --driver=docker

# Build image inside minikube
eval $(minikube docker-env)
docker build -t litvinchukroman/mapreduce-index:latest .

# Deploy infrastructure
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/storage.yaml
kubectl apply -f k8s/redis.yaml
kubectl -n mapreduce wait --for=condition=available deployment/redis --timeout=60s

# Run MapReduce (init container downloads 2GB Wikipedia automatically)
kubectl apply -f k8s/master.yaml
kubectl apply -f k8s/mappers.yaml
kubectl apply -f k8s/reducers.yaml

# Monitor
kubectl -n mapreduce get pods -w
kubectl -n mapreduce logs -f job/mapreduce-master
kubectl -n mapreduce logs -f job/mapreduce-master -c download-data  # init container

# View results
kubectl -n mapreduce run reader --rm -it --restart=Never \
  --image=busybox \
  --overrides='{
    "spec": {
      "containers": [{
        "name": "reader",
        "image": "busybox",
        "command": ["sh", "-c", "ls -lh /data/output/ && echo --- && head -5 /data/output/index-0000"],
        "volumeMounts": [{"name": "data", "mountPath": "/data"}]
      }],
      "volumes": [{
        "name": "data",
        "persistentVolumeClaim": {"claimName": "mapreduce-data"}
      }]
    }
  }'

# Cleanup
kubectl delete namespace mapreduce
```

### Option C: Cloud Cluster (for 80GB)

For the full 80GB Wikipedia dump, use a cloud Kubernetes cluster (GKE, EKS, AKS) with:
- NFS or cloud storage with `ReadWriteMany` access mode
- 8+ mapper/reducer pods with 4GB RAM each
- 200GB persistent volume

Edit `k8s/master.yaml` init container command to remove the `--max-size-gb` flag, and increase `parallelism` in `mappers.yaml` / `reducers.yaml` to 8.

## Data on the PVC

```
/data/
├── input/
│   └── wikipedia.txt              ← source corpus
├── intermediate/
│   ├── map-0/
│   │   ├── part-0                 ← entries for reducer 0 (sorted by term)
│   │   ├── part-1                 ← entries for reducer 1
│   │   └── ...part-31
│   ├── map-1/
│   └── ...
└── output/
    ├── index-0000                 ← final inverted index, shard 0
    ├── index-0001                 ← shard 1
    └── ...index-0031              ← shard 31
```

## Test Collections

| Collection | Size | Documents | Download |
|---|---|---|---|
| 20 Newsgroups | 18 MB | ~18,000 | `--dataset small` |
| Wikipedia (partial) | 2 GB | ~300,000 | `--dataset large --max-size-gb 2` |
| Wikipedia (full) | 80 GB | ~7,000,000 | `--dataset large` |
| Synthetic | configurable | configurable | `generate_test_data.py --size-gb N` |
