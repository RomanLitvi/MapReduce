#!/bin/bash
# Deploy MapReduce inverted index to Kubernetes
# Usage: ./deploy.sh [build|deploy|generate|run|all|clean]

set -euo pipefail

IMAGE_NAME="mapreduce-index"
IMAGE_TAG="latest"
NAMESPACE="mapreduce"

build() {
    echo "=== Building Docker image ==="
    docker build -t ${IMAGE_NAME}:${IMAGE_TAG} .
    echo "Done."
}

deploy_infra() {
    echo "=== Deploying infrastructure ==="
    kubectl apply -f k8s/namespace.yaml
    kubectl apply -f k8s/storage.yaml
    kubectl apply -f k8s/redis.yaml
    echo "Waiting for Redis..."
    kubectl -n ${NAMESPACE} wait --for=condition=available deployment/redis --timeout=60s
    echo "Done."
}

generate_data() {
    echo "=== Generating test data (80GB) ==="
    kubectl -n ${NAMESPACE} delete job data-generator --ignore-not-found
    kubectl apply -f k8s/data-generator.yaml
    echo "Data generation started. Monitor with:"
    echo "  kubectl -n ${NAMESPACE} logs -f job/data-generator"
}

run_mapreduce() {
    echo "=== Running MapReduce ==="

    # Clean previous jobs
    kubectl -n ${NAMESPACE} delete job mapreduce-master --ignore-not-found
    kubectl -n ${NAMESPACE} delete job mapreduce-mappers --ignore-not-found
    kubectl -n ${NAMESPACE} delete job mapreduce-reducers --ignore-not-found

    # Start all components (workers wait for phase signal from master)
    kubectl apply -f k8s/master.yaml
    kubectl apply -f k8s/mappers.yaml
    kubectl apply -f k8s/reducers.yaml

    echo ""
    echo "MapReduce started! Monitor with:"
    echo "  kubectl -n ${NAMESPACE} logs -f job/mapreduce-master"
    echo "  kubectl -n ${NAMESPACE} logs -f job/mapreduce-mappers"
    echo "  kubectl -n ${NAMESPACE} logs -f job/mapreduce-reducers"
}

clean() {
    echo "=== Cleaning up ==="
    kubectl delete namespace ${NAMESPACE} --ignore-not-found
    echo "Done."
}

case "${1:-all}" in
    build)       build ;;
    deploy)      deploy_infra ;;
    generate)    generate_data ;;
    run)         run_mapreduce ;;
    all)         build && deploy_infra && generate_data ;;
    clean)       clean ;;
    *)           echo "Usage: $0 [build|deploy|generate|run|all|clean]" ;;
esac
