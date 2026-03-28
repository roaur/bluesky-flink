#!/bin/bash
set -e

echo "🚀 Starting Kind cluster setup for Bluesky Pipeline..."

# 1. Create Kind cluster if it doesn't exist
if ! kind get clusters | grep -q "bluesky-cluster"; then
    echo "Creating kind cluster 'bluesky-cluster'..."
    kind create cluster --name bluesky-cluster --config ./k8s/kind-config.yaml
else
    echo "Cluster 'bluesky-cluster' already exists."
fi

# 2. Build local consumer image and load into Kind
echo "Building firehose-consumer image..."
docker build -t firehose-consumer:latest ./firehose-consumer/
echo "Loading image into kind..."
kind load docker-image firehose-consumer:latest --name bluesky-cluster

# 3. Apply manifests
echo "Applying Kubernetes manifests..."
kubectl config use-context kind-bluesky-cluster
kubectl apply -f ./k8s/00-namespace.yaml
kubectl apply -f ./k8s/01-configmaps.yaml

# 4. Create Flink ConfigMap from existing properties
echo "Creating Flink ConfigMap..."
kubectl create configmap flink-config --from-file=./compose/config/flink/ -n bluesky --dry-run=client -o yaml | kubectl apply -f -

# 5. Apply rest of the infrastructure
kubectl apply -f ./k8s/02-minio.yaml
kubectl apply -f ./k8s/03-nessie.yaml
kubectl apply -f ./k8s/04-redpanda.yaml
kubectl apply -f ./k8s/05-clickhouse.yaml
kubectl apply -f ./k8s/06-flink.yaml

echo "🎉 Infrastructure deployed to Kind! The firehose consumer will be deployed in Phase 2 once the code is updated."
# We'll apply 07-firehose-consumer.yaml later.
