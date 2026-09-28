#!/usr/bin/env bash
# Stands up the local dev/test stack on `kind`: cluster, namespaces/RBAC,
# Kafka, MinIO, and the three worker images (python-worker, uv-step-worker,
# adapter-runner — spark-runner is skipped, see README.md's "Local dev
# stack" for why). See scripts/run-coordinator.sh to run the coordinator
# against it afterwards, and scripts/kind-down.sh to tear it down.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

CLUSTER_NAME="canoe"

if ! command -v kind >/dev/null 2>&1; then
  echo "kind is not installed — see https://kind.sigs.k8s.io/docs/user/quick-start/#installation" >&2
  exit 1
fi

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  echo "kind cluster '$CLUSTER_NAME' already exists, reusing it (run scripts/kind-down.sh first for a clean slate)"
else
  kind create cluster --name "$CLUSTER_NAME" --config deploy/kind/kind-config.yaml
fi

echo "==> applying namespaces, RBAC, Kafka, MinIO"
kubectl apply -f deploy/kind/namespace.yaml
kubectl apply -f deploy/k8s/namespace.yaml
kubectl apply -f deploy/k8s/rbac.yaml
kubectl apply -f deploy/kind/kafka.yaml
kubectl apply -f deploy/kind/minio.yaml

echo "==> waiting for Kafka and MinIO to be ready"
kubectl -n canoe-system rollout status deployment/kafka --timeout=180s
kubectl -n canoe-system rollout status deployment/minio --timeout=180s
kubectl -n canoe-system wait --for=condition=complete job/minio-init-buckets --timeout=120s

echo "==> building worker images"
docker build -t canoe/python-worker:dev -f workers/python-worker/Dockerfile workers/python-worker
docker build -t canoe/uv-step-worker:dev -f workers/uv-step-worker/Dockerfile workers/uv-step-worker
docker build -t canoe/adapter-runner:dev -f workers/adapter-runner/Dockerfile workers/adapter-runner
# spark-runner is deliberately not built/loaded here — the local stack's
# smoke pipeline has no spark_sql/spark_dataframe step. Build and
# `kind load docker-image canoe/spark-runner:dev` it yourself if you add
# one; it also needs its own RBAC (see deploy/k8s/rbac.yaml's
# canoe-spark ServiceAccount/Role, already applied above).

echo "==> loading images into kind"
kind load docker-image canoe/python-worker:dev canoe/uv-step-worker:dev canoe/adapter-runner:dev --name "$CLUSTER_NAME"

cat <<EOF

Stack is up.
  Kafka:          localhost:9092 (host) / kafka.canoe-system.svc.cluster.local:9092 (in-cluster)
  MinIO API:      localhost:9000 (host) / minio.canoe-system.svc.cluster.local:9000 (in-cluster)
  MinIO console:  http://localhost:9001  (minioadmin / minioadmin)

Next: scripts/run-coordinator.sh
EOF
