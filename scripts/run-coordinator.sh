#!/usr/bin/env bash
# Runs canoe-coordinator on the host against the kind cluster created by
# scripts/kind-up.sh — see README.md's "Local dev stack" for why the
# coordinator runs on the host rather than in-cluster for local dev.
#
# Usage: scripts/run-coordinator.sh <compiled-pipeline.yaml>
#   e.g. canoe-dslc examples/local-smoke.canoe.hcl -o examples/local-smoke.generated.yaml
#        scripts/run-coordinator.sh examples/local-smoke.generated.yaml
#
# The coordinator runs on the host, but every worker pod/Job it creates
# runs inside the kind cluster — Kafka/MinIO's host-reachable addresses
# (via NodePort) and in-cluster addresses (via Service DNS) are different
# strings, so the coordinator needs to use one for itself and hand pods
# the other. `--pod-kafka-bootstrap`/`--pod-s3-endpoint` exist exactly
# for this split (see `crates/canoe-coordinator/src/main.rs`'s `Cli` docs
# and `crates/canoe-k8s/src/s3_env.rs`) — everywhere else (a real EKS
# deployment, or any single-network setup) leaves them unset and gets the
# coordinator's own address for both.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

CONFIG="${1:?usage: scripts/run-coordinator.sh <compiled-pipeline.yaml>}"

# Static dev credentials for MinIO — must match deploy/kind/minio.yaml.
# Read directly by the coordinator's own S3 calls (object_store's
# AmazonS3Builder::from_env()); AWS_ENDPOINT here is the coordinator's
# own (host-reachable) address — --pod-s3-endpoint below overrides what
# gets forwarded to pods instead of this value.
export AWS_ACCESS_KEY_ID="minioadmin"
export AWS_SECRET_ACCESS_KEY="minioadmin"
export AWS_ALLOW_HTTP="true"
export AWS_REGION="us-east-1" # MinIO ignores the value but object_store requires one be set
export AWS_ENDPOINT="http://localhost:9000"

# Pods inside kind reach the host-run coordinator's metrics endpoint via
# the kind docker network's gateway IP, not `localhost` (which inside a
# pod means the pod itself) — this is the standard way to reach a
# host-bound service from a container on Linux without Docker Desktop's
# `host.docker.internal`.
GATEWAY_IP="$(docker network inspect kind --format '{{ (index .IPAM.Config 0).Gateway }}' 2>/dev/null || true)"
if [ -z "$GATEWAY_IP" ]; then
  echo "warning: couldn't determine the kind docker network's gateway IP — is the cluster up (scripts/kind-up.sh)? Falling back to 172.17.0.1" >&2
  GATEWAY_IP="172.17.0.1"
fi
METRICS_URL="http://${GATEWAY_IP}:9095/metrics/report"

K8S_API_SERVER="$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')"

exec cargo run -p canoe-coordinator -- \
  --config "$CONFIG" \
  --kafka-bootstrap "localhost:9092" \
  --pod-kafka-bootstrap "kafka.canoe-system.svc.cluster.local:9092" \
  --pod-s3-endpoint "http://minio.canoe-system.svc.cluster.local:9000" \
  --namespace canoe-workers \
  --worker-image canoe/python-worker:dev \
  --uv-worker-image canoe/uv-step-worker:dev \
  --adapter-runner-image canoe/adapter-runner:dev \
  --k8s-api-server "$K8S_API_SERVER" \
  --metrics-url "$METRICS_URL" \
  --metrics-addr "0.0.0.0:9095"
