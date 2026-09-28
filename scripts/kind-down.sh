#!/usr/bin/env bash
# Tears down the local dev/test stack created by scripts/kind-up.sh.
set -euo pipefail

CLUSTER_NAME="canoe"
kind delete cluster --name "$CLUSTER_NAME"
