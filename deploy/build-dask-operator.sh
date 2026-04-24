#!/usr/bin/env bash
set -euo pipefail

# Build the Dask Kubernetes Operator image for arm64 (Apple Silicon).
#
# The upstream image (ghcr.io/dask/dask-kubernetes-operator) is amd64-only.
# This script clones the source, builds a native arm64 image with podman,
# and loads it into a Lima instance running k3s.
#
# Usage:
#   ./deploy/build-dask-operator.sh <lima-instance-name> [tag]
#
# Example:
#   ./deploy/build-dask-operator.sh k3s
#   ./deploy/build-dask-operator.sh k3s 2026.3.0

LIMA_INSTANCE="${1:?Usage: $0 <lima-instance-name>}"
TAG="${2:-2026.3.0}"
IMAGE="ghcr.io/dask/dask-kubernetes-operator:${TAG}"
TMPDIR=$(mktemp -d)

cleanup() {
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

echo "Cloning dask-kubernetes..."
git clone --depth 1 https://github.com/dask/dask-kubernetes.git "$TMPDIR/dask-kubernetes"

echo "Building $IMAGE with podman..."
podman build -t "$IMAGE" \
    -f "$TMPDIR/dask-kubernetes/dask_kubernetes/operator/deployment/Dockerfile" \
    "$TMPDIR/dask-kubernetes"

echo "Loading image into Lima instance '$LIMA_INSTANCE'..."
podman save "$IMAGE" | limactl shell "$LIMA_INSTANCE" sudo ctr -n k8s.io images import -

echo "Done. Image '$IMAGE' is available in the k3s containerd runtime."
