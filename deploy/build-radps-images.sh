#!/usr/bin/env bash
set -euo pipefail

# Build the RADPS custom container images for arm64 (Apple Silicon).
#
# The upstream images published to ghcr.io/casangi are amd64-only.
# This script clones the source repos, builds native arm64 images
# with podman, and loads them into a Lima instance running k3s.
#
# Requires: podman, limactl, git
#
# Usage:
#   ./deploy/build-radps-images.sh <lima-instance-name>
#
# Example:
#   ./deploy/build-radps-images.sh k3s

LIMA_INSTANCE="${1:?Usage: $0 <lima-instance-name>}"
TMPDIR=$(mktemp -d)

cleanup() {
    rm -rf "$TMPDIR"
}
trap cleanup EXIT

load_image() {
    local image="$1"
    echo "Loading $image into Lima instance '$LIMA_INSTANCE'..."
    podman save "$image" | limactl shell "$LIMA_INSTANCE" sudo ctr -n k8s.io images import -
}

# --- radps-jupyter-notebook ---
NOTEBOOK_IMAGE="ghcr.io/casangi/radps-jupyter-notebook:v0.0.6"
echo "=== Building $NOTEBOOK_IMAGE ==="
git clone --depth 1 https://github.com/casangi/radps-jupyter-notebook.git "$TMPDIR/radps-jupyter-notebook"
podman build -t "$NOTEBOOK_IMAGE" "$TMPDIR/radps-jupyter-notebook"
load_image "$NOTEBOOK_IMAGE"

# --- radps-dask-worker ---
WORKER_IMAGE="ghcr.io/casangi/radps-dask-worker:latest"
echo "=== Building $WORKER_IMAGE ==="
git clone --depth 1 https://github.com/casangi/radps-dask-worker.git "$TMPDIR/radps-dask-worker"
podman build -t "$WORKER_IMAGE" "$TMPDIR/radps-dask-worker"
load_image "$WORKER_IMAGE"

echo "=== Done. All images loaded into Lima instance '$LIMA_INSTANCE'. ==="
