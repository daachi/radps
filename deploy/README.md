# Local Development Deployment (macOS Apple Silicon)

Complete guide to deploying the RADPS JupyterHub + Dask + Prefect stack on a local single-node k3s cluster running in Lima.

## Prerequisites

Install the following on your Mac via Homebrew:

- `podman` — container builds (no Docker Desktop needed)
- `limactl` — Lima VM management
- `kubectl` — Kubernetes CLI
- `helm` — Helm chart management
- `kustomize` — manifest management (v5.4+)

## 1. Create the k3s VM

```bash
limactl start --name=k3s template://k3s
```

Verify it's running:

```bash
limactl list
limactl shell k3s kubectl get nodes
```

Set up a shell alias or env var for convenience (used throughout this guide):

```bash
export KUBECONFIG=~/.lima/k3s/copied-from-guest/kubeconfig.yaml
```

## 2. Build container images for arm64

All three custom images are published as amd64-only. They must be built locally for arm64 and loaded into the k3s containerd runtime.

**Important:** The `radps-jupyter-notebook` Dockerfile in the upstream repo has a bug — `pip install` runs as a non-root user and jupyterhub fails to install. The fixed Dockerfile at `~/Source/github/casangi/radps-jupyter-notebook/Dockerfile` runs pip as root.

### Dask Kubernetes Operator

```bash
./deploy/build-dask-operator.sh k3s
```

This clones `github.com/dask/dask-kubernetes`, builds the operator image, and loads it into the VM. The default tag is `2026.3.0`.

### RADPS Jupyter Notebook + Dask Worker

```bash
./deploy/build-radps-images.sh k3s
```

This clones `github.com/casangi/radps-jupyter-notebook` and `github.com/casangi/radps-dask-worker`, builds both images, and loads them into the VM.

### Verify images are loaded

```bash
limactl shell k3s sudo ctr -n k8s.io images ls | grep -E "dask-kubernetes-operator|radps"
```

## 3. Add Helm repos

```bash
helm repo add dask https://helm.dask.org/
helm repo add prefect https://prefecthq.github.io/prefect-helm
helm repo add jupyterhub https://hub.jupyter.org/helm-chart/
helm repo update
```

## 4. Install the Dask Kubernetes Operator

```bash
helm install dask-kubernetes-operator dask/dask-kubernetes-operator \
  --create-namespace -n dask-operator \
  --set image.pullPolicy=IfNotPresent
```

`IfNotPresent` is critical — without it, k3s will pull the amd64 image from ghcr.io over your local arm64 build.

Verify:

```bash
kubectl get pods -n dask-operator
```

## 5. Deploy Prefect Server

Create the namespace and apply via kustomize:

```bash
kubectl create namespace prefect-server
kustomize build --enable-helm deploy/prefect-server/overlays/local > /tmp/prefect-server-local.yaml
kubectl apply -f /tmp/prefect-server-local.yaml --namespace prefect-server
```

The `--namespace prefect-server` flag is required because the JupyterHub and Prefect Helm charts do not embed namespace in resource metadata.

Wait for pods to be ready:

```bash
kubectl get pods -n prefect-server -w
```

The Prefect UI will be accessible via the ingress at `http://prefect.noop.local`. Add this to `/etc/hosts`:

```
<lima-vm-ip>  prefect.noop.local
```

To find the Lima VM IP:

```bash
limactl shell k3s hostname -I
```

## 6. Deploy JupyterHub

Create the namespace and apply:

```bash
kubectl create namespace radps-hub
kustomize build --enable-helm deploy/radps-hub/overlays/local > /tmp/radps-hub-local.yaml
kubectl apply -f /tmp/radps-hub-local.yaml --namespace radps-hub
```

Wait for pods to be ready:

```bash
kubectl get pods -n radps-hub -w
```

All pods should reach `Running` or `Completed` status within a couple minutes.

## 7. Access the services

### JupyterHub

```bash
kubectl port-forward -n radps-hub svc/proxy-public 9999:80
```

Open `http://localhost:9999`. Authentication uses FirstUseAuthenticator — pick any username and password (8+ characters) on first login.

### Prefect UI

If ingress is configured with `/etc/hosts`, open `http://prefect.noop.local`.

Otherwise, port-forward:

```bash
kubectl port-forward -n prefect-server svc/prefect-server 4200:4200
```

Open `http://localhost:4200`.

## 8. Provision test data

The notebook expects data at `/home/jovyan/shared/data/Antennae_North.cal.lsrk.split.ps.zarr` on the shared PVC. Copy your data onto the volume from within a running Jupyter session, or use `kubectl cp`:

```bash
kubectl cp /path/to/Antennae_North.cal.lsrk.split.ps.zarr \
  radps-hub/<jupyter-pod-name>:/home/jovyan/shared/data/Antennae_North.cal.lsrk.split.ps.zarr
```

## Teardown

```bash
# JupyterHub
kubectl delete -f /tmp/radps-hub-local.yaml --namespace radps-hub
kubectl delete namespace radps-hub

# Prefect Server
kubectl delete -f /tmp/prefect-server-local.yaml --namespace prefect-server
kubectl delete namespace prefect-server

# Dask Operator
helm uninstall dask-kubernetes-operator -n dask-operator
kubectl delete namespace dask-operator
```

## Troubleshooting

### exec format error

An image was built for the wrong architecture. Rebuild with podman on your Mac (arm64) and reload into the VM.

### Pods in wrong namespace

The JupyterHub and Prefect Helm charts omit `namespace` from resource metadata. Always use `kubectl apply --namespace <ns>` when applying the kustomize output.

### Browser redirects on localhost:8080

Port 8080 may be intercepted by browser extensions. Use port 9999 or another port instead.

### Prefect UI says "can't reach localhost"

The `prefectUiApiUrl` in the Prefect values must match how you access the Prefect API from your browser (ingress hostname or port-forward URL).
