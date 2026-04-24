# kube_ipynb - Kubernetes Notebook Prerequisites

This directory contains Jupyter notebooks that run **inside the RADPS JupyterHub deployment on a Kubernetes cluster**. They cannot be run locally.

## What the notebook does

`kube_prefect_dask_tests.ipynb` creates an on-demand Dask cluster inside Kubernetes using the Dask Kubernetes Operator, connects a client, and runs test workloads including:

- Raw `dask.delayed` tasks that read `.zarr` data from shared storage via `xradio`
- Prefect flows using `DaskTaskRunner` pointed at the dynamically-created Dask scheduler

## Infrastructure requirements

You need a running Kubernetes cluster (k3s) with the following deployed in the `radps-hub` namespace:

### 1. Namespace and RBAC

Create the namespace and apply the service account and roles that allow Jupyter pods to create Dask cluster resources:

```bash
kubectl create namespace radps-hub
kubectl apply -f charts/radps_hub/dask-jupyter-rbac.yaml
```

This creates the `dask-jupyter-role` ServiceAccount with permissions to manage `daskclusters`, `daskworkergroups`, pods, and services in the namespace.

### 2. Longhorn storage

The deployment assumes [Longhorn](https://longhorn.io/) is installed as the storage provisioner (`storageClassName: longhorn`). It is used for:

- JupyterHub's database PVC
- Per-user home directory PVCs (15Gi each)
- The shared data PVC (1Ti)
- The PostgreSQL database backing Prefect Server (30Gi)

### 3. Shared storage PVC

A shared PersistentVolumeClaim provides common data access across Jupyter and Dask worker pods:

```bash
kubectl apply -f charts/radps_hub/common-storage-pvc.yaml
```

This creates `radps-hub-pvc` (1Ti, ReadWriteMany) mounted at `/home/jovyan/shared` in both Jupyter and Dask worker containers.

**Data requirement**: The notebook expects the file `/home/jovyan/shared/data/Antennae_North.cal.lsrk.split.ps.zarr` to exist on this volume. This data must be provisioned manually.

### 4. Network policy

Allow egress from pods in the namespace (required for Dask inter-pod communication and Prefect API access):

```bash
kubectl apply -f charts/radps_hub/jupyter-allow-egress.yaml
```

### 5. Dask Kubernetes Operator

The notebook uses `dask_kubernetes.operator.KubeCluster` to dynamically create Dask clusters as Kubernetes custom resources. Install the operator:

```bash
helm repo add dask https://helm.dask.org/
helm install --create-namespace -n dask-operator dask-kubernetes-operator dask/dask-kubernetes-operator
```

### 6. Prefect Server

Deploy Prefect Server so notebook flows can register and execute:

```bash
helm repo add prefect https://prefecthq.github.io/prefect-helm
helm repo update prefect
helm install prefect-server prefect/prefect-server -n radps-hub -f charts/radps_hub/prefect-server-values.yaml
```

**Note**: `prefect-server-values.yaml` contains a hardcoded IP (`10.2.97.207:30042`) for `prefectUiApiUrl` that will need to be updated for your cluster.

### 7. JupyterHub

Deploy JupyterHub using the Zero to JupyterHub Helm chart:

```bash
helm repo add jupyterhub https://hub.jupyter.org/helm/jupyterhub/
helm repo update jupyterhub
helm install radps-hub jupyterhub/jupyterhub -n radps-hub -f charts/radps_hub/jupyterhub-values.yaml
```

This deploys JupyterHub with:

- **Jupyter image**: `ghcr.io/casangi/radps-jupyter-notebook:v0.0.6` (includes all required Python packages)
- **Authentication**: FirstUseAuthenticator (create-on-first-login, 8+ char passwords)
- **HTTPS**: via TLS secret `jupyterhub-tls` on NodePort 30443
- **`PREFECT_API_URL`** env var set to `http://prefect-server:4200/api` for in-cluster Prefect access
- Shared storage volume mounted at `/home/jovyan/shared`

### 8. Custom container images

Two container images are referenced but **not built from this repository**:

- `ghcr.io/casangi/radps-jupyter-notebook:v0.0.6` — the JupyterHub single-user image (must include `dask_kubernetes`, `prefect`, `prefect_dask`, `xradio`, and standard scientific Python packages)
- `ghcr.io/casangi/radps-dask-worker` — the Dask worker image created dynamically by `KubeCluster` (must include `xradio` and any packages used inside `dask.delayed` calls)

The Dockerfiles for these images are not in this repository. Their contents and build process are undocumented.

## Running the notebook

Once all infrastructure is deployed:

1. Access JupyterHub at `https://<node-ip>:30443`
2. Log in (account created on first use)
3. Open `kube_prefect_dask_tests.ipynb`
4. Run cells sequentially — the notebook will create a Dask cluster, run workloads, and tear it down

## Summary of deployment order

```bash
# 1. Namespace and RBAC
kubectl create namespace radps-hub
kubectl apply -f charts/radps_hub/dask-jupyter-rbac.yaml

# 2. Network policy
kubectl apply -f charts/radps_hub/jupyter-allow-egress.yaml

# 3. Shared storage
kubectl apply -f charts/radps_hub/common-storage-pvc.yaml

# 4. Dask Operator (cluster-wide, separate namespace)
helm install --create-namespace -n dask-operator dask-kubernetes-operator dask/dask-kubernetes-operator

# 5. Prefect Server
helm install prefect-server prefect/prefect-server -n radps-hub -f charts/radps_hub/prefect-server-values.yaml

# 6. JupyterHub
helm install radps-hub jupyterhub/jupyterhub -n radps-hub -f charts/radps_hub/jupyterhub-values.yaml

# 7. Provision data onto the shared PVC (method depends on your environment)
```
