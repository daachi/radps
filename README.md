# RADPS
Radio Astronomy Data Processing System

## Workflow

To run the demo pipeline in a python environment with the dependencies installed, it is required to start a couple of background processes. First, to have a prefect server running:

`prefect server start &`

and also, to create the deployments required for running the calibration components in parallel:

`python prefect_workflow/deploy.py &`

The pipeline can then be executed using:

`python prefect_workflow/pipeline.py`

## Cluster infrastructure

### Required software:
- docker
- k3d
- kubectl
- helm

Deploying a local Kubernetes cluster:

```
k3d cluster create --agents 2 --agents-memory=4GB --image=rancher/k3s:v1.31.4-k3s1
```

Installing a basic Prefect deployment:
```
helm repo add prefect https://prefecthq.github.io/prefect-helm
helm install prefect-server prefect/prefect-server
helm install prefect-worker prefect/prefect-worker -f charts/worker-manifest.yaml
```

Exposing dashboard UI on the default port from a localized k8s cluster:
```
kubectl --namespace default port-forward svc/prefect-server 4200:4200 &
```

Now you can interact with the running Prefect service in the normal way:
```
# access the UI
http://127.0.0.1:4200/dashboard
# add a work pool
prefect worker start --pool "Test" &
# create a deployment
python prefect_workflow/deploy.py &
```


