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

Executables for these packages need to be installed on each machine that will be part of a local Kubernetes deployment (i.e., developer machine). For more information about the structure of these test deployments, see the project wiki page: https://github.com/casangi/RADPS/wiki/Kubernetes

Note: User account performing these installation steps must be a `sudo`er on the machine.

#### Install instructions (mac)
Installation of the required packages has been tested using [macports](https://www.macports.org/) on Sonoma 14.7 (Apple M3 Pro). Having this tool installed and pre-configured is a requirement for following the rest of these instructions. It may also be possible to build from source or use alternative installation methods (for instance, homebrew).

The first step is to make sure you have [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) (engine + virtualization for macs) installed and running on your machine. Next,
```
sudo port install k3d
sudo port install kubectl
sudo port select --set kubectl kubectl1.31
sudo port install helm-3.16
sudo port select --set helm helm3.16
```
Make sure the executables are on your PATH, by running the `k3d version`, `kubectl version`, and `helm version` commands.

#### Install instructions (RHEL8)
Installation of the required packages has been tested on a RHEL8 workstation inside NRAO-CV. These steps require a package manager configured with standard repositories. Installation of [Docker engine](https://docs.docker.com/engine/install/rhel/) is required, but on linux we can get by with just dockerd (Docker Desktop is not necessary, as with mac installation).
```
sudo yum install docker-ce.x86_64
sudo systemctl start docker
# make sure the service is running with something like
docker ps -a
```
Next, the other required packages:
```
wget -q -O - https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
wget https://get.helm.sh/helm-v3.17.0-linux-amd64.tar.gz
tar -xvf helm-v3.17.0-linux-amd64.tar.gz
# put the output somewhere on the PATH
helm repo add "stable" "https://charts.helm.sh/stable"
```

### Deploying a local Kubernetes cluster:
Using k3d to spin up a cluster is as quick and easy as running a command like this:
```
k3d cluster create --agents 2 --agents-memory=4GB --image=rancher/k3s:v1.31.4-k3s1
```
After a short while, the cluster will have been created and can be interacted with in the normal way using kubectl:
```
# to examine some properties of the running cluster
kubectl cluster-info
kubectl get nodes
kubectl get pods
kubectl get svc
```

Installing a basic Prefect deployment onto this local Kubernetes cluster is similarly straightforward using helm:
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
# run the example pipeline
python prefect_workflow/pipeline.py
```