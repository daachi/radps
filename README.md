# RADPS
Radio Astronomy Data Processing System

## Prefect workflow 
To run the demo pipeline in a python environment with the dependencies installed, it is required to start a couple of background processes. First, to have a prefect server running:

`prefect server start &`

and also, to create the deployments required for running the calibration components in parallel:

`python prefect_workflow/deploy.py &`

The pipeline can then be executed using:

`python prefect_workflow/pipeline.py`

## Airflow Workflow
1. Set up Airflow. Recommendation: use the [docker-compose](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/pipeline.html) setup described in Airflow tutorial documentation. 
2. Clone this repo and update the Airflow configuration file `airflow.cfg` to use the `airflow_workflow/dags/` directory in the repo as its `dags_folder`.
3. Enable a DAG in the Airflow UI, and it will run on the schedule defined in the definition file. Also, optionally re-parse and trigger the the DAG via the command line or UI.

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

#### Deploying a local Kubernetes cluster:
Using k3d to spin up a cluster is as quick and easy as running a command like this:
```
k3d cluster create --agents 2 --agents-memory=4GB --image=rancher/k3s:v1.31.4-k3s1
```
After a short while, the cluster will have been created and can be interacted with in the normal way using kubectl:
```
# to examine some properties of the running cluster
kubectl cluster-info
kubectl get nodes
kubectl get svc
kubectl describe pods
```

#### Dask
Installing a basic Dask deployment onto a local Kubernetes cluster can be accomplished using helm to pull down the chart published by dask, and applying some configuration changes using the YAML files stored in the charts area of this repository:
```
helm repo add dask https://helm.dask.org/
helm install dask dask/dask -f charts/prefect/dask-values.yaml
```
This creates Pods containing a basic Dask deployment: a scheduler, its dashboard, and some workers, all communicating with each other over TCP. Since this deployment is running inside of the containers spawned by k3d, it's convenient to forward outside the k3d cluster the ports at which the scheduler and its dashboard UI services are exposed. The commands to do this are conveniently reported by helm when the chart installs, but you can see them again by running `helm status dask`:
```
kubectl port-forward --namespace default svc/dask-scheduler $DASK_SCHEDULER_PORT:8786 &
kubectl port-forward --namespace default svc/dask-scheduler $DASK_SCHEDULER_UI_PORT:80 &
```
Now the scheduler UI can be opened in a browser window (with the current settings in charts/prefect/dask-values.yaml, the address will be http://localhost:$DASK_SCHEDULER_UI_PORT) without having to tunnel onto the k3d cluster.

#### Prefect
Installing a basic Prefect deployment onto a local Kubernetes cluster is similarly straightforward using helm:
```
helm repo add prefect https://prefecthq.github.io/prefect-helm
helm install prefect-server prefect/prefect-server
```
Exposing dashboard UI on the default port from a localized k8s cluster::
```
kubectl port-forward --namespace default svc/prefect-server 4200:4200 &
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

#### Airflow
Installing a basic Airflow deployment onto a local Kubernetes cluster can also be accomplished simply by using the public helm charts, following the [official documentation](https://airflow.apache.org/docs/helm-chart/stable/index.html#installing-the-chart):
```
helm repo add apache-airflow https://airflow.apache.org
helm repo update apache-airflow
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace
```
To make DAGs from a specific branch of this repository available within the containers running the airflow service, we can modify the default deployment using additional settings, like this:
```
helm upgrade --install airflow apache-airflow/airflow --namespace airflow --set dags.persistence.enabled=false --set dags.gitSync.enabled=true --set dags.gitSync.repo=https://github.com/casangi/RADPS.git --set dags.gitSync.branch=my-branch-name
```

### Deploying on `radps-k3s`
In order to issue commands to the `radps-k3s` API server, the command line tools `kubectl` and `helm` must be installed, and the KUBECONFIG environment variable must be properly set to point at the YAML file containing a valid [certificate key](https://docs.k3s.io/cluster-access) that controls access to the shared k3s cluster.

For each deployment on shared infrastructure it is important to isolate activity to a specific context. 
```
kubectl create namespace my-deployment-namespace
kubectl config set-context --current --namespace my-deployment-namespace
```
Alternatively, append all the `helm` `kubectl` commands with `--namespace my-deployment-namespace` if working across multiple namespaces.

#### Prefect

Install using public helm charts, overriding certain settings with those tracked by the chart configs in this repository:
```
helm repo add prefect https://prefecthq.github.io/prefect-helm
helm repo update prefect
helm upgrade --install prefect-server prefect/prefect-server --set server.uiConfig.prefectUiApiUrl="http://prefect.local/api" --set ingress.enabled=true --set ingress.servicePort=prefect-ingress --set backgroundServices.runAsSeparateDeployment=true
```
Prefect can be configured to interact with OS-native, locally hosted (k3d), and shared infrastructure deployments depending on context. For more information on how to do this, see the [prefect documentation](https://docs.prefect.io/v3/how-to-guides/configuration/manage-settings) and our [Kubernetes wiki page](https://github.com/casangi/RADPS/wiki/Kubernetes#configure-prefect-for-local-and-remote-development). Briefly, to set up remote access to this deployment on shared infrastructure, you can create an [ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/):
```
kubectl apply -f charts/prefect/ingress.yaml
```
Then, edit /etc/hosts on the developer machine to match spec.rules.host in [charts/prefect/ingress.yaml](https://github.com/casangi/RADPS/blob/main/charts/prefect/ingress.yaml), i.e., an IP address entry for each EXTERNAL_IP exposed through the traefik proxy (visible via `kubectl get ingress`), each named prefect.local.

##### A note on resource management

The Prefect example pipeline has been configured to run with [Dask task_runners](https://prefecthq.github.io/prefect-dask/task_runners/), as well as [static deployments](https://docs.prefect.io/v3/concepts/deployments#static-infrastructure) using the `serve()` method (see [prefect_workflow/scheduler_deploy.py](https://github.com/casangi/RADPS/blob/main/prefect_workflow/scheduler_deploy.py)). If we wanted to reconfigure the example pipeline to make use of [prefect-kubernetes Workers](https://docs.prefect.io/v3/concepts/workers), we could extend our deployment again using a public helm chart modified with some custom values:
```
helm upgrade --install prefect-worker prefect/prefect-worker -f charts/prefect/worker-manifest.yaml
```
Connecting to an external resource manager (e.g., an existing dask deployment in a separate namespace) is also possible, but would require modifications to the pipeline ([prefect_workflow/resource_management.py](https://github.com/casangi/RADPS/blob/main/prefect_workflow/resource_management.py)).

#### Airflow
Earlier deployments used the charts in the [airflow repo](https://github.com/apache/airflow/blob/main/chart/values.yaml) cloned from source, because a 3.0-compatible version of the helm chart hadn't yet been released. Now that the [1.17](https://airflow.apache.org/docs/helm-chart/stable/release_notes.html#airflow-helm-chart-1-17-0-2025-06-21) chart release is out, we can follow [the documentation](https://airflow.apache.org/docs/helm-chart/stable/index.html#installing-the-chart) tagged to a specific version:
```
# isolate our activity to a specific context
kubectl create namespace my-deployment-namespace
kubectl config set-context --current --namespace my-deployment-namespace

# then, assuming you are in the top level of a clone of the RADPS repo, add and install the chart
# overriding certain settings with those tracked by the chart configs in this repository
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install my-airflow-deployment apache-airflow/airflow --version 1.17 -f ./charts/airflow/values.yaml
```
After a while the components of the airflow deployment should finish initializing on the Kubernetes cluster. If you want to see more progress output, you can add the `--debug` flag to the helm install command. There are some open issues related to user creation and other Airflow 2 -> 3 configuration changes (see wiki for details) so it may be necessary to wait for the `my-airflow-deployment-create-user` pod to get out of a CrashLoopBackoff state, which could take a few minutes.

Once the deployment is stable (verified by inspection, e.g., via `kubectl get pods` and `helm status my-airflow-deployment`), the next step is to enable ingress by setting up a load balancer. (Technically this can be done beforehand too, as these services are isolated from one another, but it helps to make sure we have a working deployment before trying to access it.) By default the pods comprising the Airflow deployment are appended with UUIDs, so the actual pod names will be unique to your deployment, and the port to which you map the internal service is of course configurable, although the internal target should remain same.
```
UI_NAME=$(kubectl get deployments | grep api-server | cut --fields 1 --delimiter " ")
# check port availablity
kubectl get svc --all-nameservices
UI_PORT=8383 # this must be a port not already in use by another service, or the load balancer will get stuck in "Pending"
kubectl expose deployment $UI_NAME --port $UI_PORT --target-port 8080 --name=airflow-load-balancer --type=LoadBalancer
```
Now it should be possible to access the Airflow web service from the UI_PORT at one of the external IP addresses listed by `kubectl get svc airflow-load-balancer`, using the credentials accessed from the pod running the API server:
```
kubectl exec --stdin --tty  $UI_PODNAME -- /bin/bash
cat simple_auth_manager_passwords.json.generated
```
The airflow.cfg is similarly accessible from the working directory of each pod.
