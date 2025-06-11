from prefect_dask import DaskTaskRunner
import dask.distributed


def connect_to_scheduler(**kwargs):
    """
    Connect to an existing dask.distributed.Client served on localhost port 8080 (e.g., via `kubectl port-forward svc/dask-scheduler $DASK_SCHEDULER_PORT:8786 &`)
    Failing that, create an ephemeral LocalCluster

    Return a DaskTaskRunner
    """

    # try:
    #     print("Attempting to connect pipeline to existing resource manager")
    #     client = dask.distributed.Client("tcp://127.0.0.1:8080", timeout=5)
    #     tr = DaskTaskRunner(address=client.scheduler.address)
    #     ## Cf.
    #     # cluster = dask.distributed.LocalCluster()
    #     # client = cluster.get_client()
    # except:
    #     print("No Dask helm deployment detected at expected address")

    #     dask_cluster = dask.distributed.LocalCluster(
    #         **kwargs,
    #     )
    #     tr = DaskTaskRunner(
    #         address=dask_cluster.scheduler_address #Don't pass cluster itself, causes error with serialization. 
    #     )
    
    print("No Dask helm deployment detected at expected address")

    # dask_cluster = dask.distributed.LocalCluster(
    #     **kwargs,
    # )
    # # client = dask.distributed.Client(
    # #     dask_cluster,
    # # )
    
    # tr = DaskTaskRunner(
    #     address=dask_cluster.scheduler_address #Don't pass cluster itself, causes error with serialization. 
    # )
    
    tr = DaskTaskRunner(
        cluster_class=dask.distributed.LocalCluster,
        cluster_kwargs=kwargs,
    )
    

    return tr
