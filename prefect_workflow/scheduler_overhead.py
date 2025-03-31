from prefect import serve
from prefect import task, flow

from core import sleep_placeholder

@flow
def flow_test():
    sleep_placeholder(1.0)
    return True

if __name__ == "__main__":

    task_sizes = [1000, 2000, 4000, 8000, 20000, 40000, 60000, 80000]
    
    for nn in task_sizes:
        flow_deploy = flow_test.to_deployment(name="flow overhead")

    serve(flow_deploy)
