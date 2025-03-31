import os

from prefect import serve
from prefect import flow

from core import sleep_placeholder

@flow
def flow_test():
    elapsed = sleep_placeholder(0.01)
    return elapsed

if __name__ == "__main__":

    flow_deploy = flow_test.to_deployment(name="flow overhead")

    serve(
        flow_deploy,
        limit=os.cpu_count()
    )
