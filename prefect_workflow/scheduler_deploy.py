import os

from prefect import serve
from prefect import flow

from core import sleep_placeholder

@flow
def flow_test(max_time=0.01):
    elapsed = sleep_placeholder(max_time)
    return elapsed

if __name__ == "__main__":

    flow_deploy = flow_test.to_deployment(name="flow overhead")

    serve(
        flow_deploy,
        limit=os.cpu_count()
    )
