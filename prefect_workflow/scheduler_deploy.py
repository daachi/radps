from prefect import serve
from prefect import flow

from core import sleep_placeholder

@flow
def flow_test():
    elapsed = sleep_placeholder(1.0)
    return elapsed

if __name__ == "__main__":

    flow_deploy = flow_test.to_deployment(name="flow overhead")

    serve(flow_deploy)
