import os
from prefect import task, flow, exceptions
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.deployments import run_deployment

from core import sleep_placeholder

@task
def task_test(max_time=0.01):
    elapsed = sleep_placeholder(max_time)
    return elapsed

@flow
def flow_scaling_test(number_of_subflows):
    """
    Note: depends on scheduler_deploy.py having already been invoked, running as a background process.
    """
    print(f"Working on {number_of_subflows} flows in this flow invocation")
    for tt in range(0, number_of_subflows):
        run_deployment(
            name="flow-test/flow overhead",
            parameters={},
            timeout=0
        )

@flow(task_runner=ThreadPoolTaskRunner(max_workers=os.cpu_count()))
def task_scaling_test(number_of_tasks=1000):
    print(f"Working on {number_of_tasks} tasks in this flow invocation")
    results = []
    for num_tasks in range(0, number_of_tasks):
        results.append(task_test.submit(0.1))
    wait(results)
    return True

if __name__ == "__main__":

    print("Running flow_scaling_test")
    try:
        sizes = [1000, 2000, 4000, 8000, 16000]
        for size in sizes:
            flow_scaling_test(size)
    except exceptions.ObjectNotFound:
        print("Failed to find deployment expected by running scheduler_deploy.py")
        print("Skipping the rest of flow_scaling_test and proceeding")

    print("Running task_scaling_test")
    sizes = [1000, 2000, 4000, 8000, 16000, 32000, 64000, 100000]    
    for size in sizes:
        task_scaling_test(size)    
