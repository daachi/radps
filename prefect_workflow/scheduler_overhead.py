import os
import time
import pandas as pd
import psutil
import platform
import inspect

from datetime import datetime
from prefect import task, flow, exceptions
from prefect.futures import wait
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.deployments import run_deployment
from prefect.artifacts import create_table_artifact
from prefect.context import get_run_context
from prefect.logging import get_run_logger

from core import sleep_placeholder


@task
def task_test(min_time, max_time):
    elapsed = sleep_placeholder(max_time, min_sleep=min_time)
    return elapsed


@flow(log_prints=True)
def flow_scaling_test(number_of_subflows, min_time=0.001, max_time=0.01):
    """
    Note: depends on scheduler_deploy.py having already been invoked, running as a background process.
    """
    print(f"Working on {number_of_subflows} flows in this flow invocation")
    start = time.time()
    results = []
    for tt in range(0, number_of_subflows):
        flow_run = run_deployment(name="flow-test/flow-overhead", parameters={"min_time": min_time, "max_time": max_time}, timeout=0)
        results.append(flow_run)
    end = time.time()

    # Trying to figure out how to get the actual return value from the deployed flow
    actual_results = [flow_run.state for flow_run in results]
    print(actual_results)
    
    # hardcode to zero for now
#    T_sum_flow_times=sum(actual_results)
    T_sum_flow_times = 0
    
    T_workflow = end - start

    pc = get_run_context()

    timing_results = [
        {
            "date_and_time": datetime.now().isoformat(),
            "developer": os.getlogin(),
            "system_name": platform.node(),
            "workflow": inspect.stack()[0][3],
            "n_subflows": number_of_subflows,  # NOTE: n_subflows rather than n_tasks
            "min_sleep": min_time,
            "max_sleep": max_time,
            "Wall clock time": T_workflow,
            "Sum of sleep times": T_sum_flow_times,  # FIXME: this is clearly incorrect (always 0)
            "n_threads": os.cpu_count(),
            "n_processes": 1,  # think this should always be 1
            "n_parallelism": os.cpu_count(),
            "runner": str(type(pc.task_runner)),
            "workflow_orchestration_framework": "prefect",
            "backend_database": "postgres",
            "workflow_type": "flow",  # populate via argument?
            "total_memory": psutil.virtual_memory().total / (1024**3),
        }
    ]
    create_table_artifact(table=timing_results)
    return timing_results


@flow(log_prints=True, task_runner=ThreadPoolTaskRunner(max_workers=os.cpu_count()))
def task_scaling_test(number_of_tasks=1000, min_time=0.001, max_time=0.01):

    print(f"Working on {number_of_tasks} tasks in this flow invocation")

    start = time.time()
    results = []
    for num_tasks in range(0, number_of_tasks):
        duration = task_test.submit(0.001, 0.01)
        results.append(duration.result())
    end = time.time()

    T_sum_task_times = sum(results)

    T_workflow = end - start

    pc = get_run_context()

    timing_results = [
        {
            "date_and_time": datetime.now().isoformat(),
            "developer": os.getlogin(),
            "system_name": platform.node(),
            "workflow": inspect.stack()[0][3],
            "n_tasks": number_of_tasks,
            "min_sleep": min_time,
            "max_sleep": max_time,
            "Wall clock time": T_workflow,
            "Sum of sleep times": T_sum_task_times,
            "n_threads": os.cpu_count(),
            "n_processes": 1,  # think this should always be 1
            "n_parallelism": os.cpu_count(),
            "runner": str(type(pc.task_runner)),
            "workflow_orchestration_framework": "prefect",
            "backend_database": "postgres",
            "workflow_type": "task",  # populate via argument?
            "total_memory": psutil.virtual_memory().total / (1024**3),
        }
    ]
    create_table_artifact(table=timing_results)
    return timing_results


def save_timing_results(timing_results, filename="timing_results.csv"):
    """Save timing results to a csv file.
    Will append to an existing file if present.

    The input expected is a dataframe with the following contents:
        date_and_time
        developer (name of developer)
        system_name (machine/deployment name for example cvpost018)
        workflow
        n_tasks
        min_sleep
        max_sleep
        T_sum_task_times (sum of individual sleep times)
        T_workflow
        n_threads (total number of threads)
        n_processes (total number of processes)
        n_parallelism (number of cores used and the following should be true n_processes = n_threads x n_processes)
        runner (ThreadPoolTaskRunner, DaskTaskRunner, RayTaskRunner)
        workflow_orchestration_framework (prefect, airflow, dask etc)
        backend_database (postgress)
        workflow_type (prefect: sub-flow, task)
        total_memory (GB)
    """
    timing_results.to_csv(filename, mode="a", header=False)


if __name__ == "__main__":

    print("Running flow_scaling_test")
    try:
        sizes = [1000, 2000, 4000, 8000, 16000]

        overall_flow_scaling_results = []
        for size in sizes:
            timings = flow_scaling_test(size, 0.001, 0.01)
            save_timing_results(pd.DataFrame.from_dict(timings))
            overall_flow_scaling_results.append(timings[0])

    except exceptions.ObjectNotFound:
        print("Failed to find deployment expected by running scheduler_deploy.py")
        print("Skipping the rest of flow_scaling_test and proceeding")

    # Artifact for overall flow_scaling_test results:
    create_table_artifact(
         table=overall_flow_scaling_results, key="flow-scaling-results"
    )

    print("Running task_scaling_test")
    sizes = [1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000]
    overall_task_scaling_results = []

    for size in sizes:
        timing_results = task_scaling_test(size, 0.001, 0.01)
        save_timing_results(pd.DataFrame.from_dict(timing_results))
        overall_task_scaling_results.append(timing_results[0])

    # Artifact for overall task_scaling_test results:
    create_table_artifact(
        table=overall_task_scaling_results, key="task-scaling-results"
    )
