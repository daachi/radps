import asyncio
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
from prefect.flow_runs import wait_for_flow_run
from prefect_dask.task_runners import DaskTaskRunner

from performance_metrics import save_timing_results

from core import sleep_placeholder
from resource_management import connect_to_scheduler

import logging

# Increase log level here so global settings are consistent across test envs
logging.getLogger("prefect").setLevel(logging.WARNING)
logging.getLogger("dask").setLevel(logging.WARNING)
logging.getLogger("distributed").setLevel(logging.ERROR)


@task
def task_test(min_time, max_time):
    elapsed = sleep_placeholder(max_time, min_sleep=min_time)
    return elapsed


@flow(log_prints=True)
async def flow_scaling_test(number_of_subflows, min_time=0.001, max_time=0.01):
    """
    Note: depends on scheduler_deploy.py having already been invoked, running as a background process.
    """
    print(f"Working on {number_of_subflows} flows in this flow invocation")
    start = time.time()
    sub_flows = []
    for tt in range(0, number_of_subflows):
        sub_flows.append(
            await run_deployment(
                name="flow-test/flow-overhead",
                parameters={"min_time": min_time, "max_time": max_time},
                timeout=0,
            )
        )
    end = time.time()

    subflows = [
        wait_for_flow_run(flow_run.id, poll_interval=5) for flow_run in sub_flows
    ]

    # results are the FlowRun objects for each sub-flow
    results = await asyncio.gather(*subflows)
    results = [await flow_run.state.result() for flow_run in results]

    T_sum_flow_times = sum(results)

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
            "Sum of sleep times": T_sum_flow_times,
            "n_threads": 1,
            "n_processes": os.cpu_count(),
            "n_parallelism": os.cpu_count(),
            "runner": str(type(pc.task_runner)),
            "workflow_orchestration_framework": "Prefect",
            "backend_database": "postgreSQL",
            "workflow_type": "flow",  # populate via argument?
            "total_memory": psutil.virtual_memory().total / (1024**3),
        }
    ]
    await create_table_artifact(table=timing_results)
    return timing_results


def task_scaling_test(number_of_tasks=1000, min_time=0.001, max_time=0.01):

    print(f"Working on {number_of_tasks} tasks in this flow invocation")

    start = time.time()
    results = []
    for num_tasks in range(0, number_of_tasks):
        duration = task_test(0.001, 0.01)
        results.append(duration)
    end = time.time()

    T_sum_task_times = sum(results)

    T_workflow = end - start

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
            "n_processes": 1,  # think this should always be 1 for the task case
            "n_parallelism": os.cpu_count(),
            "workflow_orchestration_framework": "Prefect",
            "backend_database": "postgreSQL",
            "workflow_type": "task",  # populate via argument?
            "total_memory": psutil.virtual_memory().total / (1024**3),
        }
    ]

    return timing_results


if __name__ == "__main__":

    print("Running flow_scaling_test")
    try:
        sizes = [30, 100, 300, 1000]
        overall_flow_scaling_results = []
        for size in sizes:
            timings = asyncio.run(flow_scaling_test(size, 0.001, 0.01))
            save_timing_results(pd.DataFrame.from_dict(timings))
            overall_flow_scaling_results.append(timings[0])

    except exceptions.ObjectNotFound:
        print("Failed to find deployment expected by running scheduler_deploy.py")
        print("Skipping the rest of flow_scaling_test and proceeding")

    # Artifact for overall flow_scaling_test results:
    create_table_artifact(
        table=overall_flow_scaling_results, key="flow-scaling-results"
    )

    print("Running task_scaling_test for two different task_runners")
    # create this connection inside __main__ for multiprocessing safety
    # ref. https://prefecthq.github.io/prefect-dask/task_runners/
    dtr = connect_to_scheduler()
    ttr = ThreadPoolTaskRunner(max_workers=os.cpu_count())

    runners = [ttr, dtr]

    for runner in runners:
        if isinstance(runner, ThreadPoolTaskRunner):

            sizes = [1000, 2000, 4000, 8000, 16000, 32000, 64000, 80000, 128000]

        if isinstance(runner, DaskTaskRunner):

            sizes = [1000, 2000, 4000, 8000, 16000, 32000, 64000, 80000, 128000]

        @flow(task_runner=runner)
        def task_scaling_flow(ntasks) -> dict:
            ret = task_scaling_test(ntasks, 0.001, 0.01)
            create_table_artifact(table=ret)
            return ret

        print(f"Running task_scaling_test with {type(runner)}")
        overall_task_scaling_results = []

        for size in sizes:
            timing_results = task_scaling_flow(size)
            timing_results[0]["runner"] = type(runner)
            save_timing_results(pd.DataFrame.from_dict(timing_results))
            overall_task_scaling_results.append(timing_results[0])
