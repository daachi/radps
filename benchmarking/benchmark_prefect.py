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

from radps.prefect_workflow.performance_metrics import save_timing_results
from radps.prefect_workflow.core import sleep_placeholder
from radps.prefect_workflow.resource_management import connect_to_scheduler

import logging

# # Increase log level here so global settings are consistent across test envs
logging.getLogger("prefect").setLevel(logging.WARNING)
logging.getLogger("dask").setLevel(logging.WARNING)
logging.getLogger("distributed").setLevel(logging.WARNING)


from benchmark_utils import organize_benchmark_result


sleep_task = task(sleep_placeholder)

def schedular_benchmarks_single_run_id(list_n_tasks,list_sleep_times,n_threads_per_process,n_processes,runner):
    
    logger = logging.getLogger("RADPS")
    
    for n_tasks in list_n_tasks:
        for sleep_times in list_sleep_times:
            t_min_sleep = sleep_times[0]
            t_max_sleep = sleep_times[1]

            
            # Save the results
            run_id = 1
            
            start = time.time()
            duration_futures = []
            for i in range(n_tasks):
                duration_future = sleep_task.submit(t_max_sleep, t_min_sleep)
                duration_futures.append(duration_future)
            wait(duration_futures)
            # Retrieve the results from the completed futures
            task_durations = [future.result() for future in duration_futures]
            t_sum_task_times = sum(task_durations)
            t_workflow = time.time() - start

            
            # Save the results to a csv file
            result_dict = organize_benchmark_result(run_id=run_id, 
                                      workflow_orchestration_framework="prefect", 
                                      workflow_type="task", 
                                      n_tasks=n_tasks, 
                                      t_min_sleep=t_min_sleep, 
                                      t_max_sleep=t_max_sleep, 
                                      t_workflow=t_workflow, 
                                      t_sum_task_times=t_sum_task_times, 
                                      n_threads_per_process=n_threads_per_process, 
                                      n_processes=n_processes, 
                                      runner=runner)
            
            logger.debug(f"Result dict: {result_dict}")
    
            # What you should expect for t_workflow when sleep_times > 0.1s
            # t_workflow ~ (n_tasks/n_parallelism)* (t_max_sleep + t_min_sleep)/2
            logger.debug("t_workflow: %s", t_workflow)
            logger.debug("Average t_sum_task_times per unit of parallelism: %s", t_sum_task_times/result_dict["n_parallelism"]) 
            logger.debug("overhead percentage: %s", result_dict["overhead_percentage"])
            logger.debug("overhead per task seconds: %s", result_dict["overhead_per_task_seconds"])
            
            logger.debug("*"*50)
            
            # Save the results to a csv file
            
def run_prefect_schedular_benchmarks_threadpool(list_n_tasks, list_sleep_times,n_threads_per_process):
    
    logger = logging.getLogger("RADPS")
    logger.debug("Running Prefect Scheduler Benchmarks")

    #Tests the ThreadPoolTaskRunner
    test_flow = flow(name="schedular overhead",task_runner=ThreadPoolTaskRunner(max_workers=os.cpu_count()))(schedular_benchmarks_single_run_id)
    test_flow(list_n_tasks=list_n_tasks,list_sleep_times=list_sleep_times,n_threads_per_process=n_threads_per_process,n_processes=1,runner="ThreadPoolTaskRunner")

def run_prefect_schedular_benchmarks_dask(list_n_tasks, list_sleep_times,n_threads_per_process, n_processes):
    
    logger = logging.getLogger("RADPS")
    logger.debug("Running Prefect Scheduler Benchmarks")
    import dask
    dask_cluster = dask.distributed.LocalCluster(
        n_workers=n_processes,
        threads_per_worker=n_threads_per_process,
    )

    task_runner = DaskTaskRunner(
        address=dask_cluster.scheduler_address,
    )
    
    test_flow = flow(name="schedular overhead", task_runner=task_runner)(schedular_benchmarks_single_run_id)
    test_flow(list_n_tasks=list_n_tasks, list_sleep_times=list_sleep_times, n_threads_per_process=n_threads_per_process, n_processes=n_processes, runner="DaskTaskRunner")
    
    task_runner.client.close()
    dask_cluster.close()

    
if __name__ == "__main__":
    logger = logging.getLogger("RADPS")
    logger.level = logging.DEBUG
    
    list_n_tasks = [48]
    list_sleep_times = [(1.0,1.0)]
    n_threads_per_process = os.cpu_count()
    run_prefect_schedular_benchmarks_threadpool(list_n_tasks, list_sleep_times, n_threads_per_process)
    
    n_processes = 1
    run_prefect_schedular_benchmarks_dask(list_n_tasks, list_sleep_times, n_threads_per_process, n_processes)
    
    n_processes = os.cpu_count()
    n_threads_per_process = 1
    run_prefect_schedular_benchmarks_dask(list_n_tasks, list_sleep_times, n_threads_per_process, n_processes)
