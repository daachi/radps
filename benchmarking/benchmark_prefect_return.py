import time
import numpy as np
import random
import os
import logging
import pandas as pd

from prefect import flow, task
from prefect.task_runners import ThreadPoolTaskRunner
from prefect_dask.task_runners import DaskTaskRunner
from prefect.futures import wait

from benchmark_utils import organize_benchmark_result, generate_run_id, save_timing_results

# Logging setup
logging.getLogger("prefect").setLevel(logging.WARNING)
logging.getLogger("dask").setLevel(logging.WARNING)
logging.getLogger("distributed").setLevel(logging.WARNING)
logger = logging.getLogger("RADPS")
logger.setLevel(logging.INFO)


@task
def sleep_and_generate_data(min_sleep: float, max_sleep: float, data_size_mb: int):
    start_time = time.time()
    duration = random.uniform(min_sleep, max_sleep)
    time.sleep(duration)
    data = np.ones((data_size_mb * 1024 * 1024 // 8,), dtype=np.float64)
    total_time = time.time() - start_time
    return data, total_time


@task
def sum_time_and_data(return_data_1, return_data_2):
    start = time.time()
    data_sum = return_data_1[0] + return_data_2[0]
    time_sum = return_data_1[1] + return_data_2[1]
    time_sum += time.time() - start
    return data_sum, time_sum


# def tree_combine(tasks):
#     while len(tasks) > 1:
#         new_tasks = []
#         for i in range(0, len(tasks), 2):
#             if i < len(tasks) - 1:
#                 combined = sum_time_and_data.submit(tasks[i], tasks[i + 1])
#             else:
#                 combined = tasks[i]
#             new_tasks.append(combined)
#         tasks = new_tasks
#     return tasks[0]

def tree_submit_and_combine(n_tasks, min_sleep, max_sleep, data_size_mb):
    futures = [
        sleep_and_generate_data.submit(min_sleep, max_sleep, data_size_mb)
        for _ in range(n_tasks)
    ]
    while len(futures) > 1:
        next_level = []
        for i in range(0, len(futures), 2):
            if i + 1 < len(futures):
                combined = sum_time_and_data.submit(futures[i], futures[i + 1])
            else:
                combined = futures[i]  # odd one out
            next_level.append(combined)
        futures = next_level
    return futures[0]


def run_tree_benchmark(list_n_tasks, list_sleep_times, data_size_mb, task_runner, runner_name, n_threads_per_process, n_processes, results_csv):
    run_id = generate_run_id()

    @flow(task_runner=task_runner)
    def benchmark_flow(num_tasks, min_sleep, max_sleep):
        start_time = time.time()
        final_result = tree_submit_and_combine(num_tasks, min_sleep, max_sleep, data_size_mb)
        data_sum, t_sum_task_times = final_result.result()
        t_workflow = time.time() - start_time

        result_dict = organize_benchmark_result(
            run_id=run_id,
            workflow_orchestration_framework="prefect",
            workflow_type="tree_combine",
            n_tasks=num_tasks,
            t_min_sleep=min_sleep,
            t_max_sleep=max_sleep,
            t_workflow=t_workflow,
            t_sum_task_times=t_sum_task_times,
            n_threads_per_process=n_threads_per_process,
            n_processes=n_processes,
            runner=runner_name,
            return_size_mb=data_size_mb
        )
        save_timing_results(result_dict, filename=results_csv)

        logger.info(f"Completed run: {result_dict}")
        return result_dict

    for n_tasks in list_n_tasks:
        for sleep_times in list_sleep_times:
            benchmark_flow(n_tasks, sleep_times[0], sleep_times[1])


if __name__ == "__main__":
    list_sleep_times = [(2.0, 2.0)]
    n_threads_per_process = os.cpu_count()
    list_n_tasks = [n_threads_per_process * 4]
    data_size_mb = 10 #MB
    results_csv = "benchmark_results_return_size.csv"

    logger.info("Running with ThreadPoolTaskRunner")
    run_tree_benchmark(
        list_n_tasks=list_n_tasks,
        list_sleep_times=list_sleep_times,
        data_size_mb=data_size_mb,
        task_runner=ThreadPoolTaskRunner(max_workers=n_threads_per_process),
        runner_name="ThreadPoolTaskRunner",
        n_threads_per_process=n_threads_per_process,
        n_processes=1,
        results_csv=results_csv,
    )

    # import dask
    # dask_cluster = dask.distributed.LocalCluster(
    #     n_workers=1,
    #     threads_per_worker=n_threads_per_process,
    # )
    # logger.info("Running with DaskTaskRunner (1 process, threaded)")
    # run_tree_benchmark(
    #     list_n_tasks=list_n_tasks,
    #     list_sleep_times=list_sleep_times,
    #     data_size_mb=data_size_mb,
    #     task_runner=DaskTaskRunner(address=dask_cluster.scheduler_address),
    #     runner_name="DaskTaskRunner",
    #     n_threads_per_process=n_threads_per_process,
    #     n_processes=1,
    #     results_csv=results_csv,
    # )
    # dask_cluster.close()

    # dask_cluster = dask.distributed.LocalCluster(
    #     n_workers=os.cpu_count(),
    #     threads_per_worker=1,
    # )
    # logger.info("Running with DaskTaskRunner (multi-process)")
    # run_tree_benchmark(
    #     list_n_tasks=list_n_tasks,
    #     list_sleep_times=list_sleep_times,
    #     data_size_mb=data_size_mb,
    #     task_runner=DaskTaskRunner(address=dask_cluster.scheduler_address),
    #     runner_name="DaskTaskRunner",
    #     n_threads_per_process=1,
    #     n_processes=os.cpu_count(),
    #     results_csv=results_csv,
    # )
    # dask_cluster.close()