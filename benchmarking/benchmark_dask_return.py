import time
import numpy as np
import dask
import pandas as pd
import random
from benchmark_utils import organize_benchmark_result, generate_run_id, save_timing_results, setup_dask

import logging
# Logging setup
# logging.getLogger("dask").setLevel(logging.WARNING)
# logging.getLogger("distributed").setLevel(logging.WARNING)


logger = logging.getLogger("RADPS")
logger.setLevel(logging.INFO)

if not logger.handlers:
    # Add a StreamHandler only if not already added (avoid duplicate logs)
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def sleep_and_generate_data(min_sleep, max_sleep, data_size_mb):
    """Task that sleeps for a random duration and returns generated data and total time."""
    start_time = time.time()
    duration = random.uniform(min_sleep, max_sleep)
    time.sleep(duration)
    data = np.ones((int(data_size_mb * 1024 * 1024 / 8),), dtype=np.float64) # Create numpy array of specified size in MB
    total_time = time.time() - start_time
    return data, total_time

def sum_time_and_data(return_data, input_params):
    """Custom function to sum both data and time."""
    start = time.time()
    data_sum = return_data[0][0] + return_data[1][0]
    time_sum = return_data[0][1] + return_data[1][1]
    time_sum = time_sum + (time.time() - start)
    return data_sum, time_sum


def tree_combine(list_to_combine, reduce_node_task, input_params={}):
    while len(list_to_combine) > 1:
        new_list_to_combine = []
        for i in range(0, len(list_to_combine), 2):
            if i < len(list_to_combine) - 1:
                lazy = dask.delayed(reduce_node_task)(
                    [list_to_combine[i], list_to_combine[i + 1]],
                    dask.delayed(input_params),
                )
            else:
                lazy = list_to_combine[i]
            new_list_to_combine.append(lazy)
        list_to_combine = new_list_to_combine
    return list_to_combine[0]


def run_workflow(num_tasks, min_sleep, max_sleep, data_size_mb):
    """Run a Dask workflow with a given number of tasks."""
    tasks = [
        dask.delayed(sleep_and_generate_data)(
            dask.delayed(min_sleep), dask.delayed(max_sleep), dask.delayed(data_size_mb)
        )
        for _ in range(num_tasks)
    ]

    total_data_time_graph = tree_combine(tasks, sum_time_and_data)

    start_time = time.time()
    total_data_time = dask.compute(total_data_time_graph)[0]
    end_time = time.time()

    T_workflow = end_time - start_time
    print(total_data_time)
    return total_data_time[1], T_workflow, total_data_time[0]

def run_benchmark(list_sleep_times, list_n_tasks, data_size_mb_list, n_processes, n_threads_per_process, results_csv):
    run_id = generate_run_id()
    for n_tasks in list_n_tasks:
        for sleep_times in list_sleep_times:
            for data_size_mb in data_size_mb_list:
                # Run the benchmark flow
                timing_results = run_workflow(n_tasks, sleep_times[0], sleep_times[1], data_size_mb)
       
                result_dict = organize_benchmark_result(
                    run_id=run_id,
                    workflow_orchestration_framework="dask",
                    workflow_type="tree_combine",
                    n_tasks=n_tasks,
                    t_min_sleep=sleep_times[0],
                    t_max_sleep=sleep_times[1],
                    t_workflow=timing_results[1],
                    t_sum_task_times=timing_results[0],
                    n_threads_per_process=n_threads_per_process,
                    n_processes=n_processes,
                    runner="NA",
                    return_size_mb=data_size_mb,
                    wait_for_maping="NA"
                )
                save_timing_results(result_dict, filename=results_csv)
                logger = logging.getLogger("RADPS")
                logger.setLevel(logging.INFO)
                logger.info("t_workflow: %s", result_dict["t_workflow"])
                logger.info("Average t_sum_task_times per unit of parallelism: %s", result_dict["t_sum_task_times"]/result_dict["n_parallelism"]) 
                logger.info("overhead percentage: %s", result_dict["overhead_percentage"])
                logger.info("overhead per task seconds: %s", result_dict["overhead_per_task_seconds"])
                logger.info("Data size per task: %s MB", data_size_mb)
                logger.info("Maximum data size: %s MB", timing_results[2].nbytes / (1024**2))
                logger.info("*"*50)



def run_set_of_benchmarks(list_sleep_times, list_n_tasks, data_size_mb_list, max_parallelism, results_csv):
    import dask
    import dask.distributed
    setup_dask()
    logger.info("#"*50)
    n_processes = 1
    n_threads_per_process = max_parallelism
    dask_cluster = dask.distributed.LocalCluster(
        n_workers=n_processes,
        threads_per_worker=n_threads_per_process,
    )
    client = dask_cluster.get_client()
    print(client)
    print(dask_cluster.dashboard_link)
    run_benchmark(list_sleep_times, list_n_tasks, data_size_mb_list, n_processes, n_threads_per_process, results_csv)
    dask_cluster.close()
    try:
        client = dask.distributed.Client.current()
        client.close()
    except:
        logger.info("No client found.")
        
    logger.info("#"*50)
    n_processes = max_parallelism
    n_threads_per_process = 1
    dask_cluster = dask.distributed.LocalCluster(
        n_workers=n_processes,
        threads_per_worker=n_threads_per_process,
    )
    client = dask_cluster.get_client()
    print(client)
    print(dask_cluster.dashboard_link)
    run_benchmark(list_sleep_times, list_n_tasks, data_size_mb_list, n_processes, n_threads_per_process, results_csv)
    dask_cluster.close()
    try:
        client = dask.distributed.Client.current()
        client.close()
    except:
        logger.info("No client found.")




if __name__ == "__main__":
    
    #To make sure that code is working:
    # import os
    # list_sleep_times = [(2.0, 2.0)]
    # max_parallelism = os.cpu_count()
    # list_n_tasks = [max_parallelism* 4]
    # data_size_mb_list = [10] #MB
    # results_csv="results/benchmark_return_results_dask_test.csv"
    # run_set_of_benchmarks(list_sleep_times, list_n_tasks, data_size_mb_list, max_parallelism, results_csv)
    
    ##Benchmark 1
    print("Doing benchmark 1.")
    import os
    #list_sleep_times = [(1.0,0.1)]
    list_sleep_times = [(6.0,4.0)]
    max_parallelism = os.cpu_count()
    list_n_tasks = [1000, 2000, 4000, 8000, 16000, 32000, 64000, 80000, 128000]
    data_size_mb_list = [0.1] #MB
    results_csv="results/benchmark_return_results.csv"
    run_set_of_benchmarks(list_sleep_times, list_n_tasks, data_size_mb_list, max_parallelism, results_csv)

    # ##Benchmark 2
    # print("Doing benchmark 2.")
    # list_sleep_times = [(4.0,2.0),(16.0,4.0),(32.0,8.0),(128.0,32.0),(256.0,64.0),(512.0,128.0)]
    # list_n_tasks = [max_parallelism*20]
    # data_size_mb_list = [0.1, 1.0, 10.0] #MB
    # results_csv="results/benchmark_return_results.csv"
    # wait_for_maping=True
    # run_set_of_benchmarks(list_sleep_times, list_n_tasks, data_size_mb_list, max_parallelism, results_csv)