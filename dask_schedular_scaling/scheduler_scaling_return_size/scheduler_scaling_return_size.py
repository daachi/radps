import time
import numpy as np
import dask
import pandas as pd
import random


def sleep_and_generate_data(min_sleep, max_sleep, data_size_mb):
    """Task that sleeps for a random duration and returns generated data and total time."""
    start_time = time.time()
    duration = random.uniform(min_sleep, max_sleep)
    time.sleep(duration)
    data = np.ones(
        (data_size_mb * 1024 * 1024 // 8,), dtype=np.float64
    )  # Create numpy array of specified size in MB
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


def main():
    # Define task sizes and data sizes
    task_sizes = [1000, 2000, 4000, 8000, 20000, 40000, 60000, 80000]
    data_sizes = [1, 10, 100, 1000]  # MB
    sleep_ranges = [(0.1, 1.0)]

    # Set up a Dask cluster
    from toolviper.dask import local_client

    n_parallelism = 16
    client = local_client(cores=n_parallelism, memory_limit="20GB")

    # input("Press Enter to continue...")  # Pause execution until Enter is pressed

    # Get max available parallelism
    n_parallelism = len(client.ncores())

    results = []
    for min_sleep, max_sleep in sleep_ranges:
        for data_size in data_sizes:
            for num_tasks in task_sizes:
                T_sum_task_times, T_workflow, sum_data = run_workflow(
                    num_tasks, min_sleep, max_sleep, data_size
                )
                results.append(
                    (
                        num_tasks,
                        min_sleep,
                        max_sleep,
                        data_size,
                        T_sum_task_times,
                        T_workflow,
                        sum_data.size,
                        n_parallelism,
                    )
                )
                print(
                    "num_tasks, min_sleep, max_sleep, T_sum_task_times, T_workflow, n_parallelism",
                    num_tasks,
                    min_sleep,
                    max_sleep,
                    data_size,
                    T_sum_task_times,
                    T_workflow,
                    sum_data.size,
                    n_parallelism,
                )
                # input("Press Enter to continue...")  # Pause execution until Enter is pressed

    # Save results to a DataFrame and write to disk
    df = pd.DataFrame(
        results,
        columns=[
            "num_tasks",
            "min_sleep",
            "max_sleep",
            "data_size_mb",
            "T_sum_task_times",
            "T_workflow",
            "sum_data_size",
            "n_parallelism",
        ],
    )
    df.to_csv("scheduler_scaling_return_size_results.csv", index=False)

    client.shutdown()
    client.close()


if __name__ == "__main__":
    main()
