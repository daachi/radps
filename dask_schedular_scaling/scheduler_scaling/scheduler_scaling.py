import time
import numpy as np
import dask
import distributed
import pandas as pd
import random


def sleep_task(min_sleep, max_sleep):
    """Task that sleeps for a random duration between min_sleep and max_sleep."""
    duration = random.uniform(min_sleep, max_sleep)
    time.sleep(duration)
    return duration


def run_workflow(num_tasks, min_sleep, max_sleep):
    """Run a Dask workflow with a given number of tasks."""
    tasks = [dask.delayed(sleep_task)(min_sleep, max_sleep) for _ in range(num_tasks)]

    start_time = time.time()
    task_times = dask.compute(*tasks)
    end_time = time.time()

    T_sum_task_times = sum(task_times)
    T_workflow = end_time - start_time

    return T_sum_task_times, T_workflow


def main():
    # Define task sizes
    task_sizes = [1000, 2000, 4000, 8000, 20000, 40000, 60000, 80000]
    sleep_ranges = [(0.001, 0.01), (0.01, 0.1), (0.1, 1.0)]

    # Set up a Dask cluster
    from toolviper.dask import local_client

    n_parallelism = 16
    client = local_client(cores=n_parallelism, memory_limit="3GB")
    cluster = distributed.Client.current().cluster

    print("n_parallelism", n_parallelism)

    results = []
    for min_sleep, max_sleep in sleep_ranges:
        for num_tasks in task_sizes:
            # print(f"Running workflow with {num_tasks} tasks, sleep range ({min_sleep}, {max_sleep})...")
            T_sum_task_times, T_workflow = run_workflow(num_tasks, min_sleep, max_sleep)
            print(
                "num_tasks, min_sleep, max_sleep, T_sum_task_times, T_workflow, n_parallelism",
                num_tasks,
                min_sleep,
                max_sleep,
                T_sum_task_times,
                T_workflow,
                n_parallelism,
            )
            results.append(
                (
                    num_tasks,
                    min_sleep,
                    max_sleep,
                    T_sum_task_times,
                    T_workflow,
                    n_parallelism,
                )
            )

    # Save results to a DataFrame and write to disk
    df = pd.DataFrame(
        results,
        columns=[
            "num_tasks",
            "min_sleep",
            "max_sleep",
            "T_sum_task_times",
            "T_workflow",
            "n_parallelism",
        ],
    )
    df.to_csv("scheduler_scaling_results.csv", index=False)

    client.shutdown()
    client.close()


if __name__ == "__main__":
    main()
