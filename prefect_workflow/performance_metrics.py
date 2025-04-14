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
