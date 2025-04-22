def save_timing_results(timing_results, filename="timing_results.csv", header=False):
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
    timing_results.to_csv(filename, mode="a", header=header)


def organize_benchmark_result(run_id, workflow_orchestration_framework, workflow_type, n_tasks, t_min_sleep, t_max_sleep, t_workflow, t_sum_task_times, n_threads_per_process, n_processes, runner):
    from datetime import datetime
    import os
    import psutil
    import platform
    import logging
    
    n_parallelism = n_threads_per_process * n_processes
    assert n_parallelism <= os.cpu_count(), "n_parallelism should be less than or equal to the number of CPU cores"
    
    overhead_percentage = 100*(t_workflow - t_sum_task_times/n_parallelism)/(t_sum_task_times/n_parallelism)
    overhead_per_task_seconds = (t_workflow - t_sum_task_times/n_parallelism)/(n_tasks/n_parallelism)
    
    logger = logging.getLogger("RADPS")
    
    # Is this a reasonable threshold?
    if (overhead_per_task_seconds > 0.4) and (n_tasks > 4*n_parallelism): 
        logger.error("Overhead per task is greater than 0.4 seconds: %s", overhead_per_task_seconds)
        raise ValueError("Overhead per task is greater than 0.4 seconds: %s", overhead_per_task_seconds)
    
    benchmark_result = {
            "run_id": run_id,
            "workflow_orchestration_framework": workflow_orchestration_framework,
            "date_and_time": datetime.now().isoformat(),
            "developer": os.getlogin(),
            "system_name": platform.node(),
            "n_tasks": n_tasks,
            "t_min_sleep": t_min_sleep,
            "t_max_sleep": t_max_sleep,
            "t_workflow": t_workflow,
             "t_sum_task_times": t_sum_task_times,
            "t_sum_task_times_per_unit_of_parallelism": t_sum_task_times/n_parallelism,
            "overhead_percentage": overhead_percentage,
            "overhead_per_task_seconds": overhead_per_task_seconds,
            "n_threads_per_process": n_threads_per_process,
            "n_processes": n_processes,  
            "n_parallelism": n_parallelism,
            "runner":runner,
            "backend_database": "postgreSQL",
            "workflow_type": workflow_type, 
            "total_memory": psutil.virtual_memory().total / (1024**3),
        }
    
    
    return benchmark_result 
