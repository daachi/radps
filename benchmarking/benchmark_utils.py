import os
from datetime import datetime
import pandas as pd
import logging

def save_timing_results(timing_results, filename="timing_results.csv"):
    """Save timing results to a csv file.
    Will append to an existing file if present.

    """
    file_exists = os.path.exists(filename)
    result_df = pd.DataFrame.from_dict([timing_results])
    result_df.to_csv(filename, mode="a", header=not file_exists, index=False)


def organize_benchmark_result(run_id, workflow_orchestration_framework, workflow_type, n_tasks, t_min_sleep, t_max_sleep, t_workflow, t_sum_task_times, n_threads_per_process, n_processes, runner, return_size_mb=None, wait_for_maping=None):
    import psutil
    import platform
    import logging
    
    n_parallelism = n_threads_per_process * n_processes
    assert n_parallelism <= os.cpu_count(), "n_parallelism should be less than or equal to the number of CPU cores"
    
    overhead_percentage = 100*(t_workflow - t_sum_task_times/n_parallelism)/(t_sum_task_times/n_parallelism)
    overhead_per_task_seconds = (t_workflow - t_sum_task_times/n_parallelism)/(n_tasks/n_parallelism)
    logger = logging.getLogger("RADPS")
    
    # Is this a reasonable threshold?
    if (overhead_per_task_seconds > 0.8) and (n_tasks > 4*n_parallelism): 
        logger.error("Overhead per task is greater than 0.8 seconds: %s", overhead_per_task_seconds)
    #     raise ValueError("Overhead per task is greater than 0.8 seconds: %s", overhead_per_task_seconds)
    
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
            "backend_database": get_database_type(workflow_orchestration_framework),
            "workflow_type": workflow_type, 
            "total_memory": psutil.virtual_memory().total / (1024**3)
        }
    if return_size_mb is not None:
        benchmark_result["return_size_mb"] =  return_size_mb
    
    if wait_for_maping is not None:
        benchmark_result["wait_for_maping"] =  wait_for_maping
    
    
    return benchmark_result 



def generate_run_id():
    run_id = os.getlogin() + "_" +datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    return run_id


def get_database_type(workflow_orchestration_framework):
    if workflow_orchestration_framework == "prefect":
        from prefect.settings import PREFECT_API_DATABASE_CONNECTION_URL
        logger = logging.getLogger("RADPS")

        # Get the connection URL
        db_url = PREFECT_API_DATABASE_CONNECTION_URL.value()
        
        db_type = db_url.split(":")[0]
        
        logger.debug("Prefect database connection URL: %s", db_url)
        return db_type
    elif workflow_orchestration_framework == "airflow":
        #No idea if this code works. Copolit suggested it.
        from airflow import settings
        from sqlalchemy.engine import Engine
        from sqlalchemy.engine.url import make_url
        
        db_url = settings.SQL_ALCHEMY_CONN
        db_type = make_url(db_url).drivername
        
        logger = logging.getLogger("RADPS")
        logger.debug("Airflow database connection URL: %s", db_url)
        
        return db_type
    elif workflow_orchestration_framework == "dask":
        # Dask does not have a database connection URL
        return "none"
    else:
        raise ValueError(f"Unknown workflow orchestration framework: {workflow_orchestration_framework}")