import time
from datetime import datetime
import random
import logging
import numpy as np
import sys
import os

from airflow.sdk import dag, task, task_group
from airflow.operators.python import get_current_context
        
# Assuming benchmark_utils.py in the dags directory
sys.path.insert(0, os.path.dirname(__file__))
from benchmark_utils import organize_benchmark_result, generate_run_id, save_timing_results



# for quick code check 
# list_n_tasks = [2, 4, 8, 16, 32]
# list_sleep_time = [(2.0,2.0)]
# data_size_mb_list = [0.1, 10] #MB

# list_n_tasks = [100]
# list_sleep_time = [(2.0,2.0)]
# data_size_mb_list = [0.01] #MB

###############################
# Note: airflow.cfg  set the global (hard) concurrency limit (default: 32 for paralleism, 
# 16 for dag_concurrency, 16 for max_active_runs_per_dag). DAG or task level setting cannot
# exceed these limits.

## Benchmark #1
# list_n_tasks =  [1000, 2000, 4000, 8000, 16000, 32000, 64000, 128000]
# list_sleep_time = [(1.0,0.1), (6.0, 4.0)]
# data_size_mb_list = [0.1 10]

# list_n_tasks =  [128000]
# list_sleep_time = [(6.0, 4.0)]
# data_size_mb_list = [0.1]

list_n_tasks =  [10]
list_sleep_time = [(6.0, 4.0)]
data_size_mb_list = [0.01]

logging.getLogger("airflow").setLevel(logging.WARNING)
logger = logging.getLogger("airflow.task")
logger.setLevel(logging.INFO)

#def sleep_and_generate_data(min_sleep:float, max_sleep:float, data_size_mb:float, task_id:int):
def sleep_and_generate_data(min_sleep:float, max_sleep:float, data_size_mb:float, task_id:int, run_id:str):
    
    start_time = time.time()
    
    duration = random.uniform(min_sleep, max_sleep)
    time.sleep(duration)
    #data = np.ones((int(data_size_mb * 1024 * 1024 / 8 ),), dtype=np.float64)
    data = np.ones((int(data_size_mb * 1024 * 1024 / 8),), dtype=np.float64).tolist()  # Simulating a large array of 1 million elements
    end_time = time.time()
    total_time = end_time - start_time
    #return data, total_time   
    return {'total_time':total_time, 'data':data, 'run_id': run_id, 'start_time': start_time, 'end_time': end_time, 'task_id': task_id}      


@task 
def sum_time_and_data(returned_data:dict):
    start = time.time()
    time_sum = sum(r['total_time'] for r in returned_data)
    data_sum = sum(np.array(r['data']) for r in returned_data)
    runid = returned_data[0]['run_id']
    start_times = [r['start_time'] for r in returned_data]
    end_times = [r['end_time'] for r in returned_data]
    t_concurrent = max(end_times) - min(start_times)
    end = time.time()
    time_sum += end - start
    t_workflow = t_concurrent + end - start
    sum_results = {'data_sum': data_sum.tolist(), 'time_sum': time_sum, 't_workflow': t_workflow, 'run_id': runid}
    print(f"Sum of task times: {time_sum:.4f} seconds, t_wokflow: {t_workflow:.4f} seconds, run_id: {runid}")
    return sum_results


# @task
# def get_run_id():
#     """ Get DAG run id """
#     context = get_current_context()
#     return context['run_id']
    
@dag(dag_id='benchmark_airflow_return_v2',
     start_date=datetime(2025,1,1),
     schedule=None,
     catchup=False,
     tags=['benchmark', 'airflow'])

def benchmark_airflow_return_v2():

    # this gives an DAG parse time error  ..... so hardcode it for now
    # get databbase info (must be done at DAG parse time)
    #from airflow import settings
    #from sqlalchemy.engine.url import make_url 
    #db_url = settings.SQL_ALCHEMY_CONN
    #db_type = make_url(db_url).drivername
    db_type = 'postgress'  


    runid = "jsteeb_k3s_test"#get_run_id() 
    @task
    def organize_and_save_airflow_benchmark_result(results:dict, 
                                          runid : str,
                                          nconcurrency:int,
                                          min_sleep_val:float,
                                          max_sleep_val:float,
                                          #t_workflow:float,
                                          n_threads_per_process:int,
                                          datasize:float,
                                          ):
        
        # modify DAG run id
        runidtime = runid.split('manual__') [1]
        modtime = datetime.fromisoformat(runidtime).strftime("%Y-%m-%d_%H-%M-%S")
        #runid = os.getlogin() + '_' + modtime
          # replace decimal with 'p' to avoid issues in group_id
        result_dict = organize_benchmark_result(
            run_id = runid,
            workflow_orchestration_framework = 'airflow',
            workflow_type = 'tree_combine',
            n_tasks = nconcurrency,
            t_min_sleep = min_sleep_val,
            t_max_sleep = max_sleep_val,
            t_workflow = results['t_workflow'],
            t_sum_task_times = results['time_sum'],
            n_threads_per_process = n_threads_per_process,
            n_processes = 1,
            runner = f'airflow_{db_type}',
            return_size_mb = datasize,
            wait_for_maping = False,
        )

        #save_timing_results(result_dict, filename = '~/airflow_benchmark_results.csv' )   
        logger = logging.getLogger("airflow.task")

        data_sum_arr = np.array(results["data_sum"])
        logger.info("t_workflow: %s" , results['t_workflow'])
        logger.info("Average t_sum_task_times per unit of parallelism: %s", results['time_sum']/n_threads_per_process) 
        logger.info("overhead percentage: %s", result_dict["overhead_percentage"])
        logger.info("overhead per task seconds: %s", result_dict["overhead_per_task_seconds"])
        logger.info("Data size per task: %s MB", datasize)
        logger.info("Maximum data size: %s MB", data_sum_arr.nbytes / (1024**2))         
        logger.info("*"*50)                        
    prev_group = None
    
    for datasize in data_size_mb_list:
        for nconcurrency in list_n_tasks:

            for max_sleep_val, min_sleep_val in list_sleep_time:
            # closure to pass ntasks to the task group
                def create_group_for_ntasks(nconcurrency, prev_dependency):
                    """
                    Create a task group that runs concurrently with the specified number of tasks.
                    """
                    # When decimal is in group id Airflow complains as it is not alphanumeric 
                    tmin = int(min_sleep_val)
                    # input return data size is in MB
                    if isinstance(datasize, float):
                        datasize_str = str(datasize).replace('.', 'p')  # replace decimal with 'p' to avoid issues in group_id
                    else:
                        datasize_str = str(datasize)    
                    @task_group(group_id=f'{datasize_str}mb_tmin{tmin}_concurrent_run_{nconcurrency}')
                    def create_concurrent_run_taskgroup():

                        @task(task_id=f'create_concurrent_ntasks_{nconcurrency}')
                        def create_concurrent_task_parameters(nconcurrency, min_sleep, max_sleep, data_size_mb, prev_result):

                            """Create parameters for concurrent tasks """
                            
                            task_params = []

                            for i in range(nconcurrency):
                                task_params.append({'min_sleep': min_sleep, 
                                                    'max_sleep': max_sleep, 
                                                    'data_size_mb':data_size_mb, 
                                                    'task_id': f'{nconcurrency}tasks_{i}', 
                                                    'run_id': f'{runid}'})
                            return task_params


                        @task
                        def run_concurrent_tasks(task_params):
                            """
                            Run concurrent tasks with the specified number of tasks.
                            """
                            return sleep_and_generate_data(**task_params)



                        task_params_list = create_concurrent_task_parameters(nconcurrency, 
                                                                            min_sleep=min_sleep_val, 
                                                                            max_sleep=max_sleep_val, 
                                                                            data_size_mb=datasize, 
                                                                            prev_result=prev_dependency)
                        
                        run_results = run_concurrent_tasks.expand(task_params=task_params_list)
                        
        
                        sum_results = sum_time_and_data.override(
                            task_id=f'sum_time_and_data_{nconcurrency}')(
                            returned_data=run_results)


                        
                        return sum_results

                    return create_concurrent_run_taskgroup()
                
    

                #start_time = time.time()
                group_results = create_group_for_ntasks(nconcurrency,prev_group)
                #t_workflow = time.time() - start_time
                n_threads_per_process = os.cpu_count()
                
                
                organize_and_save_airflow_benchmark_result.override(task_id=f'save_results_{datasize}mb_tmin{min_sleep_val}_{nconcurrency}tasks'
                                                                    )(results = group_results,
                                            runid = runid,
                                            nconcurrency = nconcurrency,
                                            min_sleep_val = min_sleep_val,
                                            max_sleep_val = max_sleep_val,
                                            #t_workflow = t_workflow,
                                            n_threads_per_process = n_threads_per_process,
                                            datasize = datasize
                                            )
        
                
                prev_group = group_results


benchmark_airflow_return_v2_dag = benchmark_airflow_return_v2()


