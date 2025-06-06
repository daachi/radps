from datetime import datetime, timedelta
from airflow.sdk import dag
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import json
from typing import List, Dict, Any
import time
import concurrent.futures

# DAG configuration
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
@dag(
    dag_id='solver_new',
    default_args=default_args,
    description='generic solver DAG',
    schedule=None,  # Manual trigger
    catchup=False,
    tags=['nested-loops'],
    max_active_runs=1,  # Prevent multiple concurrent runs
)
def solver_new_dag():

    @task
    def get_runtime_config(**context) -> Dict[str, int]:
        """
        Get runtime configuration from DAG run configuration
        """
        dag_run = context.get('dag_run')
        conf = dag_run.conf if dag_run and dag_run.conf else {}
        
        # Get configuration with defaults
        n_par = conf.get('n_par', 1)
        niter = conf.get('niter', 10)
        n_comb = conf.get('n_comb', 1)
        
        # Some validation and error handling of configuration
        if not isinstance(n_par, int) or n_par < 1:
            n_par = 1
        if not isinstance(niter, int) or niter < 1:
            niter = 1
        if not isinstance(n_comb, int) or n_comb < 1:
            n_comb = 1
        
        
        config = {
            'n_par': n_par,
            'niter': niter,
            'n_comb': n_comb,
        }
        
        print(f"Runtime Configuration - Parallel Processes: {n_par}, Inner Iterations: {niter}")
        return config

    @task
    def data_prep(data):
        """data preparation"""
        print("Running data_prep and extracting metadata")
        result = data.copy()  # Simulate data preparation
        return result
    
    def update_direction_parallel(data, n_comb):
        """ 
        Update direction in parallel - use python's concurrent.futures for parallel execution
        to avoid use of Airflow's task mapping for this part which cuases issues in passing 
        parameters dynamically.
        
        """

        def update_direction(part_id):
            """ Update direction """
            print(f"Updating direction for data for part {part_id}")
            time.sleep(1.0)
            result = 'update_direction_result_' + str(part_id)
            return result
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=n_comb) as executor:
            futures = [executor.submit(update_direction, part_id) for part_id in range(n_comb)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        return results
        # cdoe to aggregate the results here
        
    
    def update_model(data):
        """ Solve model """
        print("update model")
        time.sleep(1.0)
        result = 'update_model_result_' + str()
        return result

    @task
    def end():
        """ Final task to summarize results """
        print("All processes completed successfully")
        return "Final summary of all processes"

    @task
    def generate_process_list(config: Dict[str, int]) -> List[Dict[str, Any]]:
        """
        Generate list of processes to be executed
        This creates the input for dynamic task mapping
        """
        n_par = config['n_par']
        niter = config['niter']
        n_comb = config['n_comb']
        
        process_list = []
        for process_id in range(n_par):
            process_list.append({
                'process_id': process_id,
                'niter': niter,
                'n_comb': n_comb
            })
        
        print(f"Generated {len(process_list)} parallel rocess configurations")
        return process_list

    @task
    def execute_single_solver(process_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute all iterations for a single process
        This function will be mapped across all processes dynamically
        """
        process_id = process_config['process_id']
        niter = process_config['niter']
        n_comb = process_config['n_comb']
        
        print(f"Starting Process {process_id} with {niter} iterations")
        
        process_results = []
        previous_update_model_result = None
        
        # Execute iterations sequentially within this process
        for iteration in range(niter):
            print(f"Process {process_id}, Iteration {iteration}: Starting")
            
            # Task 1 execution
            update_direction_result = update_direction_parallel(previous_update_model_result, n_comb)
            
            # Task 2 execution
            update_model_result = update_model(update_direction_result)
            
            # Store results for this iteration
            iteration_result = {
                'iteration': iteration,
                'update_direction_result': update_direction_result,
                'update_model_result': update_model_result
            }
            process_results.append(iteration_result)
            
            # Pass task2 result to next iteration
            previous_update_model_result = update_model_result
            
            print(f"Process {process_id}, Iteration {iteration}: Completed")
        
        result = {
            'process_id': process_id,
            'iterations': process_results,
            'final_result': previous_update_model_result
        }
        
        print(f"Process {process_id}: All iterations completed, final result: {previous_update_model_result}")
        return result

        # Build the task flow using dynamic task mapping
    config = get_runtime_config()
    process_list = generate_process_list(config)

    # Use dynamic task mapping that creates tasks at runtime
    process_results = execute_single_solver.expand(process_config=process_list)

    # Summarize all results
    final_summary = end()

    data = {'spwid': [0, 1, 2, 3], 'scan': [0, 1, 2, 3, 5]}
            
    data_prep(data) >> process_list >> process_results >> final_summary
solver_new_dag_instance = solver_new_dag()