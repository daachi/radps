from airflow.sdk import dag, task, task_group
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import random


n_par = 10
n_comb = 3
niter = 5

@dag(
 dag_id="solver",
    schedule=None,
    start_date=datetime(2025,1,1),
    catchup=False,
)
def solver_dag():
    """
    Generic sovlver DAG   

    solver loop:
    for i in range(npar): # parallel process over npar (Use Executor such as DaskExecutor? to devlopy this DAG???)
          # Implemented here with this top level loop 
        for j in range(niter): # sequential process over niter 
            for k in range(n_comb) {update_direction() }- in alogorithm parallelism
            update_model()
            check_converge() - #branch 
    """
    
    # logic to determin out parallelisatin loop will be here

    npar = 10 

    @task
    def data_prep(data):
        """data preparation"""
        print("Running data_prep and extracting metadata")
        return data
    
    @task
    def get_subtask(parid, n_comb):
        """Get subtask"""
        print(f"Getting subtask for {parid} with {n_comb} combinations")
        return [f"{parid}_task_{j}" for j in range(n_comb)]

    @task
    def update_direction(subtask_name:str):
        """ Update direction """
        print(f"Updating direction for {subtask_name}")
        return True
    
    @task
    def update_model():
        """ Solve model """
        print("update model")
        return True
    
    @task
    def gather_direction(*arg):
        """ Gather direction """
        print(f"Gathering direction with data {arg}")
        return True 
    
    @task
    def finilize():
        """ Finilize """
        print("Finilize operation")
        return True
    

    
    def solve_sequence_group(parid:int, iteration:int):
        with TaskGroup(group_id=f"{parid}_{iteration}") as tg:
            subtasks = get_subtask(parid, n_comb)
            update_direction_parallel = update_direction.expand(subtask_name=subtasks)
            update_model_instance = update_model()
            update_direction_parallel >> update_model_instance

        return tg
        
    def iteration_loop(parid:int):
        with TaskGroup(group_id=f"{parid}_iteration") as tg:
            previous = None
            for i in range(niter):
                group = solve_sequence_group(parid, i)
                if previous:
                    previous >> group
                previous = group
        return tg
                
    data = {'spwid': [0, 1,2,3], 
            'scan': [0,1,2,3,5]}
    
    metadata = data_prep(data)
    finalize_task = finilize()


    for i in range(0, n_par):
        group = iteration_loop(i)
        metadata >> group >> finalize_task

solver_dag()

