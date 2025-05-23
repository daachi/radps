from airflow.sdk import dag, task, task_group
from airflow.operators.empty import EmptyOperator

from datetime import datetime
import random

def  generate_qa(processname:str):
         qascore = random.uniform(0.0, 1.0)
         print(f"QA score for {processname} is {qascore}")
         return qascore

@dag(
 dag_id="per_spw_cont_imaging",
    schedule=None,
    start_date=datetime(2025,1,1),
    catchup=False,
)
def per_spw_continuum_imaging():
    """
    DAG to perform per-spw continuum imaging
    """
     
    @task
    def extract_metadata(nchan=1)->dict:
        """ Extract metadata from the data  """
        fake_metadata = {
                   'spwid': [0, 1, 2, 3],
                    'channel': list(range(nchan)),
                    'selfcal_table': random.choice(['selfcal_table', 'None'])}
        return fake_metadata

    # dictionary returned by extract_metadata cannot be directly used
    # in the argument of .expand and refereces the key in the dictionary
    # as it is XCom object
    @task
    def get_spwlist(metadata:dict)->list:
        return metadata['spwid']
    
    @task
    def get_caltable(metadata:dict)->list:
        #chunklist = extract_metadata()['chunkid']
        return metadata['selfcal_table']

    @task
    def data_prep():
        print("Preparing data for cube_imaging")


    @task   
    def check_qa(process:str):
        """ non-branching version of check qa score"""
        qascore = generate_qa(process)
        if qascore > 0.3:
            print("QA PASS")
            return True
        else:
            print("QA FAIL")
            return False
        
    @task
    def apply_selfcal_table(caltable:str):
        """ apply selfcal table"""
        print("Applying selfcal table: {caltable}")
        return True

    join = EmptyOperator(
        task_id="join_after_branch",
        trigger_rule='none_failed_min_one_success'
    )
    @task.branch(task_id='check_selfcal_table')
    def check_selfcal_table(caltable:str):
        """"""
        if caltable == 'None':
            print("No selfcal table to apply")
            return f"join_after_branch"
        else:
            return f"apply_selfcal_table"


    @task(task_id='image_target_perspw_cont', trigger_rule='all_done')
    def image_target_perspw_cont_task(spwid):
        print(f"Processing  spw: {spwid} for continuum imaging")
        return True

   
    @task(task_id='final_process', trigger_rule='none_failed_min_one_success')
    def final_process():
        """ run by all branches at the end of the dag"""
        print("Final process")
        return True
    
    #chunklist = extract_metadata()['chunkid']\
    meta_data = extract_metadata()
    spwlist= get_spwlist(meta_data)
    caltable = get_caltable(meta_data)

    data_prep() >> meta_data
    check_selfcal_table_branch = check_selfcal_table(caltable=caltable)

    # add 
    apply_caltable = apply_selfcal_table(caltable=caltable)
    check_selfcal_table_branch >> apply_caltable >> join
    check_selfcal_table_branch >> join
    
    mapped_image_target_perspw_cont = image_target_perspw_cont_task.expand(spwid=spwlist)

    join >> mapped_image_target_perspw_cont >> check_qa('target_image') >> final_process()

   
    #mapped_image_target_perspw_cont >> check_qa('target_image') >> final_process    
per_spw_continuum_imaging()
