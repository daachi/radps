from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import random, time

def  generate_qa(processname:str):
         qascore = random.uniform(0.0, 1.0)
         print(f"QA score for {processname} is {qascore}")
         return qascore


@dag(
 dag_id="cube_imaging",
    schedule=None,
    start_date=datetime(2025,1,1),
    catchup=False,
)

def cube_imaging():
    """
    DAG to perform cube imaging
    asssuming processing a single chan chunk of data
    """
     
    @task
    def extract_metadata(nchan=1)->dict:
        """ Extract metadata from the data  """
        fake_metadata = {
                    'field': ['target'],
                    'spw': [0, 1, 2, 3],
                    'channel': list(range(nchan)),
                    'scan': [0, 1, 2, 3, 4],
                    }
        return fake_metadata

    # generate configuration (parameters) to send to the child DAG
    @task
    def make_conf(metadata:dict):
        nfield= len(list(metadata['field']))
        nspw = len(metadata['spw'])
        nchan = len(metadata['channel'])
        nscan = len(metadata['scan'])
        niter = 5
        return {'n_par': nfield*nchan,  # assume single spw process with nchan
                'n_comb': nscan,
                'niter':niter} # assuming a fixed number of scans for simplicity
    # dictionary returned by extract_metadata cannot be directly used
    # in the argument of .expand and refereces the key in the dictionary
    # as it is XCom object
    @task
    def get_spwlist(metadata:dict)->list:
        return metadata['spw']
    
    @task
    def get_chanlist(metadata:dict)->list:
        #chunklist = extract_metadata()['chunkid']
        return metadata['channel']

    @task
    def data_prep():
        print("Preparing data for cube_imaging")
        time.sleep(1.0)
        return True

    @task
    def uvcontsub(spwid):
        print(f"Processing UV continuum subtraction for spw: {spwid}")
        time.sleep(1.0)
        return True


    @task.branch
    def check_qa_branch(process:str, next_branch_taskname:str):
        """ branch task based on qa score"""
        qascore = generate_qa(process)
        if qascore > 0.3:
            print(f"QA score = {qascore} for {process} PASS")
            return f"{next_branch_taskname}"
        else:
            return "finalize_task"
             
        return qascore

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
        
    meta_data = extract_metadata(10)
    #get_chanlist(meta_data)
    dag_conf = make_conf(meta_data)


    # solver dag trigger version
    image_target_cube = TriggerDagRunOperator(
        task_id='image_target_cube',
        trigger_dag_id='solver_new',
        conf=dag_conf,
        wait_for_completion=True,
        poke_interval=30,
        execution_timeout=timedelta(600),
        allowed_states=['success'],
        failed_states=['failed'],
    )

        
        #mapped_image_cube = image_target_cube.expand(chan=chanlist)
    
    

    @task(task_id='finalize_task', trigger_rule='none_failed_min_one_success')
    def finalize_op():
        """ run by all branches at the end of the dag"""
        print("Run finalize process")
        time.sleep(1.0)
        return True
    
    #chunklist = extract_metadata()['chunkid']\
    spwlist= get_spwlist(meta_data)
    
    # use override to change the generic task_id to specific task_id 
    post_uvcontsub_branch = check_qa_branch.override(task_id='post_uvcontsub_branch')(process="uvcontsub", next_branch_taskname="image_target_cube")
    finalize_task = finalize_op()
    image_target_cube_branch = image_target_cube

    
    data_prep() >> meta_data >> spwlist >> uvcontsub.expand(spwid=spwlist)>> post_uvcontsub_branch
    post_uvcontsub_branch >> image_target_cube_branch >> check_qa('target_image') >> finalize_task
    post_uvcontsub_branch >> finalize_task


cube_imaging()

