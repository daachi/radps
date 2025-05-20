from airflow.sdk import dag, task, task_group
from datetime import datetime
import random

def  generate_qa(processname:str):
         qascore = random.uniform(0.0, 1.0)
         print(f"QA score for {processname} is {qascore}")
         return qascore

@dag(
 dag_id="cube_imging",
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
                   'spwid': [0, 1, 2, 3],
                    'channel': list(range(nchan))
                    }
        return fake_metadata

    # dictionary returned by extract_metadata cannot be directly used
    # in the argument of .expand and refereces the key in the dictionary
    # as it is XCom object
    @task
    def get_spwlist(metadata:dict)->list:
        return metadata['spwid']
    
    @task
    def get_chanlist(metadata:dict)->list:
        #chunklist = extract_metadata()['chunkid']
        return metadata['channel']

    @task
    def data_prep():
        print("Preparing data for cube_imaging")

    
    @task
    def uvcontsub(spwid):
        print(f"Processing UV continuum subtraction for spw: {spwid}")
        return True


    
    @task.branch
    def check_qa_branch(process:str, next_branch_taskname:str):
        """ branch task based on qa score"""
        qascore = generate_qa(process)
        if qascore > 0.3:
            print(f"QA score = {qascore} for {process} PASS")
            return f"{next_branch_taskname}_group"
        else:
            return "final_process"
             
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
        
    @task_group(group_id='image_target_cube_group')
    def image_target_cube_group():
        meta_data = extract_metadata(10)
        chanlist = get_chanlist(meta_data)

        @task(task_id='image_target_cube')
        def image_target_cube(chan):
            print(f"Processing cube imaging channel: {chan} for cube imaging")
            return True
    
        mapped_image_cube = image_target_cube.expand(chan=chanlist)


    @task(task_id='final_process', trigger_rule='none_failed_min_one_success')
    def final_process():
        """ run by all branches at the end of the dag"""
        print("Final process")
        return True
    
    #chunklist = extract_metadata()['chunkid']\

    meta_data = extract_metadata(10)
    spwlist= get_spwlist(meta_data)
    
    post_uvcontsub_branch = check_qa_branch.override(task_id='post_uvcontsub_branch')(process="uvcontsub", next_branch_taskname="image_target_cube")
    #image_target_cube_mapped = image_target_cube.expand(chan=chanlist)
    image_target_cube_branch = image_target_cube_group()
    data_prep() >> meta_data >> spwlist >> post_uvcontsub_branch
    post_uvcontsub_branch >> image_target_cube_branch >> check_qa('target_image') >> final_process()
    post_uvcontsub_branch >> final_process()


    #data_prep ()  >> meta_data >> spwlist >> chanlist >>  uvcontsub.expand(spwid=spwlist) >> image_target_cube.expand(chan=chanlist)

cube_imaging()
