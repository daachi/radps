from datetime import datetime
import random, time
from airflow.sdk import dag, task, task_group
from airflow.sdk import Param


@dag(
    dag_id="cont_imaging_with_selfcal",
    schedule=None,
    start_date=datetime(2025,1,1),
    catchup=False,
)
def cont_imaging_with_selfcal():

    @task()
    def prep_data(data):
        print("Preparing data for selfcal")
        return data

    @task()
    def make_cont_image():
        print("Making aggregated continuum image")
        return 'cont_image'

    @task(task_id="generate_initial_snr")
    def generate_initial_snr() -> float:
        """ randomly generate SNR"""
        print("Calculate SNR...")
        snr = random.uniform(1.0,100.0)
        #snr= 1.0
        print(f'=> initial SNR = {snr}')
        return snr


    @task.branch(task_id='check_initial_snr')
    def check_initial_snr(snr: float):
        """Branch based on SNR value"""
        snr_value = snr
        print(f"SNR value is {snr_value}")
        if snr_value > 4.0:
            print(f"SNR is {snr_value}, proceed with selfcal loop")
            return "selfcal_loop_group_p0"
        else:
            return "skip_op"
        
    branch_by_snr = check_initial_snr(generate_initial_snr())
    
    
    @task(task_id='skip_task', trigger_rule='none_failed_min_one_success')
    def skip_op():
        print("Skipping selfcal loop")
   

    @task(task_id='finalize_task', trigger_rule='none_failed_min_one_success')
    def finalize_op():
        print("Do fininalize op")

    
    skip_remaining = skip_op()

    selfcal_soltypes = ['p0', 'p1', 'p2', 'ap0']
    data = 'taget_data'       
    
    #store the references
    solve_refs ={}
    check_image_snr_branch ={}
    imaging_refs = {}

    #prep_data(data) >> make_cont_image() >> branch_by_snr

    for soltype in selfcal_soltypes:
        @task_group(group_id=f'selfcal_loop_group_{soltype}')
        def selfcal_loop_group(data):
            @task
            def solve(soltype):
                print(f"Doing solve for soltype {soltype}")
                return f"caltable__{soltype}"
            
            @task
            def applymodel(caltable, data):
                print(f"Applying calibration model, {caltable} to {data}")
                return 'calibrated_data'
            
            @task
            def imaging(data):
                print(f" Do selfcal imaging for {data}")
                return "image"
            
            
            cal_solve = solve(soltype)
            cal_apply = applymodel(cal_solve,data)
            target_image = imaging(cal_apply)
            #check_image_snr_branch = check_image_snr(image_target)

            return cal_solve, target_image
        
        solve_result, imaging_result = selfcal_loop_group(data)
        imaging_refs[soltype] = imaging_result
        solve_refs[soltype] = solve_result
    
        @task.branch(task_id=f'check_image_snr_{soltype}')
        def check_image_snr(soltype, image):
            print(f"Checking SNR for {image}")
            snr = random.uniform(1.0, 10.0)
            print(f"Calculated SNR: {snr}")
            if snr > 4.0:
                print(f"soltype = {soltype}, {selfcal_soltypes[-1]}")
                if soltype != selfcal_soltypes[-1]:
                    return f"selfcal_loop_group_{selfcal_soltypes[selfcal_soltypes.index(soltype) + 1]}.solve"
                else:
                    return 'finalize_task'     
            else:
                return 'skip_task'

        check_image_snr_branch[soltype] = check_image_snr(soltype, imaging_refs[soltype])        
 
   # branch_by_snr >> solve_refs[selfcal_soltypes[0]]  


    # define graph
    prep_data(data) >> make_cont_image() >> branch_by_snr
    branch_by_snr >> solve_refs[selfcal_soltypes[0]]  
    branch_by_snr >> skip_remaining


    for soltype in selfcal_soltypes:
        branch = check_image_snr_branch[soltype]

        if selfcal_soltypes.index(soltype) + 1 < len(selfcal_soltypes):
            branch >> solve_refs[selfcal_soltypes[selfcal_soltypes.index(soltype) + 1]]
        else:
            branch >> finalize_op()        
    
        branch >> skip_remaining

    skip_remaining >> finalize_op()


cont_imaging_with_selfcal()


    
