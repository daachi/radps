#Spectral line imaging for target(s)

from prefect import task, flow
from prefect.logging import get_run_logger
# import imaging stage specific functions
from stage_image_cont_selfcal import (
    solve,  
    calc_heuristics, 
    archive_export, 
    generate_fake_image,
)
from core import (
    fake_qa_score, 
    create_qa_artifact,
    sleep_placeholder,
    create_context,
    load_context,
    add_to_context,
)
import os

@task
def cubeimage_qa_score(image_data):
    """ Calculate QA score for cube images"""
    sleep_placeholder(1.0) 
    qascore = fake_qa_score('cubeimage_qa_score')
    qascore["cube_image_data"] = image_data 
    return qascore

@task
def uvcontsub(inp: dict, chunkid: int=0, spw_for_trigger_partial_failure: int=-1 ):
    """ 
    Perform uv continuum subtraction for a given chunk
    trigger_partial_failure = True will raise an execption for chunkid=2
    """
    sleep_placeholder(2.0)
    if spw_for_trigger_partial_failure > -1:
        if chunkid == spw_for_trigger_partial_failure:
            raise RuntimeError(f'uvcontsub failed for {chunkid}')

    return  

@task(log_prints=True)
def uv_continuum_subtraction(data: dict) -> dict:
    """
    Perform continuum subraction in uv domain
    - Run in parallel across spws
    - This tasks has parameters which need to be manually edited to trigger
    failure modes

    Parameters:
      data: fake target source visibility data containing spectral line data

    Returns:
      uvcont_ret - continuum subtracted data optionally conttain continuum fit 
      data

    """
    print("UV continuum subtraction simulating parallel execution")
    
    # Parameters to trigger some failure modes
    # no failure
    # raise_execption, failed_spw = True, -1
    # For failure handling test
    # Trigger failure on uvcontsub and raise an exception for a specifed spw
    # raise_exception, failed_spw = True, 2
    # Trigger failure on uvcontsub for a specified spw but continue on 
    raise_exception, failed_spw =False, 2
    print(f"Run with raise_exception={raise_exception}, failed_spw={failed_spw}")

    n_field = data['target']['n_field']
    n_spw = data['target']['n_spw']
    n_scan = data['target']['n_scan']
    uvcont_ret = dict()
    uvcont_par=[]
    # parallel processing across fields and scans 
    print(f"Concurrently process uvcontsub across n_field({n_field}) and scans({n_scan}).")
    for ipar in range(n_field*n_scan):
        spw_par = []
        for ispw in range(n_spw):
            # concurrently run uvcontsub 
            # spw_par stores PrefectFuture objects
            # future.results() by default raise an exception if submitted task fails
            spw_par.append(uvcontsub.submit(data,ispw,failed_spw))
        uvcont_par.append([i.result(raise_on_failure=raise_exception) for i in spw_par]) 
    # ex for getting failed state from future 
    for i in spw_par:
        if i.state.is_failed():
            print(f'uvcontsub failed for {i}')

    uvcont_ret['uvcont_result']=uvcont_par
    uvcont_ret['datashape'] = dict(data)
    print('uvcont_ret=',uvcont_ret)
    return uvcont_ret

    
@flow(log_prints=True)
def image_target_cube(data: dict={},src: str='target'):
    """ 
    Cube imaging on target

    Parameters:
      data: fake continuum subtracted target visibility data 
            default: try to load from existing context 
      src: target source name, default: 'target'

    Returns:
      images: cube images

    Context to be saved: cube images, spectral line data, QA scores
      uvcontsub fit results (continuum model)
    
    """
    print("Starting cube imaging for target")
    calibrated_data = dict()
    if data == dict():
        print('Loading existing context...')
        if not os.path.exists('context.pkl'): 
        #if fileexist == False:
            raise OSError("No context.pkl found. Please run previous stages first.")
            
        else:
            print('context.pkl found. Loading context...')
            data = load_context()
            # check if calibrated data exist from previous stage
            # ToDo: change to use 'stage' key to pull the relevant context
            if 'calibrated_data' not in data:
                raise OSError("No calibrated data found. Please run previous stages first.")
            else:
                calibrated_data = {key: data['calibrated_data'][key] 
                                   for key in data['calibrated_data'].keys() & {src}}
            # simulate data context from previous stages so that calibrated data exist in the context
            #init_context = create_context()
            #input_context = add_to_context(data, key='calibrated_data')
            #all_calibrated_data = load_context()['calibrated_data']
            #print('all_calibrated_data = ', all_calibrated_data)
            #calibrated_data = {key: all_calibrated_data[key] for key in all_calibrated_data.keys() & {'target'}}
    else:
        calibrated_data = dict(data) 

    print(f"Input calibrated_data: {calibrated_data}")
    # Do spectral line existance check and return relevant data
    #  - currently return input data 
    has_spectraldata = calc_heuristics(calibrated_data)
    if has_spectraldata == dict():
        print("No spectral data found. Cube imaging stage is skipped")
        return 
    else: # do target cube imaging
        try:  
            # uvcontsub
            uvcontsub_res = uv_continuum_subtraction(has_spectraldata)
            cur_context = add_to_context(uvcontsub_res, key='uvcontsub') 
            image_data = solve(uvcontsub_res['datashape'], src='target', 
                          combine='scan', soltype='cube_imaging')
            qa_result = cubeimage_qa_score(image_data)
            print("Archiving the resultant cube images... ") 
            # parallize across fields and spws
            archived_data = archive_export(image_data, src='target', paraxes='fieldandspw')
            stored_context = add_to_context(inp=archived_data, key='image')
            # fake artifact generation
            create_qa_artifact(qa_result, artifact_type = "table")
            image_result = dict()
            image_result["url"] = generate_fake_image(image_data["image"])
            create_qa_artifact(image_result, artifact_type = "image")
            print(f"Final stored context: {stored_context}")

        except Exception as e:
            print(f"Cube imaging failed with error: {e}")
            stored_context = data    
    return stored_context


if __name__ == '__main__':
    data = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
             'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
             'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1} }
    image_target_cube(data)
    
