#Spectral line imaging for target(s)

from prefect import task, flow
# import imaging stage specific functions
from stage_image_cont_selfcal import (
    load_context,  
    solve,  
    calc_heuristics, 
    archive_export, 
    store_context,
)
from core import fake_qa_score, create_qa_artifact, sleep_placeholder

@task
def cubeimage_qa_score(image_data):
    """ Calculate QA score for cube images"""
    sleep_placeholder(1.0) 
    return fake_qa_score('cubeimage_qa_score')

@task
def uvcontsub(inp, chunkid=0, spw_for_trigger_partial_failure=-1 ):
    """ 
    Perform uv continuum subtraction for a given chunk
    trigger_partial_failure = True will raise an execption for chunkid=2
    """
    sleep_placeholder(2.0)
    if spw_for_trigger_partial_failure > -1:
        if chunkid == spw_for_trigger_partial_failure:
            raise RuntimeError(f'uvcontsub failed for {chunkid}')

    return  

@task
def uv_continuum_subtraction(data) -> dict:
    """ Perform continuum subraction in uv domain"""
    print("UV continuum subtraction simulating parallel execution")
    
    # Parameters to trigger some failure modes
    # no failure
    # raise_execption, failed_spw = True, -1
    # For failure handling test
    # Trigger failure on uvcontsub and raise an exception for a specifed spw
    # raise_exception, failed_spw = True, 2
    # Trigger failure on uvcontsub for a specified spw but continue on 
    raise_exception, failed_spw =False, 2

    n_field = data['target']['n_field']
    n_spw = data['target']['n_spw']
    n_scan = data['target']['n_scan']
    uvcont_ret = dict()
    uvcont_par=[]
    # parallel processing across fields and scans 
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
    return uvcont_ret

    
@flow
def image_target_cube(data):
    """ Cube imaging on target """
    print("Starting cube imaging for target")

    calibrated_data = load_context(data, src='target')

    # Do spectral line existance check and return relevant data
    has_spectraldata = calc_heuristics(calibrated_data)
    if has_spectraldata == dict():
        print("No spectral data found. Cube imaging stage is skipped")
        return inp 
    else: # do target cube imaging
        try:  
            # uvcontsub
            uvcontsub_res = uv_continuum_subtraction(has_spectraldata)

            image_data = solve(uvcontsub_res['datashape'], src='target', 
                          combine='scan', soltype='cube_imaging')
            qa_result = cubeimage_qa_score(image_data)
            archived_data = archive_export(image_data, src='target', paraxes='fieldandspw')
            stored_context = store_context(archived_data)
            qa_result['cube_image_data']=image_data
            create_qa_artifact(qa_result)
        except Exception as e:
            print(f"Cube imaging failed with error: {e}")
            stored_context = data    
    return stored_context


if __name__ == '__main__':
    data = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
             'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
             'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1} }
    image_target_cube(data)
    