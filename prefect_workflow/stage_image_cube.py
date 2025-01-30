#Spectral line imaging for target(s)
from prefect import task, flow
from stage_image_cont_selfcal import (
    load_context,  
    solve,
    calc_heuristics, 
    calc_qa,
    archive_export, 
    store_context,
)
from time import sleep
ns = 1

@task
def uvcontsub(inp, chunkid=0):
    """ Perform uv continuum subtraction for a given chunk"""
    sleep(ns)
    return

@task
def uv_continuum_subtraction(inp):
    """ Perform continuum subraction in uv domain"""
    n_field = inp['target']['n_field']
    n_spw = inp['target']['n_spw']
    n_scan = inp['target']['n_scan']
    uvcont_ret = dict()
    uvcont_par=[]
    # parallel processing across fields and scans 
    for ipar in range(n_field*n_scan):
        spw_par = []
        for ispw in range(n_spw):
            spw_par.append(uvcontsub.submit(inp,ispw))
        uvcont_par.append([i.result() for i in spw_par]) 
    uvcont_ret['uvcont_result']=uvcont_par
    uvcont_ret['datashape'] = dict(inp)
    return uvcont_ret

    
@flow
def stage_image_target_cube(inp):
    calibrated_data = load_context(inp, src='target')

    # Do spectral line existance check and return
    has_spectraldata = calc_heuristics(calibrated_data)
    print('calibrated_data=', calibrated_data)
    print('has_spectraldata=', has_spectraldata)
    if has_spectraldata == dict():
        return (f"No spectral data found. Cube imaging stage is skipped")
    else: # do target cube imaging

        # uvcontsub
        uvcontsub_res = uv_continuum_subtraction(has_spectraldata)
        image_data = solve(uvcontsub_res['datashape'], src='target', 
                       combine='scan', soltype='cube_imaging')
        qa_result = calc_qa(image_data)
        print('qa_result=',qa_result)
        print('image_data=', image_data)

        archived_data = archive_export(image_data, src='target', paraxes='fieldandspw')
        stored_context = store_context(archived_data)
    return stored_context


inp = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1}, 
       'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4}, 
       'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1} }
stage_image_target_cube(inp)