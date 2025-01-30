#per-SPW continuum imaging
from prefect import task, flow
from stage_image_cont_selfcal import (
    load_context, 
    applymodel, 
    solve, 
    archive_export, 
    store_context,
    generate_image_datashape,
)

@flow
def stage_image_perspw_cont(inp,src='target'):
    """
    per-SPW continuum imaging 
    """
    res = load_context(inp,src)
    # check if selcal is done
    # and if that is the case, apply best calibration solution to data
    res2 = res
    if 'selfcal_sol' in res:
        caltable = inp['selfcal_sol']
        res2 = applymodel(caltable, inp, src='target')
    # do per spw imaging (solve per spw and field)
    image_data = solve(res, src='target', combine='scan', soltype='imaging')
    # export data
    print('image_data=', image_data)
    archived_data = archive_export(image_data, src='target', paraxes='fieldandspw')
    #store context
    stored_context = store_context(archived_data)
    return stored_context

inp = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
             'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
             'target':{'n_field':1, 'n_spw':3, 'n_scan':5} }
stage_image_perspw_cont(inp)
