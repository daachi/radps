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
from core import fake_qa_score, create_qa_artifact

@task
def perspw_cont_imaging_qa_score(image_data):
    """ Calculate QA score for per-SPW continuum images"""
    retdict ={}
    for ispw in range(image_data['target']['n_spw']):
        qascore=fake_qa_score('perspw_cont_imaging_qa_score_spw'+str(ispw))
        retdict.update(qascore)
    return retdict

@flow
def image_perspw_cont(data,src='target'):
    """
    per-SPW continuum imaging 
    """
    print("Starting per-SPW continuum imaging")
    res = load_context(data,src)
    # check if selcal is done
    # and if that is the case, apply best calibration solution to data
    res2 = dict(res)
    if 'selfcal_sol' in res2[src]:
        print("Selfcal solution found, applying to data")
        caltable = data[src]['selfcal_sol']
        res2 = applymodel(caltable, data, src='target')
    # do per spw imaging (solve per spw and field)
    image_data = solve(res2, src='target', combine='scan', soltype='imaging')
    qa_score = perspw_cont_imaging_qa_score(image_data)
    qa_score['perspw_cont_image'] = image_data
    # export data
    archived_data = archive_export(image_data, src='target', paraxes='fieldandspw')
    #store context
    stored_context = store_context(archived_data)
    create_qa_artifact(qa_score)
    return stored_context


if __name__ == '__main__':

    data = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
           'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
           'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'selfcal_sol':'caltable_loc'} }
    image_perspw_cont(data)
