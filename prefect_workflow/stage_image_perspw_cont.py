#per-SPW continuum imaging
import os
import asyncio

from prefect import task, flow
# import imaging stage specific functions
from stage_image_cont_selfcal import (
    applymodel, 
    solve, 
    archive_export, 
    generate_image_datashape,
    find_data_context,
)
from int_clean import int_clean

# import re-used functions
from core import fake_qa_score, create_qa_artifact, load_context, add_to_context

@task
def perspw_cont_imaging_qa_score(image_data):
    """ Calculate QA score for per-SPW continuum images"""
    retdict ={}
    for ispw in range(image_data['target']['n_spw']):
        qascore=fake_qa_score('perspw_cont_imaging_qa_score_spw'+str(ispw))
        retdict.update(qascore)
    return retdict

@flow(log_prints=True)
def image_perspw_cont(data: dict={}, src: str='target', interactive: bool=False) -> dict:
    """
    per-SPW continuum imaging 
    """
    print("Starting per-SPW continuum imaging")
    if data == dict():
        print('Loading existing context...')
        if not os.path.exists('context.pkl'): 
            raise OSError("No context.pkl found. Please run previous stages first.")
        else:
            print('context.pkl found. Loading context...')
            calibrated_data = find_data_context(load_context(), stage='findcont', context_key='datashape')
    else:
        calibrated_data = dict(data)

    res2 = dict(calibrated_data)
    # check if selcal is done
    # and if that is the case, apply best calibration solution to data
    if 'selfcal_sol' in calibrated_data[src]:
        print("Selfcal solution found, applying to data")
        caltable = calibrated_data[src]['selfcal_sol']
        res2 = applymodel(caltable, calibrated_data, src)
    # do per spw imaging (solve per spw and field)
    if interactive:
        image_data = asyncio.run(int_clean(res2, src, combine='scan', soltype='imaging'))
    else:
        image_data = solve(res2, src, combine='scan', soltype='imaging')
    qa_score = perspw_cont_imaging_qa_score(image_data)
    #qa_score['perspw_cont_image'] = image_data
    # export data
    archived_data = archive_export(image_data, src, paraxes='fieldandspw')
    #store context
    stored_context = add_to_context(inp=archived_data, key='image', stage='image_perspw_cont')
    stored_context = add_to_context(inp=qa_score, key='qa_scores', stage='image_perspw_cont')
    print(f'Final stored context: {stored_context}')
    create_qa_artifact(qa_score, artifact_type="table")

    return stored_context


if __name__ == '__main__':

    data = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
           'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
           'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'selfcal_sol':'caltable_loc'} }

    use_context = True # to test retrieving input data from the existing context
    if use_context:
        # fix the existing context 
        context = load_context()
        if 'image_cont_selfcal' in context and 'datashape' not in context['image_cont_selfcal']:
            updated_context = add_to_context(data, key='datashape', stage='image_cont_selfcal')
            data={}
    image_perspw_cont(data, src='target', interactive=False)
