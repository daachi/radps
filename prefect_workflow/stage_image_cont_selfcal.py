# Stage: Continuum imaging with self-calibration
from prefect import task, flow, tags
from prefect.runtime import task_run, flow_run
from core import (fake_qa_score, create_qa_artifact, sleep_placeholder,
                  load_context, add_to_context)
import numpy as np
from matplotlib.image import imsave
import os
import pathlib

# Re-usable across stages?
def generate_image_datashape(imsize,nchan=1,npol=1)-> dict:
    """ Generate a fake image data shape """
    imageshape = {'x':imsize, 'y':imsize, 'nchan':nchan, 'npol':npol}
    return imageshape

def generate_vis_datashape(addchan=False) -> dict:
    """ Return predefined vis data shape """
    datashape = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
                 'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
                 'target':{'n_field':1, 'n_spw':3, 'n_scan':5} }
    if addchan:
        nchan = 2
        datashape['target']['n_nchan'] = nchan
    return datashape 

def generate_fake_image(datashape):
    """ Generate a fake png image """ 
    imsize = datashape['x']
    nchan = datashape['nchan']
    npol = datashape['npol']
    imdata = np.zeros((imsize,imsize,nchan,npol))
    imdata[0,0,0,0] = 1.0
    imsave('fake_image.png',imdata)
    imageurl = pathlib.Path('fake_image.png').resolve().as_uri()
    return imageurl 


# Can be re-used across stages but need to be modified to be useful   
def generate_flow_name() -> str:
    """ Generate flow name based on runtime info"""
    flow_name = flow_run.flow_name 
    flow_params = flow_run.parameters
    # solove flow specific parameter 
    if 'soltype' in flow_params:
        typeparam = flow_params['soltype']
    return f"{typeparam}_{flow_name}"

# Imaging stage specific functions
@task
def data_prep(data):
    sleep_placeholder(1.0)
    return data 

@task
def calc_heuristics(data, type=''):
    """Calculate heuristics """
    sleep_placeholder(1.0) 
    if type == 'boolean':
        return True
    else:
        return data 

@task
def solve_model(data, id, niter=2):
    for i in range(0,niter):
        calc_update_direction(inp,id)
        update_model(inp,id)
        check_converge(inp,id)
    return

@task # in-algorithm parallelism
def calc_update_direction(data, id):
    """Calculte update direction"""
    sleep_placeholder(1.0)
    return 

@task 
def gather_direction(data):
    """Gather update direction"""
    sleep_placeholder(1.0)
    return 

# following tasks probably need to be flows as it involves parallelism
@task # in-algorithm parallelism
def update_model(data, id=0):
    """Update model""" 
    sleep_placeholder(1.0)
    return

@task # in-algorithm parallelism
def check_converge(data, id=0):
    """Check convergence"""
    sleep_placeholder(1.0)
    return

@task
def applymodel(inp, datashape, src):
   """ Apply calibration model """ 
   sleep_placeholder(1.0)
   return datashape


@task
def archive_export_func(inp,id=0):
    """Archive and export results"""
    sleep_placeholder(1.0)
    return

@flow
def archive_export(data, src, paraxes='fieldandspw') -> list:
    """Export and archive results
    parallize by field and spw ('fieldandspw')
    or 
    parallize by field ('field')
    """
    n_field = data[src]['n_field']
    n_spw = data[src]['n_spw']
    if paraxes == 'fieldandspw':
        npar = n_field*n_spw
    else:
        npar = n_field

    exp_par = []
    for i in range(0, npar):
        exp_par.append(archive_export_func.submit(data,i))
    sleep_placeholder(1.0)
    return [j.result() for j in exp_par]


@flow(flow_run_name=generate_flow_name)
def solve(data, src, combine=None, niter=2, soltype='calibration'):
    """
    general solver
      soltype determines main output result type either caltable/visibilities or images   
    """
    datashape = dict(data)
    n_field = datashape[src]['n_field']
    n_spw = datashape[src]['n_spw']
    n_scan = datashape[src]['n_scan']
    n_chan = 1
    if soltype == 'cube_imaging':
        if 'n_chan' in datashape[src]:
            n_chan = datashape[src]['n_chan']

    prep = data_prep(data)
    cal_par = []
    ret = {} 
    if combine is None:
        for i in range(0,n_field*n_spw*n_scan):
            cal_par.append(solve_model.submit(prep, i) )
            ret = [i.result() for i in cal_par]
    else:
        if combine == 'scan':
            if soltype == 'cube_imaging':
                n_par = n_field*n_spw*n_chan 
            else:
                n_par = n_field*n_spw
            n_comb = n_scan
        elif combine == 'spw':
            n_par = n_field*n_scan
            n_comb = n_spw
        else:  # combine spw and scan
            n_par = n_field
            n_comb = n_spw*n_scan
            ret = data 
        model_par = [] # calibration solutions(caltable) or images
        #for i in range(0, n_par): ## separate solution for each of n_par
        model = prep

        for iter in range(0,niter):  ## Number of solver loops (iterations)
            #    for j in range(0,n_comb): ## In-algorithm parallelism 
                    # caltable pre-apply (or model vis prediction) happens on the same parallelization 
                    # axis as the update_direction calculation.
            res_futures = [calc_update_direction.submit(model,j) for j in range(0, n_comb)] 
            # gather updated directions
            dir_res = gather_direction.submit(res_futures)
            # n_par parallelization
            modelc_futures = [update_model.submit(dir_res,i) for i in range(0, n_par)]
            model_futures = [check_converge.submit(modelc_future) for modelc_future in modelc_futures] 
            model = [model_future for model_future in model_futures] 
            for model_future in model_futures:
                model_future.wait()
            model_par.append(model)
        if ret==dict() and type == 'calibration':
            ret = model_par
        elif 'imaging' in soltype:
            # add input vis data(shape) info 
            srcdata = dict()
            srcdata[src] = dict(datashape[src])
            if soltype == 'cube_imaging':
                ret['image'] = generate_image_datashape(512,nchan=n_chan) 
            else:
                ret['image'] = generate_image_datashape(512)
            ret.update(srcdata)
    return ret

@flow (log_prints=True, description='Continuum imaging with self-calibration stage')
def image_cont_selfcal(data: dict={}, src: str='target', doselfcal: bool=False):
    """Continuum imaging with self-calibration"""
    print("Stating continuum imaging with self-calibration")

    # load target calibrated visibility data 
    if data == dict():
        print('Loading existing context...')
        if not os.path.exists('context.pkl'): 
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
    else:
        calibrated_data = dict(data)
    
    # make aggregate continuum image
    with tags('Initial imaging pre-selfcal'):
        target_image_data = solve(calibrated_data,
                                  src='target', combine='both', soltype='imaging')
    
    # QA: doselfcal = True and SNR > predifined_SNR_threshold 
    qa_result = calc_heuristics(target_image_data, type='bool')

    selfcalresult = {}
    selfcal_hueristics = True
    count = 0
    lastiter = 0
    if qa_result and doselfcal: # QA passes and selfcal is requested
         # selfcal iteration loop
        while(selfcal_hueristics):
            with tags('gain calibration'):
                cal_table = solve(calibrated_data,src='target',combine='spw')
            updated_data = applymodel(cal_table,inp, src='target') ## Apply caltables.
            with tags('selfcal imaging'):
                updated_image = solve(updated_data,src='target',combine='both',soltype='imaging') 
            # Save model visibilities
            updated_model_data = applymodel(updated_data,inp,src='target')
            #qascore = calc_qa(updated_image)
            qa_return = fake_qa_score('image_SNR')
            snr = dict()
            snr['image_SNR'] = 10*qa_return['image_SNR'] # make fake SNR using qa value
            selfcalresult[count]=dict()
            selfcalresult[count]['updated_image']=updated_image
            selfcalresult[count]['QA'] = snr 
            # Currently, selfcal_hueristics is a boolean but in real case
            # this should include new parameters to solve in next self-cal cycle....
            selfcal_hueristics = calc_heuristics(selfcalresult,type='boolean')   
            # if qascore and/or selfcal_hueristics need to backout 
            # to previous images/vis data as final result
            # get out of loop for now (assuming 'stop selfcal' condition reached) 
            if count != 0:
                if selfcalresult[count]['QA']['image_SNR'] < selfcalresult[count-1]['QA']['image_SNR']:
                    selfcal_hueristics = False # stop selfcal as SNR degraded.
                    lastiter = count-1
                elif count == 2: 
                    print("Iteration count limit reached for selfcal loop")
                    lastiter = count
                    selfcal_hueristics = False
            else:
                count += 1
    else:
        print("Self-calibration not performed.")        
        return 'skipped'
    # Usually it requires to rollback to previous images and cal solutions when exit from
    # selfcal loop and before saving the results. 
    # selfcalresult['updated_image'] = previous_image
    # Export continuum images, parallelize by field only
    archived_data = archive_export(selfcalresult[lastiter]['updated_image'],src='target',paraxes='field')
    stored_context = add_to_context(inp=archived_data)
    print(f'Final stored context: {stored_context}')    
    #create_qa_artifact(selfcalresult[lastiter]['QA'], artifact_type='table')   
    create_qa_artifact(selfcalresult[lastiter]['QA'])   
    return updated_image


if __name__ == "__main__":
   inp = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
             'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
             'target':{'n_field':1, 'n_spw':3, 'n_scan':5} }
   #task_solve(inp,datashape,src='target')
   image_cont_selfcal(inp, doselfcal=True)
