# Stage: Continuum imaging with self-calibration

from prefect import task, flow, tags
from prefect.runtime import task_run, flow_run
from time import sleep
from example_calibration_pipeline import fake_qa_score, create_qa_artifact
ns = 1

# Re-usable across stages?
def generate_image_datashape(imsize,nchan=1,npol=1)-> dict:
    imageshape = {'x':imsize, 'y':imsize, 'nchan':nchan, 'npol':npol}
    return imageshape

def generate_vis_datashape(addchan=False) -> dict:
    """ Return predefine vis data shape """
    datashape = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
                 'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
                 'target':{'n_field':1, 'n_spw':3, 'n_scan':5} }
    if addchan:
        nchan = 2
        datashape['target']['n_nchan'] = nchan
    return datashape 
    
# Re-usable across stage once with modification
def generate_flow_name():
    """ generate flow name based on runtime info"""
    flow_name = flow_run.flow_name 
    flow_params = flow_run.parameters
    typeparam = flow_params['soltype']
    return f"{typeparam}_{flow_name}"

# Re-usable across stages but execpt inp (data) to be in a specific format``
@task
def load_context(inp,src):
    """load context"""
    seldata = dict()
    # assume here inp is a data structure without any data
    if isinstance(inp,dict) and src in inp:
        seldata[src] = dict(inp[src])
    return seldata

@task
def data_prep(inp):
    sleep(ns)
    return inp

@task 
def calc_qa(inp):
    """Calculate QA metrics"""
    sleep(ns)
    #pass or fail
    return inp

@task
def calc_heuristics(inp, type=''):
    """Calculate heuristics """
    sleep(ns)
    if type == 'boolean':
        return True
    else:
        return inp


@task # in-algorithm parallelism
def calc_update_direction(inp, id):
    """Calculte update direction"""
    sleep(ns)
    return 

# following tasks probably need to be flows as it involves parallelism
@task # in-algorithm parallelism
def update_model(inp, id=0):
    """update model""" 
    sleep(ns)
    return

@task # in-algorithm parallelism
def check_converge(inp, id=0):
    """check convergence"""
    sleep(ns)
    return

@task
def applymodel(inp, datashape, src):
   """ Apply calibration model """ 
   sleep(ns)
   return datashape

@flow
def store_context(inp):
    """Store context"""
    sleep(ns)
    ret = inp
    return ret

@task
def archive_export_func(inp,id=0):
    """archive and export results"""
    sleep(ns)
    return

@flow
def archive_export(inp, src, paraxes='fieldandspw') -> list:
    """Export and archive results
    parallize by field and spw ('fieldandspw')
    or 
    parallize by field ('field')
    """
    n_field = inp[src]['n_field']
    n_spw = inp[src]['n_spw']
    if paraxes == 'fieldandspw':
        npar = n_field*n_spw
    else:
        npar = n_field

    exp_par = []
    for i in range(0, npar):
        exp_par.append(archive_export_func.submit(inp,i))
    sleep(ns)
    return [j.result() for j in exp_par]


@flow(flow_run_name=generate_flow_name)
def solve(inp, src, combine=None, niter=2, soltype='calibration'):
    """
    general solver
      soltype determines main output result type either caltable/visibilities or images   
    """
    datashape = dict(inp)
    n_field = datashape[src]['n_field']
    n_spw = datashape[src]['n_spw']
    n_scan = datashape[src]['n_scan']
    n_chan = 1
    if soltype == 'cube_imaging':
        if 'n_chan' in datashape[src]:
            n_chan = datashape[src]['n_chan']

    prep = data_prep(inp)
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
            ret = inp
        model_par = [] # calibration solutions(caltable) or images
        for i in range(0, n_par): ## separate solution for each of n_par
            model = prep

            for iter in range(0,niter):  ## Number of solver loops (iterations)
                res_par=[]
                for j in range(0,n_comb): ## In-algorithm parallelism 
                    res_par.append(calc_update_direction.submit(model,j)) ## caltable pre-apply (or model vis prediction) happens on the same parallelization axis as the update_direction calculation.
                modelc = update_model(res_par,i)
                model = check_converge(modelc,i)
            model_par.append(model)
        if ret==dict() and type == 'calibration':
            ret = model_par
        elif 'imaging' in soltype:
            if soltype == 'cube_imaging':
                print('n_chan==', n_chan)
                ret = generate_image_datashape(512,nchan=n_chan) 
            else:
                ret = generate_image_datashape(512)
            ret.update(dict(inp))
    return ret

@flow (description='Continuum imaging with self-calibration stage')
def image_cont_selfcal(inp,src='target', doselfcal=False):
    """workflow for continuum imaging with self-calibration"""

    # load target calibrated visibility data 
    calibrated_target_vis = load_context(inp,src=src)
    
    # make aggregate continuum image
    with tags('initial imaging'):
        target_image_data = solve(calibrated_target_vis,
                                  src='target', combine='both', soltype='imaging')
    
    # QA: doselfcal = True and SNR > predifined_SNR_threshold 
    qa_result = calc_heuristics(target_image_data, type='bool')

    selfcalresult = {}
    selfcal_hueristics = True
    count = 0
    if qa_result and doselfcal: # QA passes and selfcal is requested
         # selfcal iteration loop
        while(selfcal_hueristics):
            with tags('gain calibration'):
                cal_table = solve(calibrated_target_vis,src='target',combine='spw')
            updated_data = applymodel(cal_table,inp, src='target') ## Apply caltables.
            with tags('selfcal imaging'):
                updated_image = solve(updated_data,src='target',combine='both',soltype='imaging') 
            # Save model visibilities
            updated_model_data = applymodel(updated_data,inp,src='target')
            #qascore = calc_qa(updated_image)
            qa_score = fake_qa_score('selfcal_qa_score')
            print('count=',count)
            selfcalresult[count]=dict()
            selfcalresult[count]['updated_image']=updated_image
            selfcalresult[count]['QA'] = qa_score
            # Currently, selfcal_hueristics is a boolean but in real case
            # this should include new parameters to solve in next self-cal cycle....
            selfcal_hueristics = calc_heuristics(selfcalresult,type='boolean')   
            # if qascore and/or selfcal_hueristics need to backout 
            # to previous images/vis data as final result
            # get out of loop for now (assuming 'stop selfcal' condition reached) 
            if count == 2: 
                print('Iteration count limit reached for selfcal loop')
                selfcal_hueristics = False
            count += 1
    else:
        print('Self-calibration not performed.')        
        return 'skipped'
    # Usually it requires to rollback to previous images and cal solutions when exit from
    # selfcal loop and before saving the results. 
    # selfcalresult['updated_image'] = previous_image
    # Export continuum images, parallelize by field only
    print('selfcalresult=',selfcalresult)
    archived_data = archive_export(selfcalresult[2]['updated_image'],src='target',paraxes='field')
    stored_context = store_context(archived_data)
    create_qa_artifact(qa_score)
    return stored_context


if __name__ == "__main__":
   inp = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
             'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
             'target':{'n_field':1, 'n_spw':3, 'n_scan':5} }
   #task_solve(inp,datashape,src='target')
   stage_image_cont_selfcal(inp, doselfcal=True)