# Stage: Continuum imaging with self-calibration

from prefect import task, flow
from prefect import runtime, tags
from time import sleep

ns = 1

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
    return 

@task
def calc_heuristics(inp, type):
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
def solve(inp, src, combine=None, niter=2):
    """general solver"""
    datashape = dict(inp)
    n_field = datashape[src]['n_field']
    n_spw = datashape[src]['n_spw']
    n_scan = datashape[src]['n_scan']
    print("running solve...")
    prep = data_prep(inp)
    cal_par = []
    ret = []
    if combine is None:
        for i in range(0,n_field*n_spw*n_scan):
            cal_par.append(solve_model.submit(prep, i) )
            ret = [i.result() for i in cal_par]
    else:
        if combine == 'scan':
            n_par = n_field*n_spw
            n_comb = n_scan
        elif combine == 'spw':
            n_par = n_field*n_scan
            n_comb = n_spw
        else:  # combine spw and scan
            n_par = n_field
            n_comb = n_spw*n_scan
            ret = inp
        model_par = []
        for i in range(0, n_par): ## separate solution for each of n_par
            model = prep # prefect.get...

            for iter in range(0,niter):  ## Number of solver loops (iterations)
                res_par=[]
                for j in range(0,n_comb): ## In-algorithm parallelism 
                    res_par.append(calc_update_direction.submit(model,j)) ## caltable pre-apply (or model vis prediction) happens on the same parallelization axis as the update_direction calculation.
                modelc = update_model(res_par,i)
                model = check_converge(modelc,i)
            model_par.append(model)
        if ret==[]:
            ret = model_par
        
    return ret

@flow (description='Continuum imaging with self-calibration stage')
def stage_image_cont_selfcal(inp,src='target', doselfcal=False):
    """workflow for continuum imaging with self-calibration"""
    # load target calibrated visibility data 
    calibrated_target_vis = load_context(inp,src=src)
    print('calibrated_target_vis=',calibrated_target_vis)
    # make aggregate continuum image
    with tags('initial imaging'):
        target_image_data = solve(calibrated_target_vis,src='target',combine='both') ## Cont Image each field separately. Combine on scan and spw. 
    print('initial imaging done target_image_data=', target_image_data)
    # QA: doselfcal = True and SNR > snrThreshold
    qa_result = calc_heuristics(target_image_data, type='bool')

    selfcalresult = {}
    selfcal_hueristics = True
    count = 0
    if qa_result and doselfcal: # QA passes and selfcal is requested
         # selfcal iteration loop
        while(selfcal_hueristics):
            print('cal_table solve')
            cal_table = solve(calibrated_target_vis,src='target',combine='spw') ## Gain Solve per timestep (with combinespw)
            updated_data = applymodel(cal_table,inp, src='target') ## Apply caltables.
            print('image solve')
            updated_image = solve(updated_data,src='target',combine='both') ## Cont Image each field separately. Combine on scan and spw. 
            updated_model_data = applymodel(updated_data,inp,src='target') ## Save model visibilities
            qascore = calc_qa(updated_image)
            selfcalresult['updated_image']=updated_image
            selfcalresult['QA'] = qascore
            selfcal_hueristics = calc_heuristics(selfcalresult,type='boolean')   
            count += 1
            # get out of loop 
            if count == 2: 
                print('Iteration count limit reached for selfcal loop')
                selfcal_hueristics = False
    #res6 = task_archive_export(res,datashape,src='target',par=2) ## Export continuum images, parallelize by field only
    #res7 = task_store_context(res6)
    return 


if __name__ == "__main__":
   inp = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
             'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
             'target':{'n_field':1, 'n_spw':3, 'n_scan':5} }
   #task_solve(inp,datashape,src='target')
   stage_image_cont_selfcal(inp, doselfcal=True)