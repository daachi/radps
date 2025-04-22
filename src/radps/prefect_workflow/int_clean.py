# interactive clean simulation
from prefect import task, flow, pause_flow_run
from prefect.events import emit_event
from prefect.input import RunInput
import asyncio
from time import sleep
from stage_image_cont_selfcal import solve
class UserInput(RunInput):
    cycleniter: int
    threshold: float 
    stop: bool = False
    interactive: bool = True

@flow
#def clean_engine(niter, iterdone=0, maxiter=None):
def clean_engine(data, src, niter, iterdone=0, combine=None, soltype=None, maxiter=None):
    if maxiter is not None and niter+iterdone > maxiter: 
        niter = maxiter - iterdone
        print(f'cycleniter is adjusted to {niter}')
    for i in range(niter):
        sleep(0.5)
    ret = solve(data, src, combine=combine, niter=niter, soltype=soltype)
    ret['niter']=niter
    return ret 

@flow (log_prints=True)
async def int_clean(data, src, combine='scan', soltype='cube_imaging', maxiter=10):
    # run initial clean
    #niter = clean_engine(1)
    ret = clean_engine(data, 'target', 1, combine=combine, soltype=soltype)
    niter = ret['niter']
    previous_cycleniter = 3
    previous_threshold = 0.1
    iterdone = 0
    interactive = True
    for i in range(maxiter):

        if niter == 1:
            newniter = niter
            niter = 0
        
        if interactive:
            emit_event(event=f"iterdone this cycle: {newniter}", 
               resource={"prefect.resource.id": "test.id"})

            # pause for input
            user_input = await pause_flow_run(wait_for_input=UserInput.with_initial_data(
                description='Continue or Stop?', 
                stop=False,
                interactive = True,
                cycleniter=previous_cycleniter,
                threshold=previous_threshold))

            if user_input.stop:
                emit_event(event=f"Stopping int_clean", 
                       resource={"prefect.resource.id": "test.id"})
                print('Stopping int_clean')
                break
            elif not user_input.interactive:
                interactive = False 
                emit_event(event=f"clean continue with non-interactive mode using the current parameters", 
                       resource={"prefect.resource.id": "test.id"})
                print('Continue to finish with non-interactive mode')
        #newniter = clean_engine(user_input.cycleniter, iterdone, maxiter)
        ret = clean_engine(data, 
                           'target', 
                           user_input.cycleniter, 
                           iterdone, 
                           combine=combine,
                           soltype=soltype,
                           maxiter=maxiter)
        newniter = ret['niter']
        print(f'iteration done in this cycle: {newniter}')
        previous_cycleniter = user_input.cycleniter
        previous_threshold = user_input.threshold
        iterdone = iterdone + newniter
        print(f'iterdone so far: {iterdone}')
        if iterdone >= maxiter:
           emit_event(event=f"Reached iteration limit: {maxiter}", 
               resource={"prefect.resource.id": "test.id"})
           print('Reached iteration limit')
           break
    #print(f'maxiter={maxiter}, iterdone={iterdone}')
    ret['iterdone'] = iterdone
    ret.update(data)
    return ret 

if __name__ == "__main__":
    data = {'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
             'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
             'target':{'n_field':1, 'n_spw':3, 'n_scan':5} }
    ret = asyncio.run(int_clean(data,'target'))
    print(f' done {ret} iterations')

