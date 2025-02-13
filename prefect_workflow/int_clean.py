# interactive clean simulation
from prefect import task, flow, pause_flow_run
from prefect.events import emit_event
from prefect.input import RunInput
import asyncio
from time import sleep

class UserInput(RunInput):
    cycleniter: int
    threshold: float 
    stop: bool = False
    interactive: bool = True

@flow
def clean_engine(niter, iterdone=0, maxiter=None):
    if maxiter is not None and niter+iterdone > maxiter: 
        niter = maxiter - iterdone
        print(f'cycleniter is adjusted to {niter}')
    for i in range(niter):
        sleep(0.5)
    return niter

@flow (log_prints=True)
async def int_clean(maxiter = 10):
    # run initial clean
    niter = clean_engine(1)
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
                print('Stopping int_clean')
                break
            elif not user_input.interactive:
                interactive = False 
                print('Continue to finish with non-interactive mode')
        newniter = clean_engine(user_input.cycleniter, iterdone, maxiter)
        print(f'iteration done in this cycle: {newniter}')
        previous_cycleniter = user_input.cycleniter
        previous_threshold = user_input.threshold
        iterdone = iterdone + newniter
        print(f'iterdone so far: {iterdone}')
        if iterdone >= maxiter:
           print('Reached iteration limit')
           break
    #print(f'maxiter={maxiter}, iterdone={iterdone}')
    return iterdone

if __name__ == "__main__":
    ret = asyncio.run(int_clean())
    print(f' done {ret} iterations')

