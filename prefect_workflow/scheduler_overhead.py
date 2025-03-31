from prefect import flow
from prefect.deployments import run_deployment

@flow
def scaling_test():
    #task_sizes = [1000, 2000, 4000, 8000, 20000, 40000, 60000, 80000]
    task_sizes = [1000]

    for num_tasks in task_sizes:
        for tt in range(0, num_tasks):
            run_deployment(
                name="flow-test/flow overhead",
                parameters={},
                timeout=0
            )
        
    return True

if __name__ == "__main__":

    scaling_test()
