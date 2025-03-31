from prefect import flow
from prefect.deployments import run_deployment

@flow
def scaling_test(task_size=1000):

    print(f"Number of tasks for this flow invocation: {task_size}")
    for tt in range(0, num_tasks):
        run_deployment(
            name="flow-test/flow overhead",
            parameters={},
            timeout=0
        )
        
    return True

if __name__ == "__main__":

    task_sizes = [1000, 2000, 4000, 8000, 16000, 32000]#, 64000, 100000]
    for num_tasks in task_sizes:
        scaling_test(num_tasks)
