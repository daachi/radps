import asyncio 

from prefect import flow, task, pause_flow_run
from prefect.logging import get_run_logger
from prefect.events import emit_event
from prefect.flow_runs import wait_for_flow_run
from prefect.deployments import run_deployment

from core import (fake_data, sleep_placeholder, randomly_fail, create_qa_artifact, Context, fake_qa_score,
                  qa_failure_condition, store_context)


@flow(log_prints=True)
async def run_calibrator_import_and_prep_in_parallel(calibrators, failures=False):
    """
    Run the calibrator import and prep stage in parallel.
    This is a workaround for Prefect's lack of direct support 
    for a flow.submit() analogous to task.submit()
    """
    sub_flows = []
    results = []

    for calibrator in calibrators:
        sub_flows.append(
            await run_deployment(
                    name="calibrator-data-import-and-prep/import data and prep",
                    parameters={"calibrator": calibrator, "failures": failures},
                    timeout=0,
                    )
        )

    data = []
#    for flow_run in sub_flows:
#        await wait_for_flow_run(flow_run.id, poll_interval=5)
#        results.append(fake_data((100, 100)))
    subflows = [wait_for_flow_run(flow_run.id, poll_interval=5) for flow_run in sub_flows]
    results = await asyncio.gather(*subflows)
    for result in results:
        data.append(fake_data((100, 100)))

    return data


# NOTE: could be combined with the similar task from target import
@task(retries=2, tags=["io"])
def import_data_from_archive(data, failures=False) -> dict:
    """
    Simulate importing data from the archive.
    """
    sleep_placeholder()
    if randomly_fail(on=failures):
        raise Exception("Import data from archive failed")
    else:
        return fake_data((1000, 1000))


# NOTE: could be combined with the similar task from target flagging
@task(tags=["flagging"])
def apply_online_flags(data):
    sleep_placeholder()
    return data


# NOTE: could be combined with the similar task from target flagging
@task(tags=["io"])
def get_antpos_info(data):
    sleep_placeholder()
    return fake_data((100, 100))


# NOTE: could be combined with the similar task from target flagging
@task(tags=["heuristics"])
def create_antpos_table(antenna_position_corrections, data):
    sleep_placeholder()
    return fake_data((100, 100))


# NOTE: could be combined with the similar task from target flagging
@task(tags=["calibration"])
def apply_antpos(table, data):
    sleep_placeholder()
    return fake_data((1000, 1000))


# NOTE: could be combined with the flow to import and prep target data
@flow(log_prints=True)
def calibrator_data_import_and_prep(calibrator, failures=False):
    """
    Import and prepare calibration data.
    """
    logger = get_run_logger()
    logger.info(f"Starting calibrator data import and prep for {calibrator}")

    context = Context()

    logger.info(f"Importing data from archive for {calibrator}")
    calibrator_data = import_data_from_archive(calibrator, failures=failures)

    logger.info(f"Applying online flags for {calibrator}")
    flagged_data = apply_online_flags(calibrator_data)

    logger.info(f"Getting antenna position information for {calibrator}")
    antpos_info = get_antpos_info(calibrator)

    logger.info(f"Creating antenna position table for {calibrator}")
    antpos_table = create_antpos_table(antpos_info, flagged_data)

    logger.info(f"Applying antenna position corrections for {calibrator}")
    result = apply_antpos(flagged_data, antpos_table)
    logger.info(f"Result of calibrator data import and prep for {calibrator}: {result}")

    logger.info("Updating context and creating QA artifact")
    qa_score = fake_qa_score('data_import_and_prep', result=result)
    create_qa_artifact(qa_score)
    context.update(qa_score)
    store_context(context=context)

    if qa_failure_condition(qa_score['data_import_and_prep'], failures_on=failures):
        emit_event(event="low_qa.imported.event!", resource={"prefect.resource.id": "test.id"})
        pause_flow_run()

    return context.path


if __name__ == "__main__":
    calibrators = ["J1752-2956", "J1851+0035"]
    run_calibrator_import_and_prep_in_parallel(calibrators)
