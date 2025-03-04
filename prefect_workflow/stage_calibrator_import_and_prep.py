import asyncio

from prefect import flow, pause_flow_run
from prefect.logging import get_run_logger
from prefect.events import emit_event
from prefect.flow_runs import wait_for_flow_run
from prefect.deployments import run_deployment
from stage_data_import_and_prep import (fake_archive_query, alma_antpos_query, fake_flagging,
                                        generate_and_apply_gain_table)

from core import (generate_random_complex_array, create_qa_artifact, create_context, fake_qa_score,
                  qa_failure_condition, add_to_context, load_context)


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
    subflows = [wait_for_flow_run(flow_run.id, poll_interval=5) for flow_run in sub_flows]
    results = await asyncio.gather(*subflows)
    for result in results:
        data.append(generate_random_complex_array((100, 100)))

    return data


# NOTE: could be combined with the flow to import and prep target data
@flow(log_prints=True)
def calibrator_data_import_and_prep(calibrator, failures=False):
    """
    Import and prepare calibration data.
    """
    logger = get_run_logger()
    logger.info(f"Starting calibrator data import and prep for {calibrator}")

    context = create_context()

    logger.info(f"Importing data from archive for {calibrator}")
    calibrator_data = fake_archive_query(calibrator, failures=failures, extra_data=False, stage_name="calibrator_data_import_and_prep")

    logger.info(f"Applying online flags for {calibrator}")
    flagged_data = fake_flagging(calibrator_data, calibrator)

    logger.info(f"Getting antenna position information for {calibrator}")
    antpos_result_json = alma_antpos_query(calibrator)

    logger.info(f"Generating and applying antenna position corrections for {calibrator}")
    generate_and_apply_gain_table(flagged_data, antpos_result_json, calibrator, stage_name="calibrator_data_import_and_prep")

    logger.info("Updating context and creating QA artifact")
    qa_name = f"calibrator_data_import_and_prep_{calibrator}"
    qa_score = fake_qa_score(qa_name)
    create_qa_artifact(qa_score)
    current_context = add_to_context(qa_score, key="qa", stage="calibrator_data_import_and_prep")

    if qa_failure_condition(qa_score[qa_name], failures_on=failures):
        emit_event(event="low_qa.imported.event!", resource={"prefect.resource.id": "test.id"})
        pause_flow_run()

    print("Context after calibrator data import and prep:")
    print(load_context())


if __name__ == "__main__":
    calibrators = ["J1752-2956", "J1851+0035"]
    run_calibrator_import_and_prep_in_parallel(calibrators)
