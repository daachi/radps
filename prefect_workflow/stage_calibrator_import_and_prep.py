from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.events import emit_event


from core import fake_data, sleep_placeholder, randomly_fail, create_qa_artifact, Context, fake_qa_score


# NOTE: could be combined with the similar task from target import
@task(retries=3, tags=["io"])
def import_data_from_archive(data) -> dict:
    """
    Simulate importing data from the archive.
    """
    sleep_placeholder()
    if randomly_fail():
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
def calibrator_data_import_and_prep(calibrator):
    """
    Import and prepare calibration data.
    """
    logger = get_run_logger()
    logger.info(f"Starting calibrator data import and prep for {calibrator}")

    context = Context()

    logger.info(f"Importing data from archive for {calibrator}")
    calibrator_data = import_data_from_archive(calibrator)

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

    if qa_score['data_import_and_prep'] < 0.67:
        emit_event(event="low_qa.imported.event!", resource={"prefect.resource.id": "test.id"})

    create_qa_artifact(qa_score)
    context.update(qa_score)
    context.save()

    return context.path
