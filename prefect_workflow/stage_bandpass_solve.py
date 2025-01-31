from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.events import emit_event

from core import fake_data, sleep_placeholder, randomly_fail, create_qa_artifact, Context, fake_qa_score


# Bandpass Solution
# NOTE: stage-specific
@task(tags=["flagging"])
def autoflag_bandpass(bp_data):
    sleep_placeholder()


# NOTE: stage-specific
@task(retries=3, tags=["io"])
def query_calmod(bandpass_calibrator):
    if randomly_fail():
        raise Exception("Query calmod failed")
    else:
        return fake_data((100, 100))


# NOTE: stage-specific
@task(tags=["heuristics"])
def calmod(bandpass_calibrator):
    sleep_placeholder()


# NOTE: stage-specific
@task(tags=["imaging"])
def save_model_vis(bp_data):
    sleep_placeholder()


# NOTE: stage-specific, but could use solver from Tak's work
@task(tags=["calibration"])
def amp_phase_solve(bp_data):
    sleep_placeholder()


# NOTE: stage-specific
@task(tags=["qa"])
def bandpass_qa_score(bp_data) -> dict:
    """
    Calculate a QA score for the bandpass solution.
    """
    sleep_placeholder()
    return fake_qa_score('bandpass_qa_score')


# NOTE: stage-specific
@flow(log_prints=True)
def bandpass_solve(bpcal):
    """
    Do the bandpass solution
    """
    logger = get_run_logger()
    logger.info(f"Starting bandpass solve for {bpcal}")

    context = Context.load()

    logger.info(f"Flagging bandpass data for {bpcal}")
    flagged_bandpass = autoflag_bandpass(bpcal)

    logger.info(f"Querying calmod for {bpcal}")
    query_calmod(bpcal)

    logger.info(f"Calmod for {bpcal}")
    calmod(bpcal)

    logger.info(f"Saving model vis for {bpcal}")
    flagged_bandpass_saved_model = save_model_vis(flagged_bandpass)

    logger.info(f"Calculating bandpass solution for {bpcal}")
    bandpass_solution = amp_phase_solve(flagged_bandpass_saved_model)  # TODO: expand this out to the solver loop
    qa_score = bandpass_qa_score(bandpass_solution)

    logger.info(f"Bandpass QA Scores: {qa_score['bandpass_qa_score']}")
    if qa_score['bandpass_qa_score'] < 0.67:
        emit_event(event="low_qa.bandpass.event!", resource={"prefect.resource.id": "test.id"})

    context.update(qa_score)
    context.save()
    create_qa_artifact(qa_score)
