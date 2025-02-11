from prefect import flow, task, pause_flow_run
from prefect.logging import get_run_logger
from prefect.events import emit_event

from core import (fake_data, sleep_placeholder, randomly_fail, create_qa_artifact, Context, fake_qa_score,
                  qa_failure_condition, load_context, store_context)


# Bandpass Solution
@task(tags=["flagging"])
def autoflag_bandpass(bp_data):
    sleep_placeholder()


@task(retries=2, tags=["io"])
def query_calmod(bandpass_calibrator, failures=False):
    if randomly_fail(on=failures):
        raise Exception("Query calmod failed")
    else:
        return fake_data((100, 100))


@task(tags=["heuristics"])
def calmod(bandpass_calibrator):
    sleep_placeholder()


@task(tags=["imaging"])
def save_model_vis(bp_data):
    sleep_placeholder()


@task(tags=["calibration"])
def amp_phase_solve(bp_data):
    sleep_placeholder()


@task(tags=["qa"])
def bandpass_qa_score(bp_data) -> dict:
    """
    Calculate a QA score for the bandpass solution.
    """
    sleep_placeholder()
    return fake_qa_score('bandpass_qa_score')


@flow(log_prints=True)
def bandpass_solve(bpcal, failures=False):
    """
    Do the bandpass solution
    """
    logger = get_run_logger()
    logger.info(f"Starting bandpass solve for {bpcal}")

    context = load_context()

    logger.info(f"Flagging bandpass data for {bpcal}")
    flagged_bandpass = autoflag_bandpass(bpcal)

    logger.info(f"Querying calmod for {bpcal}")
    query_calmod(bpcal, failures=failures)

    logger.info(f"Calmod for {bpcal}")
    calmod(bpcal)

    logger.info(f"Saving model vis for {bpcal}")
    flagged_bandpass_saved_model = save_model_vis(flagged_bandpass)

    logger.info(f"Calculating bandpass solution for {bpcal}")
    bandpass_solution = amp_phase_solve(flagged_bandpass_saved_model)  # TODO: expand this out to the solver loop
    qa_score = bandpass_qa_score(bandpass_solution)

    logger.info("Updating context and creating QA artifact")
    context.update(qa_score)
    store_context(context=context)
    create_qa_artifact(qa_score)
    logger.info(f"Bandpass QA Scores: {qa_score['bandpass_qa_score']}")

    if qa_failure_condition(qa_score['bandpass_qa_score'], failures_on=failures):
        emit_event(event="low_qa.bandpass.event!", resource={"prefect.resource.id": "test.id"})
        pause_flow_run()


if __name__ == "__main__":
    bandpass_solve()
