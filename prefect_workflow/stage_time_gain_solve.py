import random

from prefect import flow, task, pause_flow_run
from prefect.logging import get_run_logger
from prefect.events import emit_event

from core import (sleep_placeholder, create_qa_artifact,
                  fake_qa_score, qa_failure_condition,
                  load_context, store_context)
from typing import List

# Time Gain Solve
@task(tags=["heuristics"])
def calc_snr(gaincal):
    sleep_placeholder()


# NOTE: stage-specific, but could use solver from Tak's work
@task(tags=["calibration"])
def per_spw_gain_soln(gaincal):
    sleep_placeholder()


# NOTE: stage-specific, but could use solver from Tak's work
@task(tags=["calibration"])
def best_spw_gain_soln(gaincal):
    sleep_placeholder()


# NOTE: stage-specific, but could use solver from Tak's work
@task(tags=["calibration"])
def combinespw_gain_soln(gaincal):
    sleep_placeholder()


# NOTE: stage-specific, but could use solver from Tak's work
@task(tags=["calibration"])
def global_gain_soln(gaincal):
    sleep_placeholder()


@task(tags=["qa"])
def gaincal_qa_score(gaincal) -> dict:
    """
    Calculate a QA score for the gaincal solution.
    """
    sleep_placeholder()
    return fake_qa_score('gaincal_qa_score')


@task(tags=["heuristics"])
def all_spws_high_snr(spws: List[int], snr) -> bool:
    """
    Return True if all spws have high SNR.
    """
    return random.choice([True, False])


@task(tags=["heuristics"])
def any_spw_high_snr(spws: List[int], snr) -> bool:
    """
    Return True if any spw has high SNR.
    """
    return random.choice([True, False])


@flow(log_prints=True)
def time_gain_solve(gaincal, failures=False):
    logger = get_run_logger()
    logger.info(f"Starting time gain solve for {gaincal}")

    context = load_context()
    spws = [0, 1, 2, 3]  # pretend these come from the load_context() call

    logger.info(f"Calculating SNR for {gaincal}")
    snr = calc_snr(gaincal)

    if all_spws_high_snr(spws, snr):
        per_spw_gain_soln(gaincal)
    elif any_spw_high_snr(spws, snr):
        best_spw_gain_soln(gaincal)
    else:
        combinespw_gain_soln(gaincal)
    result = global_gain_soln(gaincal)

    qa = gaincal_qa_score(result)

    create_qa_artifact(qa)
    context.update(qa)
    store_context(context=context)
    logger.info(f"Gaincal QA Scores: {qa['gaincal_qa_score']}")

    if qa_failure_condition(qa['gaincal_qa_score'], failures_on=failures):
        emit_event(event="low_qa.gaincal.event!", resource={"prefect.resource.id": "test.id"})
        pause_flow_run()


if __name__ == "__main__":
    time_gain_solve()
