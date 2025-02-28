import random

from prefect import flow, task, pause_flow_run
from prefect.logging import get_run_logger
from prefect.events import emit_event

from core import (sleep_placeholder, create_qa_artifact,
                  fake_qa_score, qa_failure_condition,
                  load_context, add_to_context)
from stage_image_cont_selfcal import find_data_context, solve

from typing import List

# Time Gain Solve
@task(tags=["heuristics"])
def calc_snr(gaincal):
    sleep_placeholder()


@task(tags=["calibration"])
def per_spw_gain_soln(gaincal, src):
    return solve(gaincal['datashape'], src=src)


@task(tags=["calibration"])
def best_spw_gain_soln(gaincal, src):
    return solve(gaincal['datashape'], src=src)


@task(tags=["calibration"])
def combinespw_gain_soln(gaincal, src):
    return solve(gaincal['datashape'], src=src, combine="spw")



@task(tags=["calibration"])
def global_gain_soln(gaincal, src):
    return solve(gaincal['datashape'], src=src, combine="spw")


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
def time_gain_solve(gaincal_name, gaincal, failures=False):
    logger = get_run_logger()
    logger.info(f"Starting time gain solve for {gaincal}")

    context = load_context()

    try:
        gaincal = find_data_context(context, stage="stage_calibrator_data_import_and_prep", context_key='datashape')
    except:
        data = {gaincal_name:{'n_field':1, 'n_spw':3, 'n_scan':1},
                'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
                'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1} }
        gaincal = {}
        gaincal['datashape'] = {}
        gaincal['datashape'][gaincal_name] = dict(data[gaincal_name])

    spws = [0, 1, 2, 3]  # pretend these come from the load_context() call

    logger.info(f"Calculating SNR for {gaincal_name}")
    snr = calc_snr(gaincal)

    if all_spws_high_snr(spws, snr):
        per_spw_gain_soln(gaincal, gaincal_name)
    elif any_spw_high_snr(spws, snr):
        best_spw_gain_soln(gaincal, gaincal_name)
    else:
        combinespw_gain_soln(gaincal, gaincal_name)
    result = global_gain_soln(gaincal, gaincal_name)

    qa = gaincal_qa_score(result)

    create_qa_artifact(qa)
    add_to_context(result, key="caltable", stage="gaincal")
    new_context = add_to_context(qa, key="qa", stage="gaincal")
    print("context updated after gaincal:")
    print(new_context)
    logger.info(f"Gaincal QA Scores: {qa['gaincal_qa_score']}")

    if qa_failure_condition(qa['gaincal_qa_score'], failures_on=failures):
        emit_event(event="low_qa.gaincal.event!", resource={"prefect.resource.id": "test.id"})
        pause_flow_run()


if __name__ == "__main__":
    gaincal_name = "J1851+0035"
    data = {gaincal_name:{'n_field':1, 'n_spw':3, 'n_scan':1},
        'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
        'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1} }
    gaincal = {}
    gaincal['datashape'] = {}
    gaincal['datashape'][gaincal_name] = dict(data[gaincal_name])
    time_gain_solve(gaincal, gaincal_name)
