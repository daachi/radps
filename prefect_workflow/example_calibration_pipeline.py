from prefect import flow, task
from prefect.artifacts import (
    create_markdown_artifact,
    create_table_artifact,
    create_image_artifact
    )
from prefect.logging import get_run_logger

from typing import List

import numpy as np
import time
import random

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

# Calibrator Data Import and Prep

# TODO: if we keep the 'tags' thing, define tags for each rather than copy-paste strings


# NOTE: Since nested flows cannot be cancelled without cancelling the
# parent flow, in the future we should consider using separate
# deployments for each step in the pipeline as recommended in
# https://docs.prefect.io/v3/develop/write-flows
@flow(log_prints=True)
def calibration_pipeline_example():
    """
    Example calibration pipeline implementation in Prefect from Figure 1 of "An Example RADPS Workflow Decomposition"
    """
    bpcal = "bandpass_calibrator_name"
    gaincal = "gain_calibrator_name"
    calibrators = [bpcal, gaincal]

    for calibrator in calibrators:
        calibrator_data_import_and_prep(calibrator)  # TODO: do this in parallel eventually, but cannot as a flow
    bandpass_solve(bpcal)
    time_gain_solve(gaincal)
    for calibrator in calibrators:  # TODO: do this in parallel eventually, but cannot as a flow
        image_calibrator(calibrator)


def fake_data(dimensions: tuple) -> dict:
    """
    Create fake data
    """
    data = np.random.rand(*dimensions)
    return data


def fake_qa_score(name: str = None) -> dict:
    """
    Create a fake QA score withith a random value.
    """
    score = random.random()
    if name:
        return {name: score}
    else:
        return {'qa_score': score}


def create_qa_artifact(qa_scores: dict, artifact_type=None):
    """
    Create a markdown artifact with QA scores.
    """

    if artifact_type == "table":

        qa_table = []

        for key in qa_scores.keys():
            qa_table.append({"measure": key, "result" : qa_scores[key]})

        create_table_artifact(
            key="qa-report",
            table=qa_table,
            description="QA Report",
        )

    if artifact_type == "image":
        image = qa_scores["url"]

        create_image_artifact(
            image_url = image,
            description = "qa-report"
            )

    else:
        # just fall back to the original behavior
        qa_markdown = "# QA Scores:"
        for key, value in qa_scores.items():
            qa_markdown += f"\n- {key}: {value}"

        create_markdown_artifact(
            key="qa-report",
            markdown=qa_markdown,
            description="QA Report",
        )



def randomly_fail() -> bool:  # TODO: Possibly maybe make this a decorator instead?
    """
    Randomly return True or False.
    Intended to test the ability to handle failures.
    As is, this is an unrealistically high failure rate.
    """
    return random.choice([True, False])


def sleep_placeholder():
    """
    Sleep for 3 seconds. Intended to represent
    a quick task runtime.
    """
    time.sleep(3)


@task(tags=["util"])
def update_context(input_dict: dict):
    """
    Save the current state of the pipeline.
    """
    sleep_placeholder()


@task(tags=["util"])
def load_context():
    """
    Load the saved state of the pipeline.
    """
    sleep_placeholder()


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


@task(tags=["flagging"])
def apply_online_flags(data):
    sleep_placeholder()


@task(tags=["io"])
def get_antpos_info(data):
    sleep_placeholder()


@task(tags=["heuristics"])
def create_antpos_table(antenna_position_corrections, data):
    sleep_placeholder()
    return fake_data((100,100))


@task(tags=["calibration"])
def apply_antpos(table, data):
    sleep_placeholder()
    return fake_data((1000, 1000))


@flow(log_prints=True)
def calibrator_data_import_and_prep(calibrator):
    """
    Import and prepare calibration data.
    """
    logger = get_run_logger()
    logger.info(f"Starting calibrator data import and prep for {calibrator}")

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

    logger.info("Updating context and creating QA artifact")
    update_context(result)
    create_qa_artifact(fake_qa_score('data_import_and_prep'))


# Bandpass Solve
@task(tags=["flagging"])
def autoflag_bandpass(bp_data):
    sleep_placeholder()


@task(tags=["heuristics"])
def calmod():
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
def bandpass_solve(bpcal):
    logger = get_run_logger()
    logger.info(f"Starting bandpass solve for {bpcal}")

    load_context()
    autoflag_bandpass(bpcal)
    calmod()
    save_model_vis(bpcal)
    amp_phase_solve(bpcal)
    qa = bandpass_qa_score(bpcal)
    update_context(qa)
    logger.info(f"Bandpass QA Scores: {qa['bandpass_qa_score']}")
    create_qa_artifact(qa)


# Time Gain Solve
@task(tags=["heuristics"])
def calc_snr(gaincal):
    sleep_placeholder()


@task(tags=["calibration"])
def per_spw_gain_soln(gaincal):
    sleep_placeholder()


@task(tags=["calibration"])
def best_spw_gain_soln(gaincal):
    sleep_placeholder()


@task(tags=["calibration"])
def combinespw_gain_soln(gaincal):
    sleep_placeholder()


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
def all_spws_high_snr(spws: List[int]) -> bool:
    """
    Return True if all spws have high SNR.
    """
    return random.choice([True, False])


@task(tags=["heuristics"])
def any_spw_high_snr(spws: List[int]) -> bool:
    """
    Return True if any spw has high SNR.
    """
    return random.choice([True, False])


@flow(log_prints=True)
def time_gain_solve(gaincal):
    logger = get_run_logger()
    logger.info(f"Starting time gain solve for {gaincal}")

    load_context()
    spws = [0, 1, 2, 3]  # pretend these come from the load_context() call
    calc_snr(gaincal)
    if all_spws_high_snr(spws):
        per_spw_gain_soln(gaincal)
    elif any_spw_high_snr(spws):
        best_spw_gain_soln(gaincal)
    else:
        combinespw_gain_soln()
    global_gain_soln(gaincal)
    qa = gaincal_qa_score(gaincal)
    update_context(qa)
    logger.info(f"Gaincal QA Scores: {qa['gaincal_qa_score']}")
    create_qa_artifact(qa)


# Image Calibrators
@task(tags=["calibration"])
def apply_cal(calibrator):
    sleep_placeholder()


@task(tags=["imaging"])
def image_continuum(calibrator):
    sleep_placeholder()


@task(retries=3, tags=["io"])
def export_continuum_images_to_archive(calibrator):
    sleep_placeholder()
    if randomly_fail():
        raise Exception("Export of calibrator images to the Archive failed.")


@flow(log_prints=True)
def image_calibrator(calibrator):
    logger = get_run_logger()
    logger.info(f"Imaging {calibrator}")
    load_context()
    apply_cal(calibrator)
    image_continuum(calibrator)
    result = export_continuum_images_to_archive(calibrator)
    logger.info(f"Exported continuum images to archive: {result}")
    update_context(result)
    create_qa_artifact(fake_qa_score('imaging_qa_score'))

# Target Data Import and Prep

# Calibrate Target and Find Continuum

# Cube Imaging

# Continuum Imaging with Self-Calibration

# Per-SPW Continuum Imaging


if __name__ == "__main__":
    calibration_pipeline_example()
