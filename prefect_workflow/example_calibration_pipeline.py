from prefect import flow, task

from prefect.artifacts import create_markdown_artifact
from prefect.deployments import run_deployment
from prefect.events import emit_event
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
import pickle

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

# Calibrator Data Import and Prep

# TODO: if we keep the 'tags' thing, define tags for each rather than copy-paste strings


class Context:
    path = "context.pkl"

    def __init__(self):
        self.qa_scores = {}

    def update(self, qa_score):
        for key, value in qa_score.items():
            self.qa_scores[key] = value

    def save(self, filename='context.pkl'):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, filename=path):
        with open(filename, 'rb') as f:
            return pickle.load(f)

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
#        run_deployment(
#            name="calibrator-data-import-and-prep/import data and prep",
#            parameters={"calibrator": calibrator},
#            job_variables={"env": {"MY_ENV_VAR": "staging"}},
#            timeout=0
#            )
        calibrator_data_import_and_prep(calibrator)  # TODO: do this in parallel eventually, but cannot as a flow
    bandpass_solve(bpcal)
    time_gain_solve(gaincal)
    for calibrator in calibrators:  # TODO: do this in parallel eventually, but cannot as a flow
        image_calibrator(calibrator)
    context = Context.load()
    create_qa_artifact(context.qa_scores)


def fake_data(dimensions: tuple) -> dict:
    """
    Create fake data
    """
    data = np.random.rand(*dimensions)
    return data


def fake_qa_score(name: str = None, **kwargs) -> dict:
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



def randomly_fail() -> bool:
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
    return data


@task(tags=["io"])
def get_antpos_info(data):
    sleep_placeholder()
    return fake_data((100, 100))


@task(tags=["heuristics"])
def create_antpos_table(antenna_position_corrections, data):
    sleep_placeholder()
    return fake_data((100, 100))


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


# Bandpass Solution
@task(tags=["flagging"])
def autoflag_bandpass(bp_data):
    sleep_placeholder()


@task(retries=3, tags=["io"])
def query_calmod(bandpass_calibrator):
    if randomly_fail():
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
def time_gain_solve(gaincal):
    logger = get_run_logger()
    logger.info(f"Starting time gain solve for {gaincal}")

    context = Context.load()
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

    if qa['gaincal_qa_score'] < 0.67:
        emit_event(event="low_qa.gaincal.event!", resource={"prefect.resource.id": "test.id"})

    logger.info(f"Gaincal QA Scores: {qa['gaincal_qa_score']}")
    create_qa_artifact(qa)

    context.update(qa)
    context.save()


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

    context = Context.load()
    calibrated_vis = apply_cal(calibrator)

    logger.info("Imaging calibrator: {calibrator}")
    images = image_continuum(calibrated_vis)

    logger.info(f"Exporting continuum images to archive: {images}")
    result = export_continuum_images_to_archive(images)

    qa_score = fake_qa_score('imaging_qa_score', result=result)

    if qa_score['imaging_qa_score'] < 0.67:
        emit_event(event="low_qa.imaging.event!", resource={"prefect.resource.id": "test.id"})

    create_qa_artifact(qa_score)
    context.update(qa_score)
    context.save()

# Target Data Import and Prep

# Calibrate Target and Find Continuum

# Cube Imaging

# Continuum Imaging with Self-Calibration

# Per-SPW Continuum Imaging


if __name__ == "__main__":
    #calibrator_data_import_and_prep.serve(  # Flow to deploy
    #    name="import data and prep",  # Name of the deployment
    #)
    calibration_pipeline_example()
