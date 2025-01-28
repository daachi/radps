from prefect import flow, task
from typing import List

import time
import random

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

# Calibrator Data Import and Prep

# TODO: if we keep the 'tags' thing really need to define tags for each rather than copy-paste strings


# NOTE: Since nested flows cannot be cancelled without cancelling the
# parent flow, in the future we should consider using separate
# deployments for each step in the pipeline as recommended in
# https://docs.prefect.io/v3/develop/write-flows
@flow(log_prints=True)
def calibration_pipeline_example():
    """
    Example calibration pipeline implementation in Prefect from Figure 1 of "An Example RADPS Workflow Decomposition"
    """
    bpcal = "bpcal"  # TODO: use more realistic data
    gaincal = "gaincal"  # TODO: use more realistic data
    calibrators = [bpcal, gaincal]
    for calibrator in calibrators:
        calibrator_data_import_and_prep(calibrator)  # TODO: do this in parallel eventually, but cannot as a flow
    bandpass_solve(bpcal)
    time_gain_solve(gaincal)
    for calibrator in calibrators:  # TODO: do this in parallel eventually, but cannot as a flow
        image_calibrator(calibrator)


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


@task(log_prints=True, tags=["util"])
def update_context(input_dict: dict):
    """
    Save the current state of the pipeline.
    """
    sleep_placeholder()


@task(log_prints=True, tags=["util"])
def load_context():
    """
    Load the saved state of the pipeline.
    """
    sleep_placeholder()


@task(log_prints=True, retries=3, tags=["io"])
def import_data_from_archive(data):
    if randomly_fail():
        raise Exception("Import failed")
    else:
        sleep_placeholder()


@task(log_prints=True, tags=["flagging"])
def apply_online_flags(data):
    sleep_placeholder()


@task(log_prints=True, tags=["heuristics"])
def calc_antpos(data):
    sleep_placeholder()


@task(log_prints=True, tags=["calibration"])
def apply_antpos(data):
    sleep_placeholder()


@flow(log_prints=True)
def calibrator_data_import_and_prep(calibrator):
    import_data_from_archive(calibrator)
    apply_online_flags(calibrator)
    calc_antpos(calibrator)
    apply_antpos(calibrator)
    inp = {}
    update_context(inp)


# Bandpass Solve
@task(log_prints=True, tags=["flagging"])
def autoflag_bandpass(bp_data):
    sleep_placeholder()


@task(log_prints=True, tags=["heuristics"])
def calmod():
    sleep_placeholder()


@task(log_prints=True, tags=["imaging"])
def save_model_vis(bp_data):
    sleep_placeholder()


@task(log_prints=True, tags=["calibration"])
def amp_phase_solve(bp_data):
    sleep_placeholder()


@task(log_prints=True, tags=["qa"])
def bp_qa_score(bp_data):
    sleep_placeholder()


@flow(log_prints=True)
def bandpass_solve(bpcal):
    load_context()
    autoflag_bandpass(bpcal)
    calmod()
    save_model_vis(bpcal)
    amp_phase_solve(bpcal)
    bp_qa_score(bpcal)
    inp = {}
    update_context(inp)


# Time Gain Solve
@task(log_prints=True, tags=["heuristics"])
def calc_snr(gaincal):
    sleep_placeholder()


@task(log_prints=True, tags=["calibration"])
def per_spw_gain_soln(gaincal):
    sleep_placeholder()


@task(log_prints=True, tags=["calibration"])
def best_spw_gain_soln(gaincal):
    sleep_placeholder()


@task(log_prints=True, tags=["calibration"])
def combinespw_gain_soln(gaincal):
    sleep_placeholder()


@task(log_prints=True, tags=["calibration"])
def global_gain_soln(gaincal):
    sleep_placeholder()


@task(log_prints=True, tags=["qa"])
def gaincal_qa_score(gaincal):
    sleep_placeholder()


@task(log_prints=True, tags=["heuristics"])
def all_spws_high_snr(spws: List[int]) -> bool:
    """
    Return True if all spws have high SNR.
    """
    return random.choice([True, False])


@task(log_prints=True, tags=["heuristics"])
def any_spw_high_snr(spws: List[int]) -> bool:
    """
    Return True if any spw has high SNR.
    """
    return random.choice([True, False])


@flow(log_prints=True)
def time_gain_solve(gaincal):
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
    gaincal_qa_score(gaincal)
    inp = {}
    update_context(inp)


# Image Calibrators
@task(log_prints=True, tags=["calibration"])
def apply_cal(calibrator):
    sleep_placeholder()


@task(log_prints=True, tags=["imaging"])
def image_continuum(calibrator):
    sleep_placeholder()


@task(log_prints=True, retries=3, tags=["io"])
def export_continuum_images_to_archive(calibrator):
    if randomly_fail():
        raise Exception("Export of calibrator images to the Archive failed.")
    else:
        sleep_placeholder()


@flow(log_prints=True)
def image_calibrator(calibrator):
    load_context()
    apply_cal(calibrator)
    image_continuum(calibrator)
    export_continuum_images_to_archive(calibrator)
    inp = {}
    update_context(inp)

# Target Data Import and Prep

# Calibrate Target and Find Continuum

# Cube Imaging

# Continuum Imaging with Self-Calibration

# Per-SPW Continuum Imaging


if __name__ == "__main__":
    calibration_pipeline_example()
