import time
import requests
import dask.array as da
import scipy
from matplotlib.image import imsave
import pathlib
from prefect import flow, task
from prefect.cache_policies import TASK_SOURCE

from stage_data_import_and_prep import extract_transform_load
from stage_findcont import calibrate_target_and_find_continuum

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

if __name__ == "__main__":

    # Calibrator Data Import and Prep
    calibrator_data = extract_transform_load("source_0")

    # Bandpass Solve

    # Time Gain Solve

    # Image Calibrators

    # Target Data Import and Prep
    target_data = extract_transform_load("source_1")

    # Calibrate Target and Find Continuum
    dirty_image, findcont = calibrate_target_and_find_continuum(target_data)

    # Cube Imaging

    # Continuum Imaging with Self-Calibration

    # Per-SPW Continuum Imaging
