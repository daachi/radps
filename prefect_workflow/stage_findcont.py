import requests
import dask.array as da
import scipy
from matplotlib.image import imsave
import pathlib
from prefect import flow, task
from prefect.cache_policies import TASK_SOURCE

from core import sleep_placeholder, create_qa_artifact
from stage_data_import_and_prep import (
    extract_transform_load,
    apply_caltable
    )

# Calibrate Target and Find Continuum
@task
def calculate_qa_heuristic() -> dict:
    print(
        "Pretending to perform computation that determines a QA score to return to some pipeline context"
    )

    # contains an iterative solver (sometimes)
    # for now, just calculate some moments on an array of random data
    rng = da.random.default_rng()
    random_array = rng.standard_normal(size=(1000, 1000, 10, 4))

    qa_result = {
        "stage": da.random.randint(low=1, high=42).compute().item(),
    }

    # calculate a few statistical moments
    for order in range(2, 9):
        metric = da.moment(random_array, order=order).compute().item()
        qa_result[f"order{order}"] = metric

    # arbitrarily check the value of the last moment to determine Pass/Fail
    if qa_result[f"order{order}"] > 105:
        result = True
    else:
        result = False

    qa_result["pass"] = result

    print("Calculated a fake QA score. Result:")
    print(qa_result)

    return qa_result


@flow
def make_dirty_cube(fake_visibilities) -> dict:
    print("Pretending to construct a dirty image cube from some target data")
    fake_image = fake_visibilities

    print("Simulating an 'iterative solver' by looping on some axis ")
    for nn in range(0, fake_visibilities["data"]["source_1"].shape[3]):
        fake_image["data"]["source_1"] = da.fft.fft2(
            da.rechunk(fake_visibilities["data"]["source_1"], chunks=-1)
        )
        fake_image["data"]["source_1"].compute()

    dirty_cube = fake_image

    return dirty_cube


@task
def calculate_mean_spectrum(dirty_cube) -> dict:
    print(
        "Pretending to calculate a mean spectrum for determining line-free channels in a dirty image cube"
    )
    sleep_placeholder(2)
    mean_spectrum = dirty_cube["data"]["source_1"].mean(axis=2).compute()

    return mean_spectrum


@task
def peak_over_MAD(mean_spectrum) -> dict:
    print(
        "Pretending to use median absolute deviation method to find line-free channels"
    )
    sleep_placeholder(6)
    continuum_data = {"result": scipy.stats.median_abs_deviation(mean_spectrum)}

    return {}


@flow
def find_continuum(dirty_cube) -> dict:
    print(
        "Pretending to perform some heuristics to identify line-free channels in a dirty cube image"
    )
    spectrum_from_image = calculate_mean_spectrum(dirty_cube)
    continuum_data = peak_over_MAD(spectrum_from_image)

    return continuum_data


@flow
def calibrate_target_and_find_continuum(input_data) -> dict:
    print(
        "Starting a pipeline stage that applies calibration to target and finds line-free continuum"
    )
    dummy_caltable = {"gains": da.random.random_sample(size=(4))}
    calibrated_data = apply_caltable(input_data, dummy_caltable, "source_1")

    stage_result = calculate_qa_heuristic()
    create_qa_artifact(stage_result, artifact_type="table")

    dirty_cube = make_dirty_cube(calibrated_data)

    # just write a slice of our fake image to disk
    imsave("image.png", calibrated_data["data"]["source_1"][:,:,0,0].compute())
    stage_result["url"] = pathlib.Path("image.png").resolve().as_uri()
    create_qa_artifact(stage_result, artifact_type="image")

    continuum_data = find_continuum(dirty_cube)

    return dirty_cube, continuum_data


if __name__ == "__main__":

    # Target Data Import and Prep
    target_data = extract_transform_load("source_1")

    # Calibrate Target and Find Continuum
    dirty_image, findcont = calibrate_target_and_find_continuum(target_data)
