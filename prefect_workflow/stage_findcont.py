import requests
import dask.array as da
import scipy
from matplotlib.image import imsave
import pathlib
from prefect import flow, task
from prefect.cache_policies import TASK_SOURCE

from core import (
    sleep_placeholder,
    create_qa_artifact,
    fake_qa_score,
    load_context,
    add_to_context,
)
from stage_data_import_and_prep import extract_transform_load, apply_caltable


# Calibrate Target and Find Continuum
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


@flow(log_prints=True)
def calibrate_target_and_find_continuum(input_data) -> dict:
    findcont_context = load_context()
    print(f"Initial context: {findcont_context}")
    print(
        "Starting a pipeline stage that applies calibration to target and finds line-free continuum"
    )
    dummy_caltable = {"gains": da.random.random_sample(size=(4))}
    calibrated_data = apply_caltable(input_data, dummy_caltable, "source_1")

    dirty_cube = make_dirty_cube(calibrated_data)
    findcont_context = add_to_context({"cube": dirty_cube}, "data", stage="findcont")
    print(f"Updated context: {findcont_context}")

    # do some QA stuff
    complicated_score = {}
    for nn in range(0, 9):
        complicated_score[f"parameter_{nn}"] = fake_qa_score()
    create_qa_artifact(complicated_score, artifact_type="table")
    # write a slice of our fake image to disk
    imsave("image.png", calibrated_data["data"]["source_1"][:, :, 0, 0].compute())

    complicated_score["url"] = pathlib.Path("image.png").resolve().as_uri()
    create_qa_artifact(complicated_score, artifact_type="image")

    findcont_context = add_to_context(dirty_cube, "data", stage="findcont")
    print(f"Updated context again: {findcont_context}")

    continuum_data = find_continuum(dirty_cube)

    findcont_context = add_to_context(
        {"continuum": continuum_data}, "data", stage="findcont"
    )
    print(f"Final context: {findcont_context}")

    return dirty_cube, continuum_data


if __name__ == "__main__":

    # Target Data Import and Prep
    target_data = extract_transform_load("source_1")

    # Calibrate Target and Find Continuum
    dirty_image, findcont = calibrate_target_and_find_continuum(target_data)
