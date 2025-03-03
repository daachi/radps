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
    fake_data_generator,
)
from resource_management import connect_to_scheduler
from stage_data_import_and_prep import extract_transform_load, apply_caltable
from stage_image_cube import image_target_cube

tr = connect_to_scheduler()


# Calibrate Target and Find Continuum
@task
def calculate_mean_spectrum(dirty_cube) -> dict:
    print(
        "Pretending to calculate a mean spectrum for determining line-free channels in a dirty image cube"
    )
    print(dirty_cube)
    sleep_placeholder(2)
    mean_spectrum = dirty_cube.mean(axis=2).compute()

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
    print(
        "Starting a pipeline stage that applies calibration to target and finds line-free continuum"
    )
    findcont_context = load_context()
    print(f"Initial context: {findcont_context}")

    print("Generating a calibration table and 'applying' it to the input_data")
    datashape = findcont_context["data_import_and_prep"]["datashape"]
    findcont_context = add_to_context(datashape, key="datashape", stage="findcont")
    gcal_data = fake_data_generator(datashape["calibrator"], "gcal")
    findcont_context = add_to_context({"gcal": gcal_data}, key="data", stage="findcont")

    calibrated_data = apply_caltable(input_data, gcal_data, "source_1")

    print("Calling stage_image_cube.image_target_cube")
    dirty_cube = image_target_cube({"data": da.real(calibrated_data)})
    findcont_context = add_to_context({"cube": dirty_cube}, "data", stage="findcont")

    # do some QA stuff
    complicated_score = {}
    for nn in range(0, 9):
        complicated_score[f"parameter_{nn}"] = fake_qa_score()
    create_qa_artifact(complicated_score, artifact_type="table")
    # write a slice of a fake image to disk
    image_data = fake_data_generator(datashape["calibrator"], "image")
    imsave("image.png", image_data)

    complicated_score["url"] = pathlib.Path("image.png").resolve().as_uri()
    create_qa_artifact(complicated_score, artifact_type="image")

    findcont_context = add_to_context(
        {"calibrator": image_data}, "data", stage="findcont"
    )

    continuum_data = find_continuum(image_data)
    # TODO: update find_continuum to output the dummy data expected by subsequent stages
    # until then, we'll just add it here
    data = {
        "bcal": {"n_field": 1, "n_spw": 3, "n_scan": 1},
        "gcal": {"n_field": 1, "n_spw": 3, "n_scan": 4},
        "target": {"n_field": 1, "n_spw": 3, "n_scan": 5},
    }

    findcont_context = add_to_context(data, "datashape", stage="findcont")
    print(f"Final context: {findcont_context}")

    return dirty_cube, continuum_data


if __name__ == "__main__":

    # Target Data Import and Prep
    target_data = extract_transform_load("source_1")

    # Calibrate Target and Find Continuum
    dirty_image, findcont = calibrate_target_and_find_continuum(target_data)
