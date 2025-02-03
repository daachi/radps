from prefect import flow

from stage_calibrator_import_and_prep import calibrator_data_import_and_prep
from stage_bandpass_solve import bandpass_solve
from stage_time_gain_solve import time_gain_solve
from stage_image_calibrators import image_calibrator
from stage_data_import_and_prep import extract_transform_load
from stage_findcont import calibrate_target_and_find_continuum
from stage_image_cube import image_target_cube
from stage_image_cont_selfcal import image_cont_selfcal
from stage_image_perspw_cont import image_perspw_cont
from stage_image_cont_selfcal import generate_vis_datashape

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"


@flow(log_prints=True)
def pipeline():
    """
    Example pipeline implementation in Prefect from Figure 1 of "An Example RADPS Workflow Decomposition"
    """
    # Calibrator Data Import and Prep
    calibrators = ["J1752-2956", "J1851+0035"]
    calibrator_data_import_and_prep(calibrators)

    # Bandpass Solve
    bandpass_solve(calibrators[0])

    # Time Gain Solve
    time_gain_solve(calibrators[1])

    # Image Calibrators
    for source in calibrators:
        image_calibrator(source)

    # Target Data Import and Prep
    target_data = extract_transform_load("source_1")

    # Calibrate Target and Find Continuum
    dirty_image, findcont = calibrate_target_and_find_continuum(target_data)

    # some imaging intent flags should be here

    # run all target imaging stages (both cube and continuum)
    #doCubeImaging, doContImaging, doSelfCal = True, True, True
    # Cube imging only
    doCubeImaging, doContImaging, doSelfCal = True, False, False
    # Continuum imaging + selfcal
    #doCubeImaging, doContImaging, doSelfCal = False, True, True

    calibrated_target_data = generate_vis_datashape(addchan=doCubeImaging)
    if doCubeImaging:
        # Cube Imaging
        cleaned_target_cube_image = image_target_cube(calibrated_target_data)

    if doContImaging:
        # Continuum Imaging with Self-Calibration
        cleaned_target_cont_image = image_cont_selfcal(calibrated_target_data,doselfcal=doSelfCal)
        # Per-SPW Continuum Imaging
        per_spw_target_cont_image = image_perspw_cont(calibrated_target_data)

if __name__ == "__main__":
    pipeline()
