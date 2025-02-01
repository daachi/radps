from stage_data_import_and_prep import extract_transform_load
from stage_findcont import calibrate_target_and_find_continuum
from stage_image_cube import image_target_cube
from stage_image_cont_selfcal import image_cont_selfcal
from stage_image_perspw_cont import image_perspw_cont
from stage_image_cont_selfcal import generate_vis_datashape

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

    # some imaging intent flags should be here
    doCubeImaging = True 
    doContImaging = False
    doSelfcal = False

    calibrated_data = generate_vis_datashape(addchan=True) 
    if doCubeImaging:
        # Cube Imaging
        cleaned_target_cube_image = image_target_cube(calibrated_data)
    if doContImaging:
        if doSelfcal:
            # Continuum Imaging with Self-Calibration
            cleaned_target_cont_image = image_cont_selfcal(calibrated_data)
        # Per-SPW Continuum Imaging
        per_spw_target_cont_image = image_perspw_cont(calibrated_data)
