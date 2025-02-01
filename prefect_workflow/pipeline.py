from stage_calibrator_import_and_prep import calibrator_data_import_and_prep
from stage_bandpass_solve import bandpass_solve
from stage_time_gain_solve import time_gain_solve
from stage_image_calibrators import image_calibrator
from stage_data_import_and_prep import extract_transform_load
from stage_findcont import calibrate_target_and_find_continuum
from stage_image_cube import stage_image_target_cube
from stage_image_cont_selfcal import stage_image_cont_selfcal
from stage_image_perspw_cont import stage_image_perspw_cont

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

if __name__ == "__main__":

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

    # Cube Imaging
    data_object = {
        'bcal':{'n_field':1, 'n_spw':3, 'n_scan':1},
        'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
        'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1}
    }
    stage_image_target_cube(data_object)

    # Continuum Imaging with Self-Calibration
    stage_image_cont_selfcal(data_object, doselfcal=True)

    # Per-SPW Continuum Imaging
    stage_image_perspw_cont(data_object)
