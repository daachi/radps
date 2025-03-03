import asyncio

from prefect import flow
from prefect import exceptions
from prefect.deployments import run_deployment

from stage_calibrator_import_and_prep import calibrator_data_import_and_prep, run_calibrator_import_and_prep_in_parallel
from stage_bandpass_solve import bandpass_solve
from stage_time_gain_solve import time_gain_solve
from stage_image_calibrators import image_calibrator
from stage_data_import_and_prep import extract_transform_load
from stage_findcont import calibrate_target_and_find_continuum
from stage_image_cube import image_target_cube
from stage_image_cont_selfcal import image_cont_selfcal
from stage_image_perspw_cont import image_perspw_cont
from stage_image_cont_selfcal import generate_vis_datashape
from resource_management import connect_to_scheduler

# Implementation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

tr = connect_to_scheduler()
@flow(log_prints=True, task_runner=tr)
def pipeline(imaging='cube', interactive=False):
    """
    Example pipeline implementation in Prefect from Figure 1 of "An Example RADPS Workflow Decomposition"
    
    Parameters
      imaging : 'cube', 'cont', or 'contselfcal' 
      interactive : interactive clean (only valid for 'cube' or 'cont')
    """
    if imaging not in ['cube', 'cont', 'contselfcal']:
        raise ValueError("imaging must be 'cube', 'cont', or 'contselfcal'")
    elif imaging == 'contselfcal' and interactive:
        raise ValueError("interactive imaging is only supported for  'cube' or 'cont'")
    
    # Calibrator Data Import and Prep
    calibrators = ["J1752-2956", "J1851+0035"]

    try:
        imported_calibrators = asyncio.run(
            run_calibrator_import_and_prep_in_parallel(calibrators))
    except (exceptions.ObjectNotFound, exceptions.PrefectHTTPStatusError):
        print("Failed to run deployment for calibrator import. Running in serial.")
        imported_calibrators = []
        for calibrator in calibrators:
            imported_calibrators.append(calibrator_data_import_and_prep(calibrator))

    # Bandpass Solve
    bandpass_solve(calibrators[0], imported_calibrators[0])

    # Time Gain Solve
    time_gain_solve(calibrators[1], imported_calibrators[1])

    # Image Calibrators
    for source in calibrators:
        try:
            run_deployment(
                name="image-calibrator/image calibrator and export to archive",
                parameters={"calibrator": source},
                timeout=0
                )
        except exceptions.ObjectNotFound:
            print(f"Failed to run deployment for calibrator {calibrator}. Running in serial.")
            image_calibrator(source)

    # Target Data Import and Prep
    target_data = extract_transform_load("source_1")

    # Calibrate Target and Find Continuum
    dirty_image, findcont = calibrate_target_and_find_continuum(target_data)

    calibrated_target_data={}
    # To avoild failure for no datashape context
    if imaging == 'cube':
        addchan = True
    else:
        addchan = False
    calibrated_target_data = generate_vis_datashape(addchan=addchan)
    
    if imaging == 'cube': 
        # Cube Imaging
        cleaned_target_cube_image = image_target_cube(calibrated_target_data, interactive=interactive)
    elif imaging == 'cont': # no self-calibration
        # Per-SPW Continuum Imaging
        per_spw_target_cont_image = image_perspw_cont(calibrated_target_data, interactive=interactive)
    elif imaging == 'contselfcal':    
        # Continuum Imaging with Self-Calibration
        cleaned_target_cont_image = image_cont_selfcal(calibrated_target_data,doselfcal=True)
        # Per-SPW Continuum Imaging
        per_spw_target_cont_image = image_perspw_cont(calibrated_target_data)

if __name__ == "__main__":

    import sys
    imaging = 'cube'
    interactive = False
        
    if len(sys.argv) == 2:
        imaging = sys.argv[1]
    elif len(sys.argv) == 3:
        interactive = bool(sys.argv[2])
    
    pipeline(imaging, interactive) 

