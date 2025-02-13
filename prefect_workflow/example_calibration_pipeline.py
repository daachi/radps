import asyncio

from prefect import flow, exceptions
from prefect.deployments import run_deployment

# Imports for stages
from stage_calibrator_import_and_prep import calibrator_data_import_and_prep, run_calibrator_import_and_prep_in_parallel
from stage_bandpass_solve import bandpass_solve
from stage_time_gain_solve import time_gain_solve
from stage_image_calibrators import image_calibrator

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

# Calibrator Data Import and Prep

# TODO: if we keep the 'tags' thing, define tags for each rather than copy-paste strings


# NOTE: Since nested flows cannot be cancelled without cancelling the
# parent flow, in the future we should consider using separate
# deployments for each step in the pipeline as recommended in
# https://docs.prefect.io/v3/develop/write-flows
@flow(log_prints=True)
def calibration_pipeline_example(failures=False):
    """
    Example calibration pipeline implementation in Prefect from Figure 1 of "An Example RADPS Workflow Decomposition"
    """
    # Calibrator Data Import and Prep
    calibrators = ["J1752-2956", "J1851+0035"]

    try:
        imported_calibrators = asyncio.run(
            run_calibrator_import_and_prep_in_parallel(calibrators, failures=failures))
    except exceptions.ObjectNotFound:
        print("Failed to run deployment for calibrator import. Running in serial.")
        imported_calibrators = []
        for calibrator in calibrators:
            imported_calibrators.append(calibrator_data_import_and_prep(calibrator, failures=failures))

    # Bandpass Solve
    bandpass_solve(calibrators[0], imported_calibrators[0], failures=failures)

    # Time Gain Solve
    time_gain_solve(imported_calibrators[1], failures=failures)

    # Image Calibrators
    for source in calibrators:
        try:
            run_deployment(
                name="image-calibrator/image calibrator and export to archive",
                parameters={"calibrator": source, "failures": failures},
                timeout=0
                )
        except exceptions.ObjectNotFound:
            print(f"Failed to run deployment for calibrator {calibrator}. Running serially.")
            image_calibrator(source, failures=failures)


if __name__ == "__main__":
    calibration_pipeline_example(failures=True)
