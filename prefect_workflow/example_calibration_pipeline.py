from prefect import flow

# Imports for stages
from stage_calibrator_import_and_prep import calibrator_data_import_and_prep
from stage_bandpass_solve import bandpass_solve
from stage_time_gain_solve import time_gain_solve
from stage_image_calibrators import image_calibrator
from core import Context, create_qa_artifact

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

# Calibrator Data Import and Prep

# TODO: if we keep the 'tags' thing, define tags for each rather than copy-paste strings


# NOTE: Since nested flows cannot be cancelled without cancelling the
# parent flow, in the future we should consider using separate
# deployments for each step in the pipeline as recommended in
# https://docs.prefect.io/v3/develop/write-flows
#
# NOTE: This will likely be replaced with the new top-level flow, but we could also move it to pipeline.py so we also have the 
# option to run just the imaging or just the calibration pipeline. 
@flow(log_prints=True)
def calibration_pipeline_example():
    """
    Example calibration pipeline implementation in Prefect from Figure 1 of "An Example RADPS Workflow Decomposition"
    """
    bpcal = "bandpass_calibrator_name"
    gaincal = "gain_calibrator_name"
    calibrators = [bpcal, gaincal]
    for calibrator in calibrators:
#        run_deployment(
#            name="calibrator-data-import-and-prep/import data and prep",
#            parameters={"calibrator": calibrator},
#            job_variables={"env": {"MY_ENV_VAR": "staging"}},
#            timeout=0
#            )
        calibrator_data_import_and_prep(calibrator)
    bandpass_solve(bpcal)
    time_gain_solve(gaincal)
    for calibrator in calibrators:  # TODO: do this in parallel eventually, but cannot as a flow
        image_calibrator(calibrator)
    context = Context.load()
    create_qa_artifact(context.qa_scores)


if __name__ == "__main__":
    #calibrator_data_import_and_prep.serve(  # Flow to deploy
    #    name="import data and prep",  # Name of the deployment
    #)
    calibration_pipeline_example()
