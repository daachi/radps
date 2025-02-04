# RADPS
Radio Astronomy Data Processing System

##

To run the demo pipeline in a python environment with the dependencies installed, it is required to start a couple of background processes. First, to have a prefect server running:

`prefect server start &`

and also, to create the deployments required for running the calibration components in parallel:

`python prefect_workflow/deploy &`

The pipeline can then be executed using:

`python prefect_workflow/pipeline.py`
