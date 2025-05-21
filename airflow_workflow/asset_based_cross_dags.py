from airflow.sdk import asset, Asset
from airflow.datasets import Dataset


@asset(schedule=None)
def import_data():
    return {"source_1":{"spw": [0,1,2,3], "scan": [0,1,2,3]}}

@asset(schedule=import_data)
def calibrate_data(context):

    uncalibrated_data = context["ti"].xcom_pull(
        dag_id="import_data",
        task_ids="import_data",
        key="return_value",
        include_prior_dates=True,
    )
    # do calibration
    print(f"calibrating data for {list(uncalibrated_data.keys())[0]}")
    return {"calibrated_data": uncalibrated_data}


@asset(schedule=calibrate_data)
def image_data(context):

    calibrated_data = context["task_instance"].xcom_pull(
        dag_id="calibrate_data",
        task_ids="calibrate_data",
        key="return_value",
        include_prior_dates=True,
    )
    
    print(f"image data for {list(calibrated_data["calibrated_data"].keys())[0]}")
    return {'image_data': 'source1_image'}
