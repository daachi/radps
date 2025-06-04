from airflow.sdk import Asset, dag, task


@dag(schedule=None)
def import_data_dag():

    @task(outlets=[Asset("raw_data")])
    def import_data_task():
        return {"source_1":{"spw": [0,1,2,3], "scan": [0,1,2,3]}}

    import_data_task()


import_data_dag()


@dag(schedule=[Asset("raw_data")])
def calibrate_data_dag():

    @task(outlets=[Asset("calibrated_data")])
    def calibrate_data_task(**context):
        uncalibrated_data = context["ti"].xcom_pull(
            dag_id="import_data_dag",
            task_ids="import_data_task",
            key="return_value",
            include_prior_dates=True,
        )
        # do calibration
        print(f"calibrating data for {list(uncalibrated_data.keys())[0]}")
        return {"calibrated_data": uncalibrated_data}
    


    calibrate_data_task()


calibrate_data_dag()


@dag(schedule=[Asset("calibrated_data")])
def image_data_dag():

    @task
    def image_data_task(**context):
        calibrated_data = context["ti"].xcom_pull(
            dag_id="calibrate_data_dag",
            task_ids="calibrate_data_task",
            key="return_value",
            include_prior_dates=True,
        )
        print(f"image data for {list(calibrated_data["calibrated_data"].keys())[0]}")
        return {'image_data': 'source1_image'}


    image_data_task()


image_data_dag()