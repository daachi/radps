import datetime
import pendulum
import time

from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="example_workflow",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 5, 15, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def example_workflow():

    calibrator_data = TriggerDagRunOperator(
        task_id="stage_calibrator_data_import_and_prep",
        trigger_dag_id="zimportdata"
        )

    bandpass_model_out = TriggerDagRunOperator(
        task_id="stage_bandpass_solve",
        trigger_dag_id="bandpass_solve"
        )

    time_gain_solve_out = TriggerDagRunOperator(
        task_id="stage_time_gain_solve",
        trigger_dag_id="time_gain_solve"
        )

    @task
    def image_calibrators():
        time.sleep(1.0)
        return True

    target_data = TriggerDagRunOperator(
        task_id="stage_target_data_import_and_prep",
        trigger_dag_id="zimportdata"
        )

    @task
    def calibrate_target_and_find_continuum():
        time.sleep(1.0)
        return True

    cube_imaging_out = TriggerDagRunOperator(
        task_id="stage_cube_imaging",
        trigger_dag_id="cube_imaging"
        )

    @task
    def continuum_imaging_with_selfcal():
        time.sleep(1.0)
        return True

    @task
    def per_spw_continuum_imaging():
        time.sleep(1.0)
        return True

    calibrator_data >> bandpass_model_out >> time_gain_solve_out
    [time_gain_solve_out, target_data] >> calibrate_target_and_find_continuum()
    calibrate_target_and_find_continuum() >> [cube_imaging_out, continuum_imaging_with_selfcal()]
    continuum_imaging_with_selfcal() >> per_spw_continuum_imaging()
    
dag = example_workflow()
