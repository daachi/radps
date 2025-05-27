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
        trigger_dag_id="importdata"
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
        trigger_dag_id="importdata"
        )

    calibrate_target_and_find_continuum_out = TriggerDagRunOperator(
        task_id="stage_calibrate_target_and_find_continuum",
        trigger_dag_id="calibrate_target_and_find_continuum"
        )

    cube_imaging_out = TriggerDagRunOperator(
        task_id="stage_cube_imaging",
        trigger_dag_id="cube_imaging"
        )

    continuum_imaging_with_selfcal_out = TriggerDagRunOperator(
        task_id="stage_cont_imaging_with_selfcal",
        trigger_dag_id="cont_imaging_with_selfcal"
        )

    per_spw_continuum_imaging_out = TriggerDagRunOperator(
        task_id="stage_per_spw_cont_imaging",
        trigger_dag_id="per_spw_cont_imaging"
    )

    calibrator_data >> bandpass_model_out >> time_gain_solve_out
    [time_gain_solve_out, target_data] >> calibrate_target_and_find_continuum_out
    calibrate_target_and_find_continuum_out >> [cube_imaging_out, continuum_imaging_with_selfcal_out]
    continuum_imaging_with_selfcal_out >> per_spw_continuum_imaging_out

dag = example_workflow()
