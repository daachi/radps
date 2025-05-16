import datetime
import pendulum
import time

from airflow.sdk import dag, task

@dag(
    dag_id="example_workflow",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 5, 15, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def example_workflow():

    @task
    def calibrator_data_import_and_prep():
        time.sleep(1.0)
        return True

    @task
    def bandpass_solve():
        time.sleep(1.0)
        return True

    @task
    def time_gain_solve():
        time.sleep(1.0)
        return True

    @task
    def image_calibrators():
        time.sleep(1.0)
        return True

    @task
    def target_data_import_and_prep():
        time.sleep(1.0)
        return True

    @task
    def calibrate_target_and_find_continuum():
        time.sleep(1.0)
        return True

    @task
    def cube_imaging():
        time.sleep(1.0)
        return True

    @task
    def continuum_imaging_with_selfcal():
        time.sleep(1.0)
        return True

    @task
    def per_spw_continuum_imaging():
        time.sleep(1.0)
        return True

    calibrator_data_import_and_prep() >> bandpass_solve() >> time_gain_solve()
    [time_gain_solve(), target_data_import_and_prep()] >> calibrate_target_and_find_continuum()
    calibrate_target_and_find_continuum() >> [cube_imaging(), continuum_imaging_with_selfcal()]
    continuum_imaging_with_selfcal() >> per_spw_continuum_imaging()
    
dag = example_workflow()
