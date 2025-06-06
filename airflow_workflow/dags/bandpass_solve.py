import datetime
import pendulum
import random
import time

from airflow.exceptions import AirflowException
from airflow.sdk import dag, task

@dag(
    dag_id="bandpass_solve",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 5, 15, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def bandpass_solve():

    @task
    def autoflag_bandpass_calc_src():
        time.sleep(1.0)
        return True

    @task
    def query_calmodel(retries=3):
        time.sleep(1.0)
        if random.choice([True, False]):
            raise AirflowException("Simulated query_calmodel failure for testing")

        return True

    @task
    @task(trigger_rule="none_failed_min_one_success")
    def predict_and_save_model_visibilities():
        time.sleep(1.0)
        return True

    @task
    def phase_only_solution_per_time():
        time.sleep(1.0)
        return True

    @task
    def amp_and_phase_solution_across_scans():
        time.sleep(1.0)
        return True

    @task(trigger_rule="all_failed")
    def use_backup_model(**context):
        # runs if query calmodel() fails
        print("Pretending to rolling back to a non-existent previous checkpoint")

    task1 = autoflag_bandpass_calc_src()
    task2 = query_calmodel()
    task3 = predict_and_save_model_visibilities()
    task4 = use_backup_model()
    task5 = phase_only_solution_per_time()
    task6 = amp_and_phase_solution_across_scans()

    task1 >> task2 >> [task3, task4]
    task4 >> task3 >> task5 >> task6


dag = bandpass_solve()
