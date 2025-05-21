import datetime
import pendulum
import time

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
    def query_calmodel():
        time.sleep(1.0)
        return True

    @task
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

    autoflag_bandpass_calc_src() >> query_calmodel() >> predict_and_save_model_visibilities() >> phase_only_solution_per_time() >> amp_and_phase_solution_across_scans()
    
dag = bandpass_solve()
