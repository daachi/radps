import datetime
import logging
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
        logging.info("autoflag_bandpass_calc_src running...")
        time.sleep(1.0)
        return True

    @task(retries=1, retry_delay=datetime.timedelta(seconds=10))
    def query_calmodel(**context):
        logging.info("query_calmodel running...")
        config = context['dag_run'].conf or {}
        simulate_failures = config.get('simulate_failures', True)
        failure_rate = config.get('failure_rate', 0.50)

        logging.info("simulate_failures: %s", simulate_failures)
        logging.info("failure_rate: %s", failure_rate)

        time.sleep(1.0)
        if simulate_failures and random.random() < failure_rate:
            logging.info("Simulated query_calmodel failure for testing")
            raise AirflowException("Simulated query_calmodel failure for testing")
        return True

    @task(trigger_rule="one_success")
    def predict_and_save_model_visibilities():
        logging.info("predict_and_save_model_visibilities running...")
        time.sleep(1.0)
        return True

    @task(trigger_rule="one_success")
    def phase_only_solution_per_time():
        logging.info("phase_only_solution_per_time running...")
        time.sleep(1.0)
        return True

    @task(trigger_rule="one_success")
    def amp_and_phase_solution_across_scans():
        logging.info("amp_and_phase_solution_across_scans running...")
        time.sleep(1.0)
        return True

    @task(trigger_rule="one_failed")
    def use_backup_model(**context):
        # runs if query calmodel() fails
        logging.info("Pretending to use a backup model since the query failed")

    autoflag_task = autoflag_bandpass_calc_src()
    query_task = query_calmodel()
    save_model_vis_task = predict_and_save_model_visibilities()
    backup_model_task = use_backup_model()
    phase_soln_task = phase_only_solution_per_time()
    amp_and_phase_soln_task = amp_and_phase_solution_across_scans()

    autoflag_task >> query_task
    query_task >> [save_model_vis_task, backup_model_task]
    backup_model_task >> save_model_vis_task
    save_model_vis_task >> phase_soln_task >> amp_and_phase_soln_task

dag = bandpass_solve()
