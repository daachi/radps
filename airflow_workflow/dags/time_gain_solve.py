import datetime
import pendulum
import time
import random

from airflow.sdk import dag, task
from airflow.models.taskinstance import TaskInstance

from pipeline_context import load_pipeline_context, save_pipeline_context


@dag(
    dag_id="time_gain_solve",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 5, 15, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def time_gain_solve():

    @task
    def query_calmodel():
        time.sleep(1.0)
        return True

    @task.branch
    def SNR_heuristic(task_instance: TaskInstance):
        """Logic to determine which branch to take in the stage DAG"""
        if (pendulum.datetime(2024, 10, 1, tz="UTC") > task_instance.start_date > pendulum.datetime(2023, 10, 1, tz="UTC")):
            # e.g., fixed heuristic for a given telescope observing cycle
            return "context_specific_gain_soln"
        elif random.choice([True, False]):
            return "per_spw_gain_soln"
        elif random.choice([True, False]):
            return "best_spw_gain_soln"
        else:
            return "combine_spw_gain_soln"

    @task
    def context_specific_gain_soln():
        time.sleep(1.0)
        return True

    @task
    def per_spw_gain_soln():
        time.sleep(3.0)
        return True

    @task
    def best_spw_gain_soln():
        time.sleep(1.0)
        return True

    @task
    def combine_spw_gain_soln():
        time.sleep(1.0)
        return True
    
    @task(trigger_rule="all_done")
    def global_gain_soln():
        time.sleep(1.0)
        return True

    @task(trigger_rule="all_done")
    def make_qa_score():
        time.sleep(1.0)
        return True

    pipeline_context = load_pipeline_context()
    pipeline_context >> query_calmodel() >> SNR_heuristic() >> [context_specific_gain_soln(), per_spw_gain_soln(), best_spw_gain_soln(), combine_spw_gain_soln()] >> global_gain_soln() >> make_qa_score() >> save_pipeline_context("time_gain_solve")

dag = time_gain_solve()
