import datetime
import pendulum
import time
import random

from airflow.sdk import dag, task
from airflow.models.taskinstance import TaskInstance

from pipeline_context import load_pipeline_context, save_pipeline_context


@dag(
    dag_id="image_calibrators",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2025, 5, 15, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def image_calibrators():

    @task
    def apply_calmodel():
        time.sleep(1.0)
        return True

    @task
    def make_continuum_image():
        time.sleep(5.0)
        return True

    @task(trigger_rule="none_failed")
    def export_calibrator_image():
        time.sleep(1.0)
        return True

    pipeline_context = load_pipeline_context()
    pipeline_context >> apply_calmodel() >> make_continuum_image() >> export_calibrator_image() >> save_pipeline_context("image_calibrators")

dag = image_calibrators()
