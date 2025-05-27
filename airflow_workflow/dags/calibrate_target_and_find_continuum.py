from airflow.sdk import dag, task
from datetime import datetime
import time


@dag(
 dag_id="calibrate_target_and_find_continuum",
    schedule=None,
    start_date=datetime(2025,1,1),
    catchup=False,
)
def calibrate_target_and_find_continuum():
    """
    DAG to perform per-spw continuum imaging
    """

    @task
    def flag_target_data():
        time.sleep(1.0)
        return "flagged_target_data"

    @task
    def apply_caltables():
        time.sleep(1.0)
        return "data_with_caltables_applied"

    @task
    def make_qa_score():
        time.sleep(1.0)
        # Arbitrary hard-coded QA score for now
        return 1.0

    @task
    def make_dirty_image():
        time.sleep(1.0)
        return "dirty_image"

    @task
    def find_continuum():
        time.sleep(1.0)
        return "continuum_image"

    flag_target_data() >> apply_caltables() >> make_qa_score() >> make_dirty_image() >> find_continuum()


calibrate_target_and_find_continuum()
