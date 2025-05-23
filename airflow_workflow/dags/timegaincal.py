from airflow.decorators import dag, task
from datetime import datetime
import random


@dag(dag_id="timegain", start_date=datetime(2023,1,1), schedule=None, catchup=False, tags=["skeleton"])
def timegain_dag():
    @task
    def calculate_snr():
        """
        Calculate SNR for gaincal
        """
        return {"snr_level": "high", "spw_count": 4}

    @task.branch(task_id="branching")
    def snr_choice(data):
        """
        All high snr spws
        At least one high snr spw
        No high snr spws
        """
        choices = ["all_high_snr_spws", "at_least_one_high_snr_spw", "no_high_snr_spws"]
        return random.choice(choices)

    @task(task_id="all_high_snr_spws")
    def soln_per_spw(data):
        """
        Do the per-spw solution
        """
        return {"solution": "per_spw", "data": data}

    @task(task_id="at_least_one_high_snr_spw")
    def soln_one_spw(data):
        """
        Do the solution for one spw
        """
        return {"solution": "one_spw", "data": data}

    @task(task_id="no_high_snr_spws")
    def soln_combine_spw(data):
        """
        Do the solution for combined spws
        """
        return {"solution": "combine_spw", "data": data}

    @task(trigger_rule="none_failed_min_one_success")
    def gaincal(**context):
        """
        Do the global gain solution
        """
        # fetch context
        task_instance = context['ti']

        # fetch results from completed task
        upstream_result = task_instance.xcom_pull(task_ids=['all_high_snr_spws', 'at_least_one_high_snr_spw', 'no_high_snr_spws'])

        # fetch the result that actually ran   
        result = next(r for r in upstream_result if r is not None)
        return {"gaincal_result": "completed", "input_data": result}

    @task(trigger_rule="none_failed_min_one_success")
    def qa_score(data):
        """
        Calculate a qa score
        """
        return {"qa_score": 0.95, "input_data": data}

    snr_data = calculate_snr()
    branch_choice = snr_choice(snr_data)

    per_spw_result = soln_per_spw(snr_data)
    one_spw_result = soln_one_spw(snr_data)
    combine_spw_result = soln_combine_spw(snr_data)
    final_gaincal = gaincal()
    final_qa = qa_score(final_gaincal)

    # Dependencies
    snr_data >> branch_choice >> [per_spw_result, one_spw_result, combine_spw_result] >> final_gaincal >> final_qa

timegain_dag()
print("DAG file parsed.")