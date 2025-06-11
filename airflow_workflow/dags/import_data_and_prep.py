from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id="importdata", start_date=datetime(2023,1,1), schedule=None, catchup=False, tags=["skeleton"])
def importdata_dag():
    @task
    def import_data_archive():
        """
        Import data from the archive
        """
        archive_data = "data"
        return archive_data

    @task
    def apply_flags(data):
        """
        Apply online flags to data
        """
        return data

    @task
    def antpos_correction(data):
        """
        Create and return a caltable for antenna position correction
        """
        caltable = data + "_caltable"
        return caltable

    @task
    def apply_caltable(data, caltable):
        """
        Apply the caltable to the data
        """
        return f"{data}_with_{caltable}_applied"

    # Create the pipeline
    data = import_data_archive()
    flagged_data = apply_flags(data)
    caltable = antpos_correction(flagged_data)
    final_result = apply_caltable(flagged_data, caltable)

importdata_dag()
print("DAG file parsed.")