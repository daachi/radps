from airflow.decorators import dag, task
from datetime import datetime


@dag(dag_id="zimportdata", start_date=datetime(2023,1,1), schedule=None, catchup=False, tags=["skeleton"])
def zimportdata_dag():
    @task
    def import_data_archive():
        """
        Import data from the archive
        """
        # pretend to fetch data from an archive
        archive_data = "data"
        return archive_data

    @task
    def apply_flags(data):
        """
        Apply online flags to data
        """
        # Pretend to apply online flags
        return data

    @task
    def antpos_correction(data):
        """
        Create and return a caltable for antenna position correction
        """
        caltable = data + "caltable"
        return caltable

    @task
    def apply_caltable(data, caltable):
        """
        Apply the caltable to the data
        """
        return data

    data = import_data_archive()
    flagged_data = apply_flags(data)
    caltable = antpos_correction(flagged_data)
    apply_caltable(caltable, flagged_data)

zimportdata_dag()
print("DAG file parsed.")
