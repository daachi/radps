import logging

from airflow.decorators import dag, task
from datetime import datetime
from pipeline_context import load_pipeline_context, save_pipeline_context, create_pipeline_operations, init_pipeline_db


@dag(dag_id="importdata", start_date=datetime(2023,1,1), schedule=None, catchup=False, tags=["skeleton"])
def importdata_dag():
    @task
    def setup_database():
        """
        Initialize the database tables
        """
        result = init_pipeline_db()
        logging.info(f"Database setup result: {result}")
        return "database_ready"

    @task
    def import_data_archive(pipeline_context, **airflow_context):
        """
        Import data from the archive
        """
        # Get run_id from DAG config or generate one
        run_id = airflow_context['dag_run'].run_id
        print(f"Processing run_id: {run_id}")

        get_context, save_state = create_pipeline_operations()

        context = get_context(run_id)
        logging.info(f"Existing context: {context}")

        # Save some test state
        test_state = {"status": "imported", "files": 100}
        save_state(run_id, 'import_data', test_state)
        logging.info(f"Saved test state: {test_state}")

        # Read it back to verify it worked
        updated_context = get_context(run_id)
        logging.info(f"Updated context after save: {updated_context}")
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
    db_setup = setup_database()
    pipeline_context = load_pipeline_context()
    data = import_data_archive(pipeline_context)
    flagged_data = apply_flags(data)
    caltable = antpos_correction(flagged_data)
    final_result = apply_caltable(flagged_data, caltable)
    save_context = save_pipeline_context("import data and prep")

    db_setup >> pipeline_context  # Database must be set up before import
    pipeline_context >> data >> flagged_data >> caltable >> final_result  >> save_context # Data flow


importdata_dag()
print("DAG file parsed.")
