from airflow.sdk import Asset, dag, task
import os, json, time, random
from datetime import datetime

""" 
Three DAGs describes import data, calibrate data, and imag data stages'
Each DAG updates the same 'context' json file, called processing_data.json
but calibrate and imaging DAGs sequentially triggered by specific updated info
in the processing_data.json file.

There is a check previous stage in image_data_dag (check_calibration_stage) 
but this may not be robust for more complex workflow. May be triggering from 
the parent DAG using TriggerDagRunOperator or similar operator may be more robust.
"""
METADATA = Asset("processing_metadata.json")
datadir = "/tmp/airflow_test_data"
@dag(schedule=None, 
     catchup=False, 
     start_date=datetime(2025,1,1), 
     tags=['import-data'],
     max_active_runs=1,)
def import_data_2_dag():

    @task(outlets=[METADATA])
    def import_data_task():
        """ 
        Prentend importing data by generating an initial metadata JSON file.
        """
        metadata = {"raw_data":{"source_1":{"spw": [0,1,2,3], "scan": [0,1,2,3]}}}
        
        os.makedirs(datadir, exist_ok=True)

        if os.path.exists(f"{datadir}/processing_data.json"):
            os.remove(f"{datadir}/processing_data.json")

        with open(f"{datadir}/processing_data.json", "w") as f:
            json.dump(metadata, f, indent=4)
        print("Import data and save metadata to processing_metadata.json")
        return metadata

    import_data_task()


data_import = import_data_2_dag()


@dag(schedule=[METADATA], 
     catchup=False, 
     start_date=datetime(2025,1,1), 
     tags=['calibrate-data'],
    # default_args={'retries':2, 'retry_delay':2}
    )
def calibrate_data_2_dag():

    @task(outlets=[METADATA])
   # def calibrate_data_task(**context):
    def calibrate_data_task():
        """
        Calibrate the data imported from the import_data_dag.
    
        """
        try: 
            # loading the metadata for raw_data
            with open(f"{datadir}/processing_data.json", "r") as f:
                data = json.load(f)
            print('raw_data:', data)

            if 'raw_data' not in data:
                raise ValueError("Raw data not found in metadata. Run import_data_dag first.")
            
            print("Calibrate data ...")
            time.sleep(2.0) 
            # update metadata by adding calibration stage info
            # create random failure 
            passorfail = random.choice(['Pass', 'Fail'])
            if passorfail=='Pass':
                data.update({'calibration_stage': {'status':passorfail,
                            'calibrated_data': data['raw_data']}})
            else:
                data.update({'calibration_stage': {'status':passorfail}})

            with open(f"{datadir}/processing_data.json", "w") as f:
                json.dump(data, f, indent=4)
            print("Calibration completed and metadata updated.")
            return data
        except FileNotFoundError:
            print("Processing metadata file not found. Run import_data_dag first.")
            raise 

    calibrate_data_task()

calibrate_data = calibrate_data_2_dag()


@dag(schedule=[METADATA], 
     catchup=False, 
     start_date=datetime(2025,1,1), 
     tags=['image-data'], 
     max_active_runs=1,
    # default_args={'retries':2, 'retry_delay':2}
    )
def image_data_2_dag():
    """
    Imaging with calibrated data 
    """
    @task
    def check_calibration_stage():
        """
        Check if the calibration stage is complete before proceeding with imaging.
        """
        # define timeout
        timeout = 300 # 5 min
        interval = 10 # 10 sec
        start_time = time.time()
        
        # check the completion of the calibration stage repeatedly 
        while time.time() - start_time < timeout:

            try:
                with open(f"{datadir}/processing_data.json", "r") as f:
                    loaded_data = json.load(f)

                if 'calibration_stage' in loaded_data and loaded_data['calibration_stage']['status'] == 'Pass':
                    calibrated_data = loaded_data['calibration_stage']
                    print("Found calibrated data")
                    return calibrated_data
                elif 'calibration_stage' in loaded_data and loaded_data['calibration_stage']['status'] == 'Fail':
                    raise ValueError("Calibration stage failed.")    
                else:
                    print("Waiting for calibration stage to complete...")
                    time.sleep(interval)
                     
            except FileNotFoundError:
                print("Processing metadata file not found. Run import_data_dag first.")
                raise
        raise TimeoutError(f"Calibration stage did not complete within the timeout period, {timeout}.")
    
    
    @task(outlets=[METADATA])
    def image_data_task():
        try:
            # loading the metadata for calibrated_data
            with open(f"{datadir}/processing_data.json", "r") as f:
                calibrated_data = json.load(f)

            print('calibrated_data:', calibrated_data)
            if 'calibration_stage' not in calibrated_data:
                raise ValueError("Calibration stage not found in metadata. Run calibrate_data_dag first.")
                
            print("Image data ...")
            time.sleep(2.0) 
            # update metadata by adding imaging stage info
            calibrated_data.update({'imaging_stage': {'image_data': 'source1_image'}})
            with open(f"{datadir}/processing_data.json", "w") as f:
                json.dump(calibrated_data, f, indent=4)
            print("Imaging completed and metadata updated.")

            return {'image_data': 'source1_image'}
        except FileNotFoundError:
            print("Processing metadata file not found. Run import_data_dag first.")
            raise

    check_calibration = check_calibration_stage()
    image_data_stage = image_data_task()

    check_calibration >> image_data_stage


image_data = image_data_2_dag()