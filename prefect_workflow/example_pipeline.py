import time
import dask.array as da
import xarray as xr
from prefect import flow, task

# Implemetation of the example pipeline from Figure 1
# of "An Example RADPS Workflow Decomposition"

# Calibrator Data Import and Prep

# Bandpass Solve

# Time Gain Solve

# Image Calibrators

# Target Data Import and Prep
@task
def fake_archive_query() -> dict:
    print("Pretending to fetch some data from an archive")
    time.sleep(da.random.randint(low=1, high=10, size=1))
    print("Now that that latency simulation is complete, generating some mock data to return")
    rng = da.random.default_rng()
    target_vals = rng.standard_normal(size=(100,100,2,4))
    calibrator_vals = rng.standard_normal(size=(100,100,10,4))

    fake_result = {
        "data" : {
            "source_0" : calibrator_vals,
            "source_1" : target_vals,
        },
        "data_source" : "archive",
        "url" : "https://almascience.nrao.edu/aq/",
        }
    print("All done!")

    return fake_result

@task
def fake_flagging(fake_result) -> dict:
    print("Performing a dummy sub-selection step")
    time.sleep(2)
    transformed_data = fake_result
    transformed_data["data"].pop("source_1")

    return transformed_data

@flow
def extract_transform_load() -> dict:
    print("Calling archive query task")
    fake_data = fake_archive_query()
    print("Finished retrieving some object from that task function")

    print("Calling flagging task")
    transformed_data = fake_flagging(fake_data)
    print("Finished modifying some object from that task function")

    return transformed_data

# Calibrate Target and Find Continuum

# Cube Imaging

# Continuum Imaging with Self-Calibration

# Per-SPW Continuum Imaging

if __name__ == "__main__":

    target_data = extract_transform_load()
