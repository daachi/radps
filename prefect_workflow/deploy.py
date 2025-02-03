from prefect import serve

from stage_calibrator_import_and_prep import calibrator_data_import_and_prep
from stage_image_calibrators import image_calibrator

# Deploy the the flows in which the whole stage needs to be run in parallel
if __name__ == "__main__":
    import_and_prep_deploy = calibrator_data_import_and_prep.to_deployment(
        name="import data and prep",
    )

    image_calibrator_deploy = image_calibrator.to_deployment(
        name="image calibrator and export to archive",
    )

    serve(import_and_prep_deploy, image_calibrator_deploy)
