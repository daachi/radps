from prefect import flow, task, pause_flow_run
from prefect.logging import get_run_logger
from prefect.events import emit_event
from stage_image_cont_selfcal import solve, find_data_context
from core import (sleep_placeholder, randomly_fail, create_qa_artifact, fake_qa_score,
                  qa_failure_condition, generate_random_complex_array, add_to_context, load_context)

# Image Calibrators
@task(tags=["calibration"])
def apply_cal(calibrator):
    sleep_placeholder()
    return calibrator


@task(tags=["imaging"])
def image_continuum(calibrator, calibrator_name):
    sleep_placeholder()
    return solve(calibrator, calibrator_name, combine='scan', soltype='imaging')



@task(tags=["io"])
def export_spw_to_archive(spw_image):
    sleep_placeholder(0.1)


# NOTE: should be merged with code to export target cube images to the archive
@task(retries=4, tags=["io"])
def export_continuum_images_to_archive(calibrator, failures=False):
    sleep_placeholder()
    export_results = []
    for i in calibrator:
        export_results.append(export_spw_to_archive.submit(i))
    if randomly_fail(on=failures):
        raise Exception("Export of calibrator images to the Archive failed.")
    return export_results


@flow(log_prints=True)
def image_calibrator(calibrator, failures=False):
    logger = get_run_logger()
    logger.info(f"Imaging {calibrator}")

    context = load_context()
    print("Context before imaging calibrator:")
    print(context)

    try:
        calibrator_data = find_data_context(context, stage="calibrator_data_import_and_prep", context_key='datashape')
        print(f"Using calibrator from context {calibrator_data[calibrator]}")
    except (KeyError, OSError) as e:
        print("{} not found in context. Error: {}".format(calibrator, repr(e)))
        print("Using backup default value.")

        calibrator_data = {calibrator:{'n_field':1, 'n_spw':3, 'n_scan':1},
                'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
                'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1} }

    calibrated_vis = apply_cal(calibrator_data)

    logger.info("Imaging calibrator: {calibrator}")
    images = image_continuum(calibrated_vis, calibrator)

    new_context = add_to_context({f"{calibrator}": images}, key="data", stage="calibrator_imaging")

    logger.info(f"Exporting continuum images to archive: {images}")
    result = export_continuum_images_to_archive(images, failures=failures)

    qa_score = fake_qa_score('imaging_qa_score', result=result)

    logger.info("Updating context and creating QA artifact")
    create_qa_artifact(qa_score)
    new_context = add_to_context(qa_score, key="qa", stage="calibrator_imaging")

    print("Context after imaging calibrator")
    print(new_context)

    if qa_failure_condition(qa_score['imaging_qa_score'], failures_on=failures):
        emit_event(event="low_qa.imaging.event!", resource={"prefect.resource.id": "test.id"})
        pause_flow_run()


if __name__ == "__main__":
    calibrators = ["J1752-2956", "J1851+0035"]

    for calibrator in calibrators:
        image_calibrator(calibrator)
