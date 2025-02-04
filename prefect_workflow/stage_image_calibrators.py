from prefect import flow, task, pause_flow_run
from prefect.logging import get_run_logger
from prefect.events import emit_event

from core import (sleep_placeholder, randomly_fail, create_qa_artifact, Context, fake_qa_score,
                  qa_failure_condition, fake_data)

# Image Calibrators
@task(tags=["calibration"])
def apply_cal(calibrator):
    sleep_placeholder()


# NOTE: stage-specific, but could use solver from Tak's work
@task(tags=["imaging"])
def image_continuum(calibrator):
    sleep_placeholder()
    return fake_data((100, 100))


@task(tags=["io"])
def export_spw_to_archive(calibrator):
    sleep_placeholder(0.1)


# NOTE: should be merged with code to export target cube images to the archive
@task(retries=4, tags=["io"])
def export_continuum_images_to_archive(calibrator):
    sleep_placeholder()
    export_results = []
    for i in calibrator[0]:
        export_results.append(export_spw_to_archive.submit(i))
    if randomly_fail():
        raise Exception("Export of calibrator images to the Archive failed.")
    return export_results


@flow(log_prints=True)
def image_calibrator(calibrator):
    logger = get_run_logger()
    logger.info(f"Imaging {calibrator}")

    context = Context.load()
    calibrated_vis = apply_cal(calibrator)

    logger.info("Imaging calibrator: {calibrator}")
    images = image_continuum(calibrated_vis)

    logger.info(f"Exporting continuum images to archive: {images}")
    result = export_continuum_images_to_archive(images)

    qa_score = fake_qa_score('imaging_qa_score', result=result)

    logger.info("Updating context and creating QA artifact")
    create_qa_artifact(qa_score)
    context.update(qa_score)
    context.save()

    if qa_failure_condition(qa_score['imaging_qa_score']):
        emit_event(event="low_qa.imaging.event!", resource={"prefect.resource.id": "test.id"})
        pause_flow_run()


if __name__ == "__main__":
    calibrators = ["J1752-2956", "J1851+0035"]

    for calibrator in calibrators:
        image_calibrator(calibrator)
