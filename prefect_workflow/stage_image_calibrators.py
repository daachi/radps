from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.events import emit_event

from core import sleep_placeholder, randomly_fail, create_qa_artifact, Context, fake_qa_score

# Image Calibrators
@task(tags=["calibration"])
def apply_cal(calibrator):
    sleep_placeholder()


# NOTE: stage-specific, but could use solver from Tak's work
@task(tags=["imaging"])
def image_continuum(calibrator):
    sleep_placeholder()


# NOTE: should be merged with code to export target cube images to the archive
@task(retries=3, tags=["io"])
def export_continuum_images_to_archive(calibrator):
    sleep_placeholder()
    if randomly_fail():
        raise Exception("Export of calibrator images to the Archive failed.")


@flow(log_prints=True)
# NOTE: stage-specific
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

    if qa_score['imaging_qa_score'] < 0.67:
        emit_event(event="low_qa.imaging.event!", resource={"prefect.resource.id": "test.id"})

    create_qa_artifact(qa_score)
    context.update(qa_score)
    context.save()
