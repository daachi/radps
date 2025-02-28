from prefect import flow, task, pause_flow_run
from prefect.logging import get_run_logger
from prefect.events import emit_event
from core import (fake_data, sleep_placeholder, randomly_fail, create_qa_artifact, fake_qa_score,
                  qa_failure_condition, load_context, add_to_context)
from stage_image_cont_selfcal import find_data_context, solve


# Bandpass Solution
@task(tags=["flagging"])
def autoflag_bandpass(bp_data):
    sleep_placeholder()
    return bp_data


@task(retries=4, tags=["io"])
def query_calmod(bandpass_calibrator, failures=False):
    if randomly_fail(on=failures):
        raise Exception("Query calmod failed")
    else:
        return fake_data((100, 100))


@task(tags=["heuristics"])
def calmod(bandpass_calibrator):
    sleep_placeholder()


@task(tags=["imaging"])
def save_model_vis(bp_data):
    sleep_placeholder()
    return bp_data


@task(tags=["calibration"])
def amp_phase_solve(bp_data, src):
    return solve(bp_data['datashape'], src=src, combine='scan')


@task(tags=["qa"])
def bandpass_qa_score(bp_data, bpcal) -> dict:
    """
    Calculate a QA score for the bandpass solution.
    """
    sleep_placeholder()
    qa_name = f'bandpass_qa_score_{bpcal}'
    return qa_name, fake_qa_score(qa_name)


@flow(log_prints=True)
def bandpass_solve(bpcal_name, bpcal=None, failures=False):
    """
    Do the bandpass solution
    """
    logger = get_run_logger()
    logger.info(f"Starting bandpass solve for {bpcal_name}")

    context = load_context()
    print("staritng context as of bandpass  solve")
    print(context)

    try:
        bpcal = find_data_context(context, stage="stage_data_import_and_prep", context_key='data')
    except:
        data = {bpcal_name:{'n_field':1, 'n_spw':3, 'n_scan':1},
                'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
                'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1} }
        bpcal = {}
        bpcal['datashape'] = {}
        bpcal['datashape'][bpcal_name] = dict(data[bpcal_name])

    logger.info(f"Flagging bandpass data for {bpcal_name}")
    flagged_bandpass = autoflag_bandpass(bpcal)

    logger.info(f"Querying calmod for {bpcal_name}")
    query_calmod(bpcal, failures=failures)

    logger.info(f"Calmod for {bpcal_name}")
    calmod(bpcal)

    logger.info(f"Saving model vis for {bpcal_name}")
    flagged_bandpass_saved_model = save_model_vis(flagged_bandpass)

    logger.info(f"Calculating bandpass solution for {bpcal_name}")
    bandpass_solution = amp_phase_solve(flagged_bandpass_saved_model, bpcal_name)
    qa_name, qa_score = bandpass_qa_score(bandpass_solution, bpcal_name)

    add_to_context(bandpass_solution, key="caltable", stage="bandpass")
    logger.info("Updating context and creating QA artifact")
    new_context = add_to_context(qa_score, key="qa", stage="bandpass")

    print("context after bandpass solve")
    print(new_context)

    create_qa_artifact(qa_score)
    logger.info(f"Bandpass QA Scores: {qa_score}")

    if qa_failure_condition(qa_score[qa_name], failures_on=failures):
        emit_event(event="low_qa.bandpass.event!", resource={"prefect.resource.id": "test.id"})
        pause_flow_run()


if __name__ == "__main__":
    bpcal_name = "bpcal"

    data = {bpcal_name:{'n_field':1, 'n_spw':3, 'n_scan':1},
            'gcal':{'n_field':1, 'n_spw':3, 'n_scan':4},
            'target':{'n_field':1, 'n_spw':3, 'n_scan':5, 'n_chan':1} }
    bpcal = {}
    bpcal['datashape'] = {}
    bpcal['datashape'][bpcal_name] = dict(data[bpcal_name])

    bandpass_solve(bpcal_name, bpcal)
