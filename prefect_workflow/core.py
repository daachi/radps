import time
import random
import pickle
import numpy as np
import json
import dask.array as da

from copy import deepcopy
from datetime import datetime
from prefect import flow, task
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, JSON, DateTime, func
from prefect_sqlalchemy import SqlAlchemyConnector, ConnectionComponents, SyncDriver


from prefect.artifacts import (
    create_markdown_artifact,
    create_table_artifact,
    create_image_artifact,
)


@task(log_prints=True)
def create_context():
    """Create and return context"""
    try:
        connector = SqlAlchemyConnector(
            connection_info=ConnectionComponents(
                driver=SyncDriver.SQLITE_PYSQLITE,
                database="context.db"
            )
        )
        connector.save("sqlite-block-context")
    except:
        print("Block already exists. Skipping creation.")

    try:
        with SqlAlchemyConnector.load("sqlite-block-context") as connector:
            connector.execute(
                "CREATE TABLE IF NOT EXISTS context (id INTEGER PRIMARY KEY AUTOINCREMENT, stage varchar, key varchar, data json);"
            )
    except Exception as e:
        print(f"Error creating tables: {e}")

    data = {}

    data["context"] = "context.db"
    print(f"Create context results: {data}")
    return data


@task(log_prints=True)
def load_context() -> dict:
    """Load and return context"""
    context = {}
    with SqlAlchemyConnector.load("sqlite-block-context") as connector:
        new_rows = connector.fetch_many("SELECT * FROM context", size=50)
        for row in new_rows:
            stage, key, data = row[1], row[2], json.loads(row[3])

            if stage in context:
                if key in context[stage]:
                    if isinstance(context[stage][key], dict):
                        context[stage][key].update(data)
                    else:
                        now = datetime.now()
                        datetime_string = now.strftime("%Y%m%d%H%M%S")
                        new_key = f"{key}_{datetime_string}"
                        context[stage][new_key] = data
                else:
                    context[stage][key] = data
            else:
                context[stage] = {}
                context[stage][key] = data

    context["context"] = "context.db"
    print(f"Load context results: {context}")
    return context


def generate_random_complex_array(shape):
    """
    Generate a random complex dask array of the given shape.
    """
    rng = da.random.default_rng()
    re_array = rng.standard_normal(size=shape)
    im_array = rng.standard_normal(size=shape)

    return re_array + 1j * im_array


def fake_data_generator(datashape, type):
    """
    Generate fake data based on the desired datashape
    e.g.
    datashape = {'n_field':1, 'n_spw':3, 'n_scan':5,
    'n_chan':10, 'n_ant':10, 'imsize':256, 'n_time':1

    Omitted keys will be set to default values (mostly 1) except
    for imsize which is set to 512, and nant, which is set to 2 for type="vis".
    """
    nfield = nspw = nscan = nchan = nant = npol = ntime = 1
    imsize = 512

    if "n_field" in datashape:
        nfield = datashape["n_field"]
    if "n_spw" in datashape:
        nspw = datashape["n_spw"]
    if "n_scan" in datashape:
        nscan = datashape["n_scan"]
    if "n_chan" in datashape:
        nchan = datashape["n_chan"]
    if "n_ant" in datashape:
        nant = datashape["n_ant"]
    if "n_pol" in datashape:
        npol = datashape["n_pol"]
    if "imsize" in datashape:
        imsize = datashape["imsize"]
    # time averaging for vis, caltables < n_scan
    if "n_time" in datashape:
        ntime = datashape["n_time"]
    else:
        ntime = nscan

    if type == "vis":

        # avoid setting nbaseline to 0 causing IndexError: Index 0 is out of bounds for axis 0 with size 0
        nant += 1
        nbaseline = nant * (nant - 1) / 2

        data = generate_random_complex_array((nbaseline, ntime, nfield, nspw, nchan))

    elif type == "image":

        rng = da.random.default_rng()
        data = rng.standard_normal(size=(imsize, imsize, nchan, npol))

    elif type == "bcal":

        data = generate_random_complex_array((nfield, nant, nspw, nchan))

    elif type == "gcal":
        data = generate_random_complex_array((ntime, nfield, nant, nspw))

    return data


@task(log_prints=True)
def add_to_context(inp: dict, key="data", stage="unknown_stage") -> dict:
    """
    Stores information in context.
    Will be stored under the provided key.
    Returns the full current context dict
    """
    print(f"Input to add to context: {inp}")
    with SqlAlchemyConnector.load("sqlite-block-context") as connector:
        connector.execute(
            "INSERT INTO context (stage, key, data) VALUES (:stage, :key, :data);",
            parameters={"stage": stage, "key": key, "data": json.dumps(inp)},
        )
    return load_context()

    # context_object = Context.load()

    # if stage in context_object.data:
    #     if key in context_object.data[stage]:
    #         if isinstance(context_object.data[stage][key], dict):
    #             context_object.data[stage][key].update(inp)
    #         else:
    #             now = datetime.now()
    #             datetime_string = now.strftime("%Y%m%d%H%M%S")
    #             new_key = f"{key}_{datetime_string}"
    #             context_object.data[stage][new_key] = inp
    #     else:
    #         context_object.data[stage][key] = inp
    # else:
    #     context_object.data[stage] = {}
    #     context_object.data[stage][key] = inp

    # context_object.save()
    # sleep_placeholder(1.0)
    # return context_object.to_dict()


def fake_qa_score(name: str = None, **kwargs) -> dict:
    """
    Create a fake QA score withith a random value.
    """
    score = random.random()
    if name:
        return {name: score}
    else:
        return {"qa_score": score}


def create_qa_artifact(qa_scores: dict, artifact_type=None):
    """
    Create a markdown artifact with QA scores.
    """

    if artifact_type == "table":

        qa_table = []

        for key in qa_scores.keys():
            qa_table.append({"measure": key, "result": qa_scores[key]})

        create_table_artifact(
            key="qa-report",
            table=qa_table,
            description="QA Report",
        )

    elif artifact_type == "image":
        image = qa_scores["url"]

        create_image_artifact(image_url=image, description="qa-report")

    else:
        # just fall back to the original behavior
        qa_markdown = "# QA Scores:"
        for key, value in qa_scores.items():
            qa_markdown += f"\n- {key}: {value:0.2f}"

        create_markdown_artifact(
            key="qa-report",
            markdown=qa_markdown,
            description="QA Report",
        )


def qa_failure_condition(
    qa_score: float, threshold: float = 0.67, failures_on=False
) -> bool:
    """
    Check if the QA score is below the threshold.
    """
    if failures_on:
        return qa_score < threshold
    else:
        return False


def randomly_fail(on=False) -> bool:
    """
    Randomly return True or False.
    Intended to test the ability to handle failures.
    As is, this is an unrealistically high failure rate.
    """
    if on:
        return random.choice([True, False])
    else:
        return False


def sleep_placeholder(t_max: float = 3.0, t_min: float = 0.001) -> float:
    """
    Take up some time waiting.
    Intended to represent a task runtime.
    """

    duration = random.uniform(t_min, t_max)
    time.sleep(duration)
    return duration


def data_placeholder(duration: float = 3.0):
    """
    Take up some time computing with random numbers using dask arrays.
    Intended to represent a task runtime.
    """
    something = da.random.random((2000, 2000), chunks=(500, 500))
    broadcast = something * duration
    result = da.cov(broadcast)
    try:
        # submit requests to the dask scheduler as found in default config
        result.compute()
    except ValueError:
        # might happen if using ephemeral schedulers rather than connecting via k8s
        result.compute(scheduler="threads")
    return result
