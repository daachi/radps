import time
import random
import pickle
import numpy as np

from copy import deepcopy
from datetime import datetime
from prefect import flow, task

from prefect.artifacts import (
    create_markdown_artifact,
    create_table_artifact,
    create_image_artifact
    )


class Context:
    path = "context.pkl"

    def __init__(self):
        self.data = {}
        self.save()

    def save(self, filename='context.pkl'):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    def to_dict(self) -> dict:
        """Creates and returns a dict representation of the context."""
        data = deepcopy(self.data)
        data["context"] = self.path
        return data

    @classmethod
    def load(cls, filename=path):
        """Loads a context from a file."""
        try:
            with open(filename, 'rb') as f:
                return pickle.load(f)
        except FileNotFoundError:
            print("Context file not found. Creating new context.")
            return cls()
        except (EOFError, pickle.UnpicklingError):
            print("File read error. Creating new context.")
            return cls()
        except Exception as e:
            print(f"Error loading context: {e}. Creating new context")
            return cls()


@task(log_prints=True)
def create_context():
    """Create and return context"""
    context = Context()
    return context.to_dict()


@task(log_prints=True)
def load_context() -> dict:
    """Load and return context"""
    context_object = Context.load()
    context_dict = context_object.to_dict()
    return context_dict


@task(log_prints=True)
def add_to_context(inp: dict, stage="unknown_stage", key="data") -> dict:
    """
    Stores information in context.
    Will be stored under the provided key.
    Returns the full current context dict
    """
    context_object = Context.load()

    if stage in context_object.data:
        if key in context_object.data[stage]:
            if isinstance(context_object.data[stage][key], dict):
                context_object.data[stage][key].update(inp)
            else:
                now = datetime.now()
                datetime_string = now.strftime("%Y%m%d%H%M%S")
                new_key = f"{key}_{datetime_string}"
                context_object.data[stage][new_key] = inp
        else:
            context_object.data[stage][key] = inp
    else:
        context_object.data[stage] = {}
        context_object.data[stage][key] = inp

    context_object.save()
    sleep_placeholder(1.0)
    return context_object.to_dict()


def fake_data(dimensions: tuple) -> dict:
    """
    Create fake data
    """
    data = np.random.rand(*dimensions)
    return data


def fake_qa_score(name: str = None, **kwargs) -> dict:
    """
    Create a fake QA score withith a random value.
    """
    score = random.random()
    if name:
        return {name: score}
    else:
        return {'qa_score': score}


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

        create_image_artifact(
            image_url=image,
            description="qa-report"
            )

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


def qa_failure_condition(qa_score: float, threshold: float = 0.67, failures_on=False) -> bool:
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


def sleep_placeholder(duration: float = 3.0):
    """
    Sleep for some seconds. Intended to represent
    a task runtime. Defaults to 3 seconds.
    """
    time.sleep(duration)
