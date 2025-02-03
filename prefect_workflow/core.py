import time
import random
import pickle
import numpy as np

from prefect.artifacts import (
    create_markdown_artifact,
    create_table_artifact,
    create_image_artifact
    )


class Context:
    path = "context.pkl"

    def __init__(self):
        self.qa_scores = {}

    def update(self, qa_score):
        for key, value in qa_score.items():
            self.qa_scores[key] = value

    def save(self, filename='context.pkl'):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    @classmethod
    def load(cls, filename=path):
        with open(filename, 'rb') as f:
            return pickle.load(f)


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

    if artifact_type == "image":
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


def randomly_fail() -> bool:
    """
    Randomly return True or False.
    Intended to test the ability to handle failures.
    As is, this is an unrealistically high failure rate.
    """
    return random.choice([True, False])


def sleep_placeholder(duration: float = 3.0):
    """
    Sleep for some seconds. Intended to represent
    a task runtime. Defaults to 3 seconds.
    """
    time.sleep(duration)
