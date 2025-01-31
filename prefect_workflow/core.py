import time
import random
import pickle
import numpy as np

from prefect.artifacts import (
    create_markdown_artifact,
    create_table_artifact,
    create_image_artifact
    )


# NOTE: Re-use between stages
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


# NOTE: Reused betwen calibration pipeline stages, but not elsewhere as data formats are different.
def fake_data(dimensions: tuple) -> dict:
    """
    Create fake data
    """
    data = np.random.rand(*dimensions)
    return data


# NOTE: Definitely reusable acorss stages
def fake_qa_score(name: str = None, **kwargs) -> dict:
    """
    Create a fake QA score withith a random value.
    """
    score = random.random()
    if name:
        return {name: score}
    else:
        return {'qa_score': score}


# NOTE: Reusable across stages
def create_qa_artifact(qa_scores: dict, artifact_type=None):
    """
    Create a markdown artifact with QA scores.
    """

    if artifact_type == "table":

        qa_table = []

        for key in qa_scores.keys():
            qa_table.append({"measure": key, "result" : qa_scores[key]})

        create_table_artifact(
            key="qa-report",
            table=qa_table,
            description="QA Report",
        )

    if artifact_type == "image":
        image = qa_scores["url"]

        create_image_artifact(
            image_url = image,
            description = "qa-report"
            )

    else:
        # just fall back to the original behavior
        qa_markdown = "# QA Scores:"
        for key, value in qa_scores.items():
            qa_markdown += f"\n- {key}: {value}"

        create_markdown_artifact(
            key="qa-report",
            markdown=qa_markdown,
            description="QA Report",
        )


# NOTE: Reusable across stages
def randomly_fail() -> bool:
    """
    Randomly return True or False.
    Intended to test the ability to handle failures.
    As is, this is an unrealistically high failure rate.
    """
#    return random.choice([True, False])
    return False


# NOTE: could be reused across stages if desired
def sleep_placeholder():
    """
    Sleep for 3 seconds. Intended to represent
    a quick task runtime.
    """
    time.sleep(3)
