import os
import random

import pandas as pd
from datetime import datetime
from flyteidl.core.artifact_id_pb2 import Granularity
from typing_extensions import Annotated
from unionai.artifacts import OnArtifact, ModelCard

from flytekit import ImageSpec, current_context
from flytekit import LaunchPlan
from flytekit.core.artifact import Artifact, Inputs
from flytekit.core.task import task
from flytekit.core.workflow import workflow
from flytekit.types.file import FlyteFile


# Helper function for artifact card
def generate_md_contents(df: pd.DataFrame) -> str:
    contents = "# Dataset Card\n" "\n" "## Tabular Data\n"
    contents = contents + df.to_markdown()
    return contents


image = ImageSpec(
    builder="fast-builder",
    name="artifacts-example-image",
    requirements="requirements.txt",
    registry=os.environ.get("DOCKER_REGISTRY", None),
)

# Define an artifact for workflow A
ArtifactA = Artifact(
    name="artifact_a",
)


# Attach ArtifactA to the output of task_a
@task(container_image=image)
def task_a() -> Annotated[pd.DataFrame, ArtifactA()]:
    # Create random dataframe
    random_values = [random.random() for _ in range(3)]
    df = pd.DataFrame({"col": random_values})
    # Create an artifact card
    return ArtifactA.create_from(
        df,
        ModelCard(generate_md_contents(df)),
    )


@workflow
def workflow_a() -> pd.DataFrame:
    return task_a()


# Define an artifact for workflow B
ArtifactB = Artifact(
    name="artifact_b",
)


# Attach ArtifactB to the output of task_b
@task(container_image=image)
def task_b(my_data: pd.DataFrame) -> Annotated[str, ArtifactB]:
    return f"My data is {my_data.head()}"


@workflow
def workflow_b(my_data: pd.DataFrame = ArtifactA.query()) -> str:
    return task_b(my_data=my_data)


# Create reactive workflow by defining a trigger
on_artifact_a_for_b = OnArtifact(
    trigger_on=ArtifactA,
)

# Launch plan triggers workflow_b on the creation of ArtifactA
workflow_b_lp = LaunchPlan.get_or_create(
    name="workflow_b_lp",
    workflow=workflow_b,
    trigger=on_artifact_a_for_b
)

# Define an artifact for workflow B with a time partition
ArtifactC = Artifact(
    name="artifact_c",
    time_partitioned=True,
    time_partition_granularity=Granularity.HOUR,
)


@task(container_image=image)
def task_c(my_data: pd.DataFrame) -> FlyteFile:
    # Create arbitrary file
    p = os.path.join(current_context().working_directory, "data.txt")
    f = open(p, mode="w")
    f.write(f"The shape of the data is: {my_data.shape}.")
    f.close()
    return FlyteFile(p)


# Attach ArtifactC to the output of workflow_c and add an ArtifactA query as input to workflow_c
@workflow
def workflow_c(current_datetime: datetime, my_data: pd.DataFrame = ArtifactA.query()) -> Annotated[
    FlyteFile, ArtifactC(time_partition=Inputs.current_datetime)]:
    return task_c(my_data=my_data)


# Create reactive workflow by defining a trigger with inputs
on_artifact_a_for_c = OnArtifact(
    trigger_on=ArtifactA,
    inputs={"current_datetime": datetime.now(), "my_data": ArtifactA}
)

# Launch plan triggers workflow_c on the creation of ArtifactA
workflow_c_lp = LaunchPlan.get_or_create(
    name="workflow_c_lp",
    workflow=workflow_c,
    trigger=on_artifact_a_for_c
)


@task(container_image=image)
def task_d(current_datetime: datetime, my_file: FlyteFile, my_data: pd.DataFrame):
    # Print inputs
    dt_string = current_datetime.strftime("%m/%d/%Y, %H:%M:%S")
    print(f"The time is {dt_string}")
    print(f"My file is at {my_file.path}")
    print(f"My data is {my_data.head()}")


# ArtifactA and ArtifactC queries are inputs to workflow_d
@workflow
def workflow_d(current_datetime: datetime, my_file: FlyteFile = ArtifactC.query(),
               my_data: pd.DataFrame = ArtifactA.query()):
    task_d(current_datetime=current_datetime, my_file=my_file, my_data=my_data)


# Create reactive workflow by defining a trigger on ArtifactC with inputs of ArtifactC and ArtifactA
on_artifact_c = OnArtifact(
    trigger_on=ArtifactC,
    inputs={"current_datetime": ArtifactC.time_partition, "my_file": ArtifactC.query(), "my_data": ArtifactA.query()}
)

# Launch plan triggers workflow_d on the creation of ArtifactC
workflow_d_lp = LaunchPlan.get_or_create(
    name="workflow_d_lp",
    workflow=workflow_d,
    trigger=on_artifact_c
)
