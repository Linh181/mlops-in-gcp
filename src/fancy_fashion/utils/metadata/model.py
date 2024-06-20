from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import pandas as pd
from google.cloud import aiplatform_v1

# pylint: disable=too-many-instance-attributes


@dataclass
class Pipeline:
    """Model class representing a Vertex Pipeline."""

    name: str
    display_name: str
    etag: str
    create_time: str
    update_time: str
    schema_title: str
    schema_version: str

    @classmethod
    def from_record(cls, record: aiplatform_v1.types.context.Context) -> "Pipeline":
        """Creates an instance from a Google Context record."""
        return cls(
            name=record.name,
            display_name=record.display_name,
            etag=record.etag,
            create_time=record.create_time,
            update_time=record.update_time,
            schema_title=record.schema_title,
            schema_version=record.schema_version,
        )

    @classmethod
    def from_records(
        cls, records: Iterable[aiplatform_v1.types.context.Context]
    ) -> Iterable["Pipeline"]:
        """Creates an iterable of instances from Google Context records."""
        return (cls.from_record(record) for record in records)


@dataclass
class PipelineRun:
    """Model class representing a Vertex Pipeline run."""

    # Fields provided by Google
    name: str
    display_name: str
    etag: str
    create_time: str
    update_time: str
    parent_contexts: str
    schema_title: str
    schema_version: str
    metadata: dict[str, Any]

    # Custom fields
    pipeline_name: str

    @classmethod
    def from_record(cls, record: aiplatform_v1.types.context.Context) -> "PipelineRun":
        """Creates an instance from a Google Context record."""
        return cls(
            name=record.name,
            display_name=record.display_name,
            etag=record.etag,
            create_time=record.create_time,
            update_time=record.update_time,
            parent_contexts=record.parent_contexts,
            schema_title=record.schema_title,
            schema_version=record.schema_version,
            metadata=dict(record.metadata),
            pipeline_name=str(record.parent_contexts[0]).rsplit("/", 1)[-1],
        )

    @classmethod
    def from_records(
        cls, records: Iterable[aiplatform_v1.types.context.Context]
    ) -> Iterable["PipelineRun"]:
        """Creates an iterable of instances from Google Context records."""
        return (cls.from_record(record) for record in records)


@dataclass
class Artifact:
    """Model class representing a Vertex Artifact."""

    # Fields provided by Google
    name: str
    display_name: str
    uri: str
    etag: str
    create_time: datetime
    update_time: datetime
    state: str
    description: str
    schema_title: str
    schema_version: str
    metadata: dict[str, Any]

    # Custom fields
    pipeline_name: str
    pipeline_run: str

    @classmethod
    def from_record(cls, record: aiplatform_v1.types.artifact.Artifact) -> "Artifact":
        """Creates an instance from a Google Artifact record."""
        project_id = record.name.split("/")[1]
        pipeline_run = record.uri.split(f"/{project_id}/", 1)[1].split("/", 1)[0]
        pipeline_name = "-".join(pipeline_run.split("-")[:-1])

        return cls(
            name=record.name,
            display_name=record.display_name,
            uri=record.uri,
            etag=record.etag,
            create_time=record.create_time,
            update_time=record.update_time,
            state=record.state.name,
            description=record.description,
            schema_title=record.schema_title,
            schema_version=record.schema_version,
            metadata=dict(record.metadata),
            pipeline_name=pipeline_name,
            pipeline_run=pipeline_run,
        )

    @classmethod
    def from_records(
        cls, records: Iterable[aiplatform_v1.types.artifact.Artifact]
    ) -> Iterable["Artifact"]:
        """Creates an iterable of instances from Google Artifact records."""
        return (cls.from_record(record) for record in records)


def to_dataframe(records: Iterable[Any], normalize: bool = False) -> pd.DataFrame:
    """
    Converts an iterable of model instances to a DataFrame,
    optionally normalizing nested fields.
    """
    if normalize:
        return pd.json_normalize(r.__dict__ for r in records)
    return pd.DataFrame(r.__dict__ for r in records)
