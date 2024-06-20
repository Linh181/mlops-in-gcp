from typing import Iterable, Optional

from google.cloud import aiplatform_v1

from . import expr
from .model import Artifact, Pipeline, PipelineRun


class VertexMlMetadataClient:
    """A more intuitive client for the Vertex ML Metadata Store."""

    def __init__(self, project_id: str, region: str, metadata_store: str = "default"):
        self._project_id = project_id
        self._region = region
        self._metadata_store = metadata_store

    def get_client(self) -> aiplatform_v1.MetadataServiceClient:
        """Gets the underlying Google Metadata service client for performing 'raw' queries."""
        return aiplatform_v1.MetadataServiceClient(
            client_options={"api_endpoint": f"{self._region}-aiplatform.googleapis.com"}
        )

    def list_pipelines(self) -> Iterable[Pipeline]:
        """Lists pipelines in the given project and region."""
        request = aiplatform_v1.ListContextsRequest(
            parent=self._metastore_context(),
            filter=str(expr.schema_title("system.Pipeline")),
        )
        yield from map(Pipeline.from_record, self.get_client().list_contexts(request))

    def list_pipeline_runs(self, pipeline_name: str) -> Iterable[PipelineRun]:
        """Lists runs for the given pipeline."""
        request = aiplatform_v1.ListContextsRequest(
            parent=self._metastore_context(),
            filter=str(
                expr.has_parent_context(self._pipeline_context(pipeline_name))
                & expr.schema_title("system.PipelineRun")
            ),
        )
        yield from map(PipelineRun.from_record, self.get_client().list_contexts(request))

    def list_artifacts(self, filter_expr: Optional[str] = None) -> Iterable[Artifact]:
        """Lists artifacts, optionally filtered by a given filter expression."""
        request = aiplatform_v1.ListArtifactsRequest(
            parent=self._metastore_context(),
            filter=str(filter_expr)  # Coerce to string in case we're given something else
            if filter_expr
            else None,
        )
        yield from map(Artifact.from_record, self.get_client().list_artifacts(request))

    def list_artifacts_for_pipeline(
        self, pipeline_name: str, schema_title: Optional[str] = None
    ) -> Iterable[Artifact]:
        """Lists artifacts for the given pipeline."""
        return self.list_artifacts(
            filter_expr=str(
                _in_context_with_optional_schema(
                    context=self._pipeline_context(pipeline_name),
                    schema_title=schema_title,
                )
            )
        )

    def list_artifacts_for_run(
        self, pipeline_run: str, schema_title: Optional[str] = None
    ) -> Iterable[Artifact]:
        """Lists artifacts for the given pipeline run."""
        return self.list_artifacts(
            filter_expr=str(
                _in_context_with_optional_schema(
                    context=self._run_context(pipeline_run), schema_title=schema_title
                )
            )
        )

    def _metastore_context(self) -> str:
        return expr.metastore_context(
            project_id=self._project_id,
            region=self._region,
            metadata_store=self._metadata_store,
        )

    def _pipeline_context(self, pipeline_name: str) -> str:
        return expr.pipeline_context(
            project_id=self._project_id,
            region=self._region,
            metadata_store=self._metadata_store,
            pipeline_name=pipeline_name,
        )

    def _run_context(self, run_name: str) -> str:
        return expr.run_context(
            project_id=self._project_id,
            region=self._region,
            metadata_store=self._metadata_store,
            run_name=run_name,
        )


def _in_context_with_optional_schema(context: str, schema_title: Optional[str] = None) -> expr.Expr:
    return (
        expr.in_context(context) & expr.schema_title(schema_title)
        if schema_title
        else expr.in_context(context)
    )
