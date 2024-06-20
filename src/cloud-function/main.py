from contextlib import contextmanager
from dataclasses import dataclass
import os
import tempfile
import logging
import json
import sys

from typing import Any, Dict, Tuple, Set

from google.api_core.exceptions import InvalidArgument, Forbidden, NotFound
from google.cloud.aiplatform.pipeline_jobs import PipelineJob
from google.cloud import storage
from yaml.parser import ParserError

# Parse env variables on start-up to make sure they exist.
GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_REGION = os.environ["GCP_REGION"]
GCP_ZONE = os.environ["GCP_ZONE"]

PIPELINE_NAME = os.environ["PIPELINE_NAME"]
PIPELINE_SERVICE_ACCOUNT = os.environ["PIPELINE_SERVICE_ACCOUNT"]

ARTIFACT_BUCKET = os.environ["ARTIFACT_BUCKET"]
INPUT_BUCKET = os.environ["INPUT_BUCKET"]
OUTPUT_BUCKET = os.environ["OUTPUT_BUCKET"]
DATASET = os.environ["DATASET"]

ARTIFACT_PATH = os.environ["ARTIFACT_PATH"]
RUN_PATH = os.environ["RUN_PATH"]


class Response:
    """Generic interface for SuccessResponse/ErrorResponse (see below)."""

    # Should return a response type for Flask, as a tuple consisting
    # of a JSON payload (Dict) and a status code (int).
    def to_flask(self) -> Tuple[Dict[str, Any], int]:
        """Converts the response to a JSON dict + status code for Flask."""
        raise NotImplementedError()


@dataclass
class SuccessResponse(Response):
    """Response for a successful job submission, with details such as the job ID."""

    project_id: str
    region: str
    job_id: str

    # Should return a response type for Flask, as a tuple consisting
    # of a JSON payload (Dict) and a status code (int).
    def to_flask(self) -> Tuple[Dict[str, Any], int]:
        return self.__dict__, 200


@dataclass
class ErrorResponse(Response):
    """Error response format following https://datatracker.ietf.org/doc/html/rfc7807."""

    type: str
    title: str
    detail: str

    # Status code to return
    status_code: int

    def to_flask(self):
        payload = {k: v for k, v in self.__dict__.items() if k != "status_code"}
        return payload, self.status_code


def get_pipeline_parameters(pipeline_path: str) -> Set[str]:
    """Returns parameter names from the given pipeline spec."""
    with open(pipeline_path, encoding="utf-8") as file_:
        pipeline_spec = json.load(file_)

    return set(
        pipeline_spec["pipelineSpec"]["root"]["inputDefinitions"]["parameters"].keys()
    )


class CloudLoggingFormatter(logging.Formatter):
    """Produces messages compatible with google cloud logging"""

    def format(self, record: logging.LogRecord) -> str:
        message = super().format(record)
        return json.dumps(
            {
                "message": message,
                "severity": record.levelname,
                "timestamp": {"seconds": int(record.created), "nanos": 0},
            }
        )


def setup_logging():
    """Sets up logging with the CloudLoggingFormatter."""
    root = logging.getLogger()
    handler = logging.StreamHandler(sys.stdout)
    formatter = CloudLoggingFormatter(fmt="[%(name)s] %(message)s")
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(logging.DEBUG)


@contextmanager
def get_pipeline(model_name: str, model_version: str, pipeline_name: str) -> str:
    """Fetches a pipeline definition and exposes it locally from a temporary file."""

    pipeline_url = f"gs://{ARTIFACT_BUCKET}/models/{model_name}/{model_version}/{pipeline_name}.json"

    client = storage.Client()

    with tempfile.NamedTemporaryFile() as pipeline_file:
        client.download_blob_to_file(pipeline_url, file_obj=pipeline_file)
        pipeline_file.flush()  # ensure content is written to file
        yield pipeline_file.name


def process_request(request) -> Tuple[Dict[str, Any], int]:
    """Handler for incoming requests."""

    logger = logging.getLogger()

    response = None

    # Expects parameters to be pass
    request_data: Dict[str, Any] = request.get_json(force=True)

    try:
        model_name = request_data["model_name"]
        model_version = request_data["model_version"]

        pipeline_name = request_data.get("pipeline_name", "pipeline")
        pipeline_parameters = request_data.get("pipeline_parameters", {})
    except KeyError as exc:
        response = ErrorResponse(
            type="missing_parameter",
            title="Missing required parameter",
            detail=f"Missing required parameter '{exc.args[0]}'",
            status_code=400,
        )

    if not response:  # Functional style Success/Failure objects would be nicer here.
        try:
            with get_pipeline(model_name, model_version, pipeline_name) as pipeline_path:
                # Project-specific parameters from the Cloud Function environment.
                env_parameters = {
                    k: v
                    for k, v in {
                        "project_id": GCP_PROJECT_ID,
                        "region": GCP_REGION,
                        "zone": GCP_ZONE,
                        "artifact_bucket": ARTIFACT_BUCKET,
                        "artifact_path": ARTIFACT_PATH,
                        "input_bucket": INPUT_BUCKET,
                        "output_bucket": OUTPUT_BUCKET,
                    }.items()
                    # Filter project-parameters against the pipeline parameter list
                    # to avoid passing parameters that the pipeline doesn't accept.
                    if k in get_pipeline_parameters(pipeline_path)
                }

                job = PipelineJob(
                    display_name=PIPELINE_NAME,
                    enable_caching=False,
                    template_path=pipeline_path,
                    parameter_values={
                        # Pass along any parameters we get to the function.
                        **pipeline_parameters,
                        # But override any duplicates with our environment config.
                        **env_parameters,
                    },
                    pipeline_root=f"gs://{ARTIFACT_BUCKET}/{RUN_PATH}",
                    location=GCP_REGION,
                )

                job.submit(service_account=PIPELINE_SERVICE_ACCOUNT)
                logging.info(
                    "Successfully submitted pipeline job (project=%s, region=%s, id=%s)",
                    job.project,
                    job.location,
                    job.job_id,
                )
                response = SuccessResponse(
                    project_id=job.project, region=job.location, job_id=job.job_id
                )
        except NotFound as exc:
            response = ErrorResponse(
                type="pipeline_not_found",
                title="The specified pipeline could not be found",
                detail=exc.message,
                status_code=500,
            )
        except Forbidden as exc:
            response = ErrorResponse(
                type="pipeline_access_denied",
                title="Insufficient permissions to fetch the pipeline",
                detail=exc.message,
                status_code=500,
            )
        except (ParserError, KeyError) as exc:
            response = ErrorResponse(
                type="invalid_pipeline",
                title="An error occurred while parsing the pipeline",
                detail=exc.message,
                status_code=500,
            )
        except (InvalidArgument, ValueError) as exc:
            if exc.message.startswith(
                "You do not have permission to act as service_account"
            ):
                response = ErrorResponse(
                    type="invalid_pipeline_service_account",
                    title="An invalid service account was used for running the pipeline",
                    detail=exc.message,
                    status_code=500,
                )
            else:
                response = ErrorResponse(
                    type="invalid_pipeline_parameters",
                    title="Invalid parameters were passed to the pipeline",
                    detail=exc.message,
                    status_code=400,
                )
        except Exception as exc:  # pylint: disable=broad-except
            logger.error(exc, exc_info=True)
            response = ErrorResponse(
                type="unexpected_error",
                title="An unexpected error occurred",
                detail=getattr(exc, "message", str(exc))
                .replace("\n", "\\n")
                .replace("\r", "\\r")[:989],
                status_code=500,
            )

    return response.to_flask()


# Global setup code, run once on function instantiation.
setup_logging()
