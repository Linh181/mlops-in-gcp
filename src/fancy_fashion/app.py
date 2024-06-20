import io
import os
import time
from dotenv import load_dotenv

from urllib.parse import urlparse
from google.cloud import storage

import joblib
import structlog
from structlog.processors import JSONRenderer

from fastapi import FastAPI, UploadFile
from fastapi.responses import JSONResponse, PlainTextResponse

from fancy_fashion.model import generate_prediction, LABEL_MAPPING

load_dotenv()

def _structured_log_formatter(logger, log_method, event_dict):
    event_dict['severity'] = log_method.upper()
    event_dict['message'] = event_dict.pop('event')
    return event_dict

structlog.configure_once(processors=[_structured_log_formatter, structlog.processors.JSONRenderer(sort_keys=True)])
logger = structlog.get_logger()


def _fetch_model(model_uri: str):
    storage_client = storage.Client()

    parsed_url = urlparse(model_uri, allow_fragments=False)
    bucket_name = parsed_url.netloc
    blob_name = parsed_url.path.lstrip("/")

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    with io.BytesIO(blob.download_as_bytes()) as model_file:
        model = joblib.load(model_file)

    return model


app = FastAPI()
logger.info("Running the app")

model = _fetch_model(os.environ["MODEL_URI"])
logger.info("Fetch model from artifacts")


@app.get("/ping", response_class=PlainTextResponse)
def ping():
    """Heartbeat endpoint."""
    return "pong"


@app.post("/predict", response_class=JSONResponse)
def predict(image_data: UploadFile):
    """Predict endpoint, which produces prediction for an uploaded image."""

    raw_image_data = image_data.file.read()
    image = io.BytesIO(raw_image_data)

    confidence = generate_prediction(model, image)[0]

    predicted_category = confidence.argmax(axis=0)
    prediction_confidence = confidence.max(axis=0)

    predicted_label = LABEL_MAPPING[predicted_category]

    logger.info(
        f"Generated prediction for {image_data.filename}",
        end_point="predict",
        predicted_label=predicted_label,
        prediction_confidence=prediction_confidence,
        )

    return {"filename": image_data.filename, "result": predicted_label}
