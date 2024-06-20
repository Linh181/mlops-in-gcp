import os
from dotenv import load_dotenv
from typing import Optional, NamedTuple

import kfp
from kfp.v2 import compiler
from typing import Optional, NamedTuple
import kfp  # kube flow pipelines
from kfp import components
from kfp.v2 import compiler
from kfp.v2.dsl import (
    component,
    Input,
    InputPath,
    OutputPath,
    Output,
    Dataset,
    Metrics,
    Model
)

load_dotenv()

GCP_PROJECT_ID = os.environ["GCP_PROJECT_ID"]
GCP_PROJECT_NAME = os.environ['GCP_PROJECT_NAME']
GCP_REGION = os.environ["GCP_REGION"]
USER_NAME = os.environ["USER_NAME"]


@component(
    base_image=f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}/docker/fancy-fashion-{USER_NAME}",
    output_component_file="_artifacts/train.yaml",
)
def train(train_data_path: str, model: Output[Model]) -> None:
    """Trains the model on the given dataset."""
    
    from pathlib import Path
    import joblib
    
    from fancy_fashion.model import train_model
    from fancy_fashion.util import local_gcs_path
    
    trained_model = train_model(local_gcs_path(train_data_path))

    model_dir = Path(model.path)
    model_dir.mkdir(parents=True, exist_ok=True)
    joblib.dump(trained_model, model_dir / "model.pkl")

    
@component(
    base_image=f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}/docker/fancy-fashion-{USER_NAME}",
    output_component_file="_artifacts/evaluate.yaml",
)
def evaluate(
    test_data_path: str, model: InputPath("Model"), metrics: Output[Metrics]
) -> NamedTuple("Outputs", [("loss", float), ("accuracy", float)]):
    
    from pathlib import Path
    import joblib
    
    from fancy_fashion.model import evaluate_model
    from fancy_fashion.util import local_gcs_path
    
    model_path = Path(model) / "model.pkl"
    test_data_path = local_gcs_path(test_data_path)
    
    model = joblib.load(model_path) 
    
    loss, accuracy = evaluate_model(model, test_data_path)  
    
    metrics.log_metric("loss", loss)
    metrics.log_metric("accuracy", accuracy)
    

@component(
    base_image=f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}/docker/fancy-fashion-{USER_NAME}",
    output_component_file="_artifacts/predict.yaml",
)
def predict(
    validation_data_path: str, model: InputPath("Model"), predictions: Output[Dataset]
) -> None:
    from pathlib import Path
    import joblib
    
    import pandas as pd

    from fancy_fashion.model import generate_predictions
    from fancy_fashion.util import local_gcs_path
    
    model = joblib.load(Path(model) / "model.pkl")

    predicted = generate_predictions(model, local_gcs_path(validation_data_path))
    
    predictions_dir = Path(predictions.path)
    predictions_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame.from_records(predicted).to_parquet(predictions_dir / "predictions.parquet")


@component(base_image="google/cloud-sdk:latest")
def deploy(
    model: Input[Model], 
    image: str, 
    region: str, 
    project_id: str, 
    service: str, 
    service_account: str
):
    import subprocess
    import logging

    logger = logging.getLogger(__name__)

    model_url = f"{model.uri}/model.pkl"

    process = subprocess.Popen(
        [
            "gcloud", 
            "run",
            "deploy",
            "--project", 
            project_id,
            "--image", 
            image,
            "--memory",
            "1024Mi",
            "--set-env-vars",
            f"MODEL_URI={model_url}",
            "--region",
            region,
            "--service-account",
            service_account,
            service
        ], 
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    # print the logs of the subprocess to logger
    def check_io(process):
        while True:
            output = process.stdout.readline().decode()
            if output:
                logger.log(logging.INFO, output)
            else:
                break

    # keep checking stdout/stderr until the child exits
    while process.poll() is None:
        check_io(process)
    
    # check the return code of the process command
    if process.returncode != 0:
        raise Exception(f"Subprocess failed, process exited with code {process.returncode}")


@kfp.dsl.pipeline(name=f"fancy-fashion-{USER_NAME}")
def pipeline(train_path: str, test_path: str, validation_path: str):
    train_task = train(train_path)
    evaluate_task = evaluate(test_path, train_task.outputs["model"]) 
    predict_task = predict(validation_path, train_task.outputs["model"])
    predict_task.after(evaluate_task)

    deploy(
        project_id=GCP_PROJECT_ID,
        image=f"{GCP_REGION}-docker.pkg.dev/{GCP_PROJECT_ID}/docker/fancy-fashion-serving-{USER_NAME}:latest",
        region=GCP_REGION,
        service=f"fancy-fashion-serving-{USER_NAME}",
        model=train_task.outputs["model"],
        service_account=f"vmd-fashion@{GCP_PROJECT_ID}.iam.gserviceaccount.com",
    )

if __name__ == "__main__":
    compiler.Compiler().compile(
        pipeline,
        package_path="_artifacts/pipeline.json",
    )
