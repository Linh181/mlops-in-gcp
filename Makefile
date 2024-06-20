include .env

PYTHON ?= python

.PHONY:
init:
	gcloud auth configure-docker ${GCP_REGION}-docker.pkg.dev
	poetry env use ${PYTHON}
	poetry config virtualenvs.in-project true
	poetry install

.PHONY: build
build: build-docker-pipeline build-docker-serving

.PHONY: build-docker-pipeline
build-docker-pipeline:
	docker buildx build --platform linux/amd64 -f Dockerfile.pipeline -t ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/docker/fancy-fashion-${USER_NAME} .

.PHONY: build-docker-serving
build-docker-serving:
	docker buildx build --platform linux/amd64 -f Dockerfile.serving -t ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/docker/fancy-fashion-serving-${USER_NAME} .

.PHONY: format
format:
	poetry run black src tests

.PHONY: lint
lint:
	poetry run pre-commit run --all

.PHONY: push
push: push-docker-pipeline push-docker-serving

.PHONY: push-pipeline
push-pipeline:
	mkdir -p _artifacts
	poetry run python src/fancy_fashion/pipeline.py
	gsutil cp _artifacts/pipeline.json gs://${GCP_PROJECT_ID}-fashion-artifacts/models/fancy-fashion/${USER_NAME}/pipeline.json

.PHONY: push-docker-pipeline
push-docker-pipeline: build-docker-pipeline
	docker push ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/docker/fancy-fashion-${USER_NAME}

.PHONY: push-docker-serving
push-docker-serving: build-docker-serving
	docker push ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/docker/fancy-fashion-serving-${USER_NAME}

.PHONY: run-docker-serving
run-docker-serving: build-docker-serving
	docker run -p 8081:8080 --rm -it --env MODEL_URI=${MODEL_URI} ${GCP_REGION}-docker.pkg.dev/${GCP_PROJECT_ID}/docker/fancy-fashion-serving-${USER_NAME} 

.PHONY: run-serving
run-serving:
	poetry run uvicorn fancy_fashion.app:app --host 0.0.0.0 --port 8081

.PHONY: test
test:
	poetry run pytest

# Trainer: Run this once in advance of the training
.PHONY: generate-dataset
generate-dataset:
	poetry run python scripts/generate_dataset.py && gsutil -m cp -r ./data/* gs://${GCP_PROJECT_ID}-fashion-inputs/
