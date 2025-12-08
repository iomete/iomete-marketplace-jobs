# Jupyter Notebook Scheduler

A Spark job to schedule and execute Jupyter Notebooks using Papermill.

## Features
- **Input Sources**: Git, S3, or Manual (Local).
- **Execution**: Runs notebooks via Papermill with parameter injection.
- **Output**: Uploads executed notebooks (with results) to S3.
- **Environment**: Runs as a Docker container (Spark base).

## Configuration
The job is configured via `job_config.yaml`. See the file for details.

## Build
```bash
docker build -t jupyter-notebook-scheduler .
```

## Run
```bash
docker run -v $(pwd)/job_config.yaml:/app/job_config.yaml \
           -e AWS_ACCESS_KEY_ID=... \
           -e AWS_SECRET_ACCESS_KEY=... \
           jupyter-notebook-scheduler
```

## Development
This project uses Poetry for dependency management.
```bash
poetry env use 3.11
poetry install

poetry install
poetry run python main.py
```

## Testing
```bash
poetry run pytest
```


```bash
docker buildx build --platform linux/amd64,linux/arm64 --push -f Dockerfile -t iomete.azurecr.io/iomete/iomete-notebook-scheduler:latest . --sbom=true --provenance=true
```