# Jupyter Notebook Scheduler

A Spark job to schedule and execute Jupyter Notebooks **remotely on an IOMETE
Jupyter Gateway**. Instead of running a local kernel, the notebook cells are
executed on a remote Spark-backed kernel provisioned by the gateway; the
executed notebook (with outputs) is then uploaded to S3.

## Features

- **Input Sources**: Git, S3, or Manual (Local).
- **Remote Execution**: Runs notebooks on a remote IOMETE Jupyter Gateway kernel
  (via `nbclient` + a custom XSRF/cookie-aware gateway kernel manager).
- **Parameter Injection**: Parameters from config are injected as a code cell
  (papermill-style) before execution.
- **Output**: Uploads executed notebooks (with results) to S3, including the
  partially-executed notebook on failure for debugging.
- **Environment**: Runs as a Spark Job in Kubernetes.

## Configuration

The job is configured via `config.yaml` (see the file for details). String
values support environment variable interpolation (`${VAR}` and
`${VAR:-default}`), which is used to keep secrets out of the committed config.

**Required configuration** (validated at startup):
`input.type`, `notebook.main_file`, `gateway.url`, `gateway.token`,
`output.s3_path`.

The gateway token must be provided via the `IOMETE_GATEWAY_TOKEN` environment
variable — never commit it to `config.yaml`.

## Development

This project uses Poetry for dependency management.

```bash
poetry env use 3.11
poetry install

poetry run python main.py
```

## Docker image

The job is deployed as a container image built on top of the IOMETE Spark-Py
base image. The image bundles the application code and the locked Python
dependencies (`poetry.lock` is committed so the build is reproducible).

Build and push the image:

```bash
az acr login --name iomete

docker buildx build \
  --platform linux/amd64,linux/arm64 \
  -f Dockerfile \
  -t iomete.azurecr.io/iomete/jupyter-notebook-scheduler:latest \
  --sbom=true \
  --provenance=true \
  --push .
```
