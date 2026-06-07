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

## Packaging (`dependencies.zip`)

The job is deployed as an IOMETE Spark job: a `dependencies.zip` bundle is
submitted alongside `main.py` and `config.yaml` (there is no container image).
`poetry.lock` is committed so the bundle is reproducible.

Build the bundle with the provided script (requires the Poetry export plugin —
`poetry self add poetry-plugin-export`):

```bash
./build.sh
```

This exports the locked dependency set, installs it into a clean `build/`
directory, and produces `dependencies.zip`. Submit that zip together with
`main.py` and `config.yaml` as the IOMETE job package.
