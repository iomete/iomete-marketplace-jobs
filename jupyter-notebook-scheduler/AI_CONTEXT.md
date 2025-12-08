# Jupyter Notebook Scheduler - AI Context

## Project Overview
**Goal**: Create a Spark job that executes a Jupyter Notebook and logs/saves the output.
**Context**: Part of the `iomete-marketplace-jobs` monorepo.
**Deployment**: Scheduled via Kubernetes (external to this project).

## Core Requirements
1.  **Execution**:
    - Run a specific "main" Jupyter Notebook.
    - Support dependencies (other notebooks or Python files).
    - Environment: Spark Job (likely running on Kubernetes).

2.  **Input Sources**:
    - **Git**: Repository URL, branch/tag selection.
    - **S3**: Bucket/path to file or folder.
    - **Manual**: Direct file injection (mechanism TBD).

3.  **Output**:
    - Save the executed notebook (with outputs) to a specified location.
    - Log execution details.

## Project Structure
```
jupyter-notebook-scheduler/
├── main.py                 # Entry point: Orchestrates Fetch -> Execute -> Upload
├── config.py               # Configuration loading (Env vars & YAML)
├── pyproject.toml          # Poetry configuration & dependencies
├── Dockerfile              # Job image
├── scheduler/
│   ├── __init__.py
│   ├── fetcher.py          # Handles Git/S3/Local inputs
│   ├── executor.py         # Runs notebook via Papermill
│   └── storage.py          # Handles S3 uploads
└── tests/                  # Unit tests
```

## Configuration
The job is configured via a YAML file (`config.yaml`) to avoid excessive environment variables.
**Env Var**: `CONFIG_PATH` (defaults to `/etc/config/config.yaml`).

**Structure**:
```yaml
input:
  type: "git" # Options: git, s3, manual
  path: "https://github.com/..." # or s3://...
  branch: "main" # optional, for git
  token: "..." # optional, for private repos

notebook:
  main_file: "notebooks/main.ipynb"
  parameters: # Parameters to inject via Papermill
    date: "2023-01-01"
    env: "production"

output:
  s3_path: "s3://bucket/..."
```

## Component Details

### Input Fetcher (`scheduler/fetcher.py`)
- **GitFetcher**: Clones repo, checks out branch/tag.
- **S3Fetcher**: Downloads files/folders from S3.
- **LocalFetcher**: Validates existence of mounted files (for manual injection).

### Executor (`scheduler/executor.py`)
- Integrates `papermill` for notebook execution.
- Handles parameter injection.
- Captures logs and handles execution errors.

### Storage Handler (`scheduler/storage.py`)
- Handles S3 upload logic for the output notebook.

### Main Driver (`main.py`)
- Orchestrates the flow: Fetch -> Execute -> Upload.
- Handles top-level error handling and logging.

## History & Decisions
- *2025-11-25*: Initial context creation.
- *2025-11-25*: Clarified requirements: S3 output, Papermill for params, Spark environment provided.
- *2025-11-25*: Decision to use `config.yaml` for configuration.
- *2025-11-25*: Switched to **Poetry** for dependency management.
- *2025-11-26*: Merged implementation plan into AI Context.
