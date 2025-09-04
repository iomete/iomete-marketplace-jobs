# IOMETE DistCp - Distributed File Copy Tool

A PySpark-based distributed file copying tool that supports multiple storage systems including S3, HDFS, GCS, and local filesystems.

## Features

- **Distributed Processing**: Uses PySpark to distribute file copying tasks across multiple executors
- **Multiple Storage Support**: Works with S3, HDFS, GCS, and local filesystems via PyArrow
- **Scalable**: Can handle large numbers of files efficiently
- **Simple Interface**: Easy-to-use command line interface

## Installation

### Using Poetry (Recommended)

```bash
# Install Poetry if not already installed
curl -sSL https://install.python-poetry.org | python3 -

# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### Using pip

```bash
pip install .
```

## Usage

### Basic Usage

```bash
# Using Poetry
poetry run distcp /path/to/source /path/to/destination

# Or if installed globally
distcp /path/to/source /path/to/destination

# Copy from S3 to local
distcp s3://bucket/source /path/to/destination

# Copy from local to S3
distcp /path/to/source s3://bucket/destination

# Copy from HDFS to GCS
distcp hdfs://namenode:9000/source gs://bucket/destination
```

### Options

- `--app-name`: Set Spark application name (default: "DistCp")
- `--log-level`: Set log level (default: "INFO")

## Architecture

1. **Driver Process**: 
   - Lists all files in the source directory recursively
   - Creates a Spark DataFrame with file metadata
   - Distributes copy tasks to executors

2. **Executor Processes**:
   - Receive file copy tasks
   - Use PyArrow filesystem APIs to copy files
   - Return results to driver

## Development

### Running Tests

```bash
# Using Poetry
poetry run python tests/test_distcp.py

# Or with pytest
poetry run pytest tests/
```

### Code Formatting

```bash
poetry run black .
poetry run flake8 .
```

## Supported Filesystems

- **Local**: `file://` or absolute paths
- **S3**: `s3://bucket/path`
- **HDFS**: `hdfs://namenode:port/path`
- **GCS**: `gs://bucket/path`

## Project Structure

```
iomete-distcp/
├── iomete_distcp/
│   ├── __init__.py
│   └── distcp.py
├── tests/
│   ├── __init__.py
│   └── test_distcp.py
├── pyproject.toml
└── README.md
```