# Restart Computes Tool

Operational utility to restart active compute clusters through the compute API.

## Purpose

This script is intended to reduce manual effort when many compute clusters need to be restarted, such as after upgrades, configuration changes, or maintenance operations.

It discovers active compute clusters from PostgreSQL, then restarts them one by one through the compute API by sending:

1. `STOP`
2. poll until the compute reaches `STOPPED`
3. `START`
4. poll until the compute returns to `ACTIVE`

The script is designed to be cautious and operator-friendly:

- dry run is enabled by default
- execution requires explicit confirmation
- API calls are retried on transient failures
- failed cluster restarts are retried once at workflow level
- all activity is logged to a timestamped file
- final failures are exported for follow-up

---

## Current Behavior

The script currently targets compute clusters where:

- `is_deleted = false`
- `driver_status = 'ACTIVE'`

These are discovered from the `lakehouse` table in the target PostgreSQL database.

> Note: the database is used only to discover restart candidates. Runtime lifecycle state is verified through the compute API while polling.

---

## Requirements

- Python 3.10+
- Access to the target PostgreSQL database
- Valid API token for the target IOMETE environment
- Network access to the target API base URL
- `requirements.txt` installed

---


## Environment Configuration

Use `.env.example` as the template for required configuration.

For local runs, copy `.env.example` to `.env` or another env file and fill in real values:

```bash
cp .env.example .env
```

For local Docker testing, copy it to `.env.docker` and fill in real values:

```bash
cp .env.example .env.docker
```

If no env file is provided, the script defaults to `.env`.

You can also keep multiple env files for different environments and select one at runtime.

Examples:

```bash
python restart_computes.py
python restart_computes.py --env-file .env
python restart_computes.py --env-file .env.test
python restart_computes.py --env-file configs/release.env
```

> Note: `.env.example` is a template only. It is not used as the default runtime config.

The selected env file controls the target environment by providing the PostgreSQL and API connection details. The script then discovers active compute clusters dynamically from that environment.

---

## Local Setup

Create and activate a virtual environment, then install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

---

## Usage

### Default behavior

If no env file is provided, the script loads `.env`:

```bash
python restart_computes.py
```

### Select env file at runtime

Use `--env-file` to switch environments without editing `.env` each time:

```bash
python restart_computes.py --env-file .env.test
python restart_computes.py --env-file configs/release.env
```

---

## Dry Run and Execution

Dry run is enabled by default.

In dry-run mode, the script discovers active compute clusters and prints/logs the actions it would take without sending restart requests.

To perform real restarts, set:

```env
DRY_RUN=false
```

Execution mode also requires explicit confirmation before restart requests are sent.

---

## Output

The script writes:

- a timestamped run log to the `logs/` directory
- a timestamped failed-cluster export file when clusters still fail after retry
- a final summary with success count, failure count, success rate, and timing information

---

## Docker

Build the image:

```bash
docker build -t restart-computes:local .
```

Run it with an env file:

```bash
docker run --rm --env-file .env.docker restart-computes:local
```
