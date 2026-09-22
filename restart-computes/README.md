# Restart Computes Tool

Operational utility to stop, start, or restart active compute clusters through the compute API.

## Purpose

This script reduces manual effort when many compute clusters need to be stopped or restarted, such as around upgrades, configuration changes, or maintenance operations.

It has three modes:

| Mode | What it does | Targets come from |
| --- | --- | --- |
| `stop` | Stops active computes and writes a state file listing the ones it actually stopped. | PostgreSQL |
| `start` | Starts exactly the computes listed in a state file. | the state file |
| `restart` | Stops then starts each compute in a single run. Default. | PostgreSQL |

`restart` is the default, so an invocation without `--mode` behaves as it always has.

The script is designed to be cautious and operator-friendly:

- dry run is enabled by default
- execution requires explicit confirmation
- API calls are retried on transient failures
- failed computes are retried once at workflow level
- all activity is logged to a timestamped file
- final failures are exported for follow-up
- the process exits non-zero if any compute still failed after the retry

---

## Maintenance workflow

The deployment is performed separately. This tool only handles the compute stop and start around it.

1. **Before the deployment**, stop the active computes:

   ```bash
   python restart_computes.py --mode stop --env-file .env.release
   ```

   The summary ends with the path of the state file it wrote, for example `logs/stopped_computes_20260920_140311.json`, along with the exact `--mode start` command to run later. Keep that path.

   If any compute failed to stop, the run exits non-zero and lists the failures. Do not begin the deployment until those are resolved, since an active compute is exactly what the stop was meant to clear.

2. **Run the deployment.** Nothing in this tool is involved.

3. **After the deployment**, start the computes that step 1 stopped:

   ```bash
   python restart_computes.py --mode start --env-file .env.release --state-file logs/stopped_computes_20260920_140311.json
   ```

Only computes listed in the state file are started. Computes that were already stopped before maintenance are never touched, because they were never active and so never entered the file.

### State file

`stop` writes one state file per execution. It is not cumulative: running `stop` a second time writes a second file covering only that run.

```json
{
  "stopped_at": "2026-09-20T14:03:11.482913+00:00",
  "api_base_url": "https://release.iomete.com",
  "computes": [
    {
      "id": "b3b2c8c4-83ef-4882-ab8e-8c345748fa5c",
      "domain": "default",
      "name": "hasan",
      "namespace": "spark-resources-1"
    }
  ]
}
```

`start` compares `api_base_url` against the environment it is configured for and refuses to run on a mismatch, before sending any request. That is the guard against pairing a state file with the wrong env file.

`start` does not modify or delete the state file, and it checks each compute's current status first, skipping anything already `ACTIVE` or `STARTING`. Re-running `start` with the same file is therefore safe and only retries what did not come up.

`start` needs no database access at all. Its target list comes entirely from the state file, so there is no need for a PostgreSQL tunnel after the deployment.

---

## Requirements

- Python 3.10+
- Access to the target PostgreSQL database (for `stop` and `restart` only)
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

For multiple environments or clusters, keep a separate env file for each target and select it at runtime. This avoids editing connection values between runs.

```bash
python restart_computes.py --mode stop --env-file .env.a2
python restart_computes.py --mode stop --env-file .env.p4
```
> Use the same env file for the matching start operation after maintenance.

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

**Before maintenance**

```bash
python restart_computes.py --mode stop --env-file .env.release
```

**After maintenance:** use the state file printed by the stop run

```bash
python restart_computes.py --mode start --env-file .env.release --state-file logs/stopped_computes_20260920_140311.json
```

**Existing one-run behavior (restart is the default mode)**

```bash
python restart_computes.py --mode restart --env-file .env.release
python restart_computes.py --env-file .env.release
```

**Restart one domain only**, for example after a Spark configuration change scoped to that domain:

```bash
python restart_computes.py --domain <domain-id> --env-file .env.release
python restart_computes.py --mode restart --domain <domain-id> --env-file .env.release
```

Without `--domain` every ACTIVE compute is restarted, as before. `--domain` works with restart only and is rejected with `--mode stop` or `--mode start`.

`--state-file` is required by `--mode start` and rejected by the other modes, since `stop` names its own file and `restart` needs none.

`--env-file` works with every mode and defaults to `.env`.

---

## Dry Run and Execution

Dry run is enabled by default.

In dry-run mode the script resolves its targets and prints the plan, including the URLs it would call, without sending any request. No state file is written in dry run.

To perform real changes, set:

```env
DRY_RUN=false
```

Execution mode also requires typing `YES` at the confirmation prompt, so the terminal needs to be interactive. In Docker that means passing `-it`.

---

## Output

The script writes to the `logs/` directory:

- a timestamped run log, `restart_run_<timestamp>.log`
- for `stop`, the state file `stopped_computes_<timestamp>.json`
- a timestamped failed-cluster export, `failed_clusters_<timestamp>.txt`, when computes still fail after the retry
- a final summary with succeeded, skipped and failed counts, plus timing

Terminal output uses colored status labels (`STOP`, `START`, `OK`, `SKIP`, `FAIL`, `RETRY`). The same labels are written to the log file as plain text, so the log stays readable with colors stripped.

Exit codes:

| Code | Meaning |
| --- | --- |
| `0` | Everything succeeded, or the run was a dry run or was aborted at the prompt |
| `1` | At least one compute still failed after the retry pass |
| `2` | Configuration or usage error, such as a missing state file or an environment mismatch |

---

## Docker

Build the image:

```bash
docker build -t restart-computes:local .
```

Run it with an env file. The `-it` flag is required for the confirmation prompt, and mounting `logs/` is required so the run log and the state file survive the container:

```bash
docker run --rm -it --env-file .env.docker -v "$(pwd)/logs:/app/logs" restart-computes:local
```

For the stop and start phases, append the command to override the image default:

```bash
docker run --rm -it --env-file .env.docker -v "$(pwd)/logs:/app/logs" \
  restart-computes:local python3 restart_computes.py --mode stop
```

```bash
docker run --rm -it --env-file .env.docker -v "$(pwd)/logs:/app/logs" \
  restart-computes:local python3 restart_computes.py --mode start \
  --state-file logs/stopped_computes_20260920_140311.json
```

---

## Tests

```bash
pip install pytest
python -m pytest tests/
```
