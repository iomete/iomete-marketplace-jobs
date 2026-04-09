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

For local Docker testing, copy it to `.env.docker` and fill in real values:

```bash
cp .env.example .env.docker
```
