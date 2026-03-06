# IOMETE: Orphaned Asset Detection Job

This Spark job template scans marketplace metadata and writes every orphaned relationship into the `orphaned_asset` table. A relationship is **orphaned** when a soft-deleted user (`is_deleted = true`) is still referenced by an active asset (and for bundles/data access policies we also consider soft-deleted groups).

Just like the compaction job, you can deploy it in a few clicks:

1. Navigate to **Spark → Job Templates**.
2. Find **Orphaned Asset Detection** and click **Deploy**.
3. Keep the default Docker image and `local:///app/driver.py`.
4. Adjust instance size / schedule / configuration, then **Create** the job.

The template image already contains Python dependencies and looks for the configuration file at `/etc/configs/application.conf` (the default Job Template path).

---

## Configuration (application.conf)

```hocon
{
  job: {
    // Enable verbose logging when diagnosing issues
    debug_mode: false
  }

  databases: {
    // IAM / marketplace database (source + orphaned_asset writes)
    global_db: {
      host: "localhost"
      port: 5432
      name: "iomete_iam_db"
      user: ${?DB_USER}                 // pulled from secret/env
      password: ${?DB_PASSWORD}
      ssl_mode: "require"
    }
  }
}
```

Only the credentials typically change between environments; everything else can stay with the defaults. Empty or missing env vars will raise an error when the job starts.

---

## What the Job Detects

| Asset | Condition that triggers an orphan |
| --- | --- |
| `DOMAIN` | Any owner in the `owners` array is a deleted user |
| `BUNDLE` | `owner_id` belongs to a deleted user or group |
| `DATA_ACCESS_POLICY` | Policy `identity` points at a deleted user or group |

For every match the job writes a row containing `asset_id`, `asset_name`, `domain_id` (when available), `asset_type`, `owner_type`, `owner_id`, and the original archive date. Each run truncates and repopulates the `orphaned_asset` table to reflect the current snapshot of orphaned relationships.

---

## Local Development & Tests

```bash
cd orphaned-asset-detection
python3 -m venv .env && source .env/bin/activate
pip install -e ".[dev]"
pytest --capture=no --log-cli-level=DEBUG
```

`make run` executes the driver locally (you will need real database credentials). `make docker-push` builds and publishes the multi-arch image that the template references.

---

## Behind the Scenes

1. `driver.py` starts a Spark session and loads `application.conf`.
2. `orphaned_asset_detection.main` reads every required table with Spark JDBC (deleted IAM users, domains, bundles, DSP actions, and the existing `orphaned_asset` table).
3. Simple PySpark transforms explode the `owners` JSON array, normalize owner types, and join against the deleted identities to build a unified candidate list.
4. A `LEFT ANTI` join removes anything already persisted, and Spark appends the fresh rows back into `orphaned_asset` with generated UUIDs.

This small PySpark script mirrors the compaction job style—no custom database layer, just straightforward DataFrame operations.
