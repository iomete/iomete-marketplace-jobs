# IOMETE Lakehouse Backup

Spark-based distributed file-copy utility for the IOMETE Marketplace. It is the
low-level building block on which higher-level Iceberg catalog backup and
restore workflows will be built.

## Overview

The job copies every file under a source object-store prefix to a target
object-store prefix, distributing the work across Spark executors. It is
configured declaratively with a single JSON file and is designed to run as a
scheduled Spark job on the IOMETE platform.

This first release is intentionally a minimal, correct building block. Iceberg
awareness (snapshot-consistent table backup and restore) is layered on top in a
later release.

## Scope

**Supported storage**
- S3 (source and target)

**Copy behaviour**
- Single source prefix, recursive crawl
- Per-file retry on transient failures
- Separate credentials for source and target (production account → backup
  account)
- Configuration logged at startup with secrets redacted
- Non-zero exit if any file fails to copy (failures are never silently ignored)

**Not in this release**
- HDFS / Dell Isilon targets
- Incremental / snapshot-diff copy
- Bandwidth throttling and a machine-readable result manifest

> **Consistency caveat.** This is a raw file copy with no point-in-time
> guarantee. Copying a dataset that is being mutated can produce an inconsistent
> backup. Run against a quiescent source until the Iceberg-aware layer lands.

## Configuration

The job reads a single JSON file (mounted by IOMETE at
`/etc/configs/application.json`). Only `source` and `target` are required; each
takes storage credentials and an optional prefix:

```json
{
  "source": {
    "type": "s3",
    "bucket": "<source-bucket>",
    "prefix": "<source-prefix>",
    "endpoint": "<source-endpoint>",
    "pathStyleAccess": true,
    "accessKey": "${SOURCE_ACCESS_KEY}",
    "secretKey": "${SOURCE_SECRET_KEY}"
  },
  "target": {
    "type": "s3",
    "bucket": "<target-bucket>",
    "prefix": "<target-prefix>",
    "endpoint": "<target-endpoint>",
    "pathStyleAccess": true,
    "accessKey": "${TARGET_ACCESS_KEY}",
    "secretKey": "${TARGET_SECRET_KEY}"
  }
}
```

Optional S3 fields default sensibly: `prefix` (empty), `endpoint` (AWS default),
`pathStyleAccess` (`false`), `region` (`us-east-1`). Parallelism is derived from
the Spark cluster; there are no copy-tuning knobs to set in this release.

## Set up as a job in IOMETE

1. In the IOMETE console, navigate to **Spark → Jobs** and click **Create Job**.
2. Fill in the job form:
   - **Docker Image:** `iomete.azurecr.io/iomete/iomete-lakehouse-backup:x.y.z`
   - **Main Application File:** `local:///opt/spark/jars/iomete-lakehouse-backup.jar`
   - **Main Class:** `com.iomete.backup.App`
   - **Arguments:** `/etc/configs/application.json`
3. Choose the **Instance** size appropriate for the volume of data being copied.
4. Under **Configuration**, paste your `application.json`. IOMETE mounts it at
   `/etc/configs/application.json`.
5. Provide credentials via **Environment Variables** or IOMETE **Secrets**
   (recommended) and reference them from the config with `${VAR_NAME}`
   placeholders, which the platform substitutes at deploy time. Never commit
   real access/secret keys into the config.
6. Optionally set a **Schedule** (cron) to run the backup periodically, then
   click **Create**.
7. Trigger a run from the job detail page and monitor progress under
   **Runs → Logs**.
