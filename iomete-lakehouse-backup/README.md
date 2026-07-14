# IOMETE Lakehouse Backup

Copy your data from an S3 source to an S3 or HDFS (Dell Isilon / OneFS) target,
as a Spark job on IOMETE.

## Overview

This job copies all the files from a source S3 location to a target location
(another S3 bucket, or an HDFS filesystem such as Dell Isilon). It runs the copy
in parallel across your Spark cluster, so it scales with the size of the data.
You set it up with a single JSON file and run it as a scheduled job on the
IOMETE platform.

## What it does

- Copies files from an S3 source to an S3 or HDFS target
- Copies a folder and everything under it
- Uses separate credentials for the source and the target (for example, a
  production account and a separate backup account)
- Automatically retries a file if it fails temporarily
- Prints the configuration when it starts, with credentials hidden
- Marks the whole run as failed if any file fails to copy, so a broken backup is
  never reported as successful

> **Before you run it.** This copies files exactly as they are. If the data is
> still being written while the copy runs, the backup may be inconsistent — run
> it when the source data is not changing.

## Configuration

The job reads a single JSON file (IOMETE mounts it at
`/etc/configs/application.json`). You only need to fill in `source` and
`target` — the storage credentials and, optionally, a folder (`prefix`):

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

The remaining S3 fields are optional and have sensible defaults: `prefix`
(empty), `endpoint` (AWS default), `pathStyleAccess` (`false`), `region`
(`us-east-1`). The job scales automatically with your Spark cluster, so there is
nothing extra to tune.

### HDFS target (Dell Isilon / OneFS)

To back up into an HDFS filesystem, set the `target` to `type: "hdfs"`:

```json
{
  "source": {
    "type": "s3",
    "bucket": "<source-bucket>",
    "prefix": "<source-prefix>",
    "accessKey": "${SOURCE_ACCESS_KEY}",
    "secretKey": "${SOURCE_SECRET_KEY}"
  },
  "target": {
    "type": "hdfs",
    "namenode": "<host:port>",
    "path": "<target-path>",
    "user": "<hdfs-user>"
  }
}
```

| Field | Required | Description |
|---|---|---|
| `namenode` | yes | NameNode RPC endpoint as `host:port` (e.g. `isilon.example.com:8020`). For Dell Isilon, prefer the SmartConnect zone FQDN so the connection is load-balanced across nodes. |
| `path` | no (default empty) | Directory under the filesystem root to write into. |
| `user` | yes | The identity the files are written as. HDFS `simple` authentication has no password — the job connects as this user and OneFS applies that user's POSIX permissions. Choose a user with write access to `path`. |
| `authentication` | no (default `simple`) | Only `simple` is supported; any other value is rejected at validation. Kerberos/secure clusters are not yet supported. |

> **Credentials for HDFS.** Simple authentication carries no secret or password
> — only the `user` name — so there is nothing to store in IOMETE Secrets for
> the target. Ensure the run has network reachability to the NameNode and every
> DataNode in the cluster.

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
