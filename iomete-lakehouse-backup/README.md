# IOMETE Lakehouse Backup

Copy your data between any S3-compatible object store (AWS S3, Dell ECS, Ceph)
and any HDFS-compatible storage (Dell Isilon / OneFS, or a Hadoop cluster) as a
Spark job on IOMETE.

## Overview

This job copies all files between S3 and HDFS locations. It runs the copy
in parallel across your Spark cluster, so it scales with the size of the data.
You set it up with a single JSON file and run it as a scheduled job on the
IOMETE platform.

## What it does

- Copies files between S3 and HDFS locations
- Verifies every copied file by comparing its source and target length
- Recreates empty directories when reading from HDFS
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

### Re-copying everything

A re-run skips files already at the target with the same length and a newer
timestamp. If you suspect the target contents are wrong, force a full copy by
adding a `copy` block — no need to delete the target:

```json
{
  "copy": { "skipIdentical": false }
}
```

### Tuning how the work is split (advanced)

The job divides the files into units of work of about 1 GB and copies them in
parallel. The defaults suit most backups, so skip this section unless your run
matches one of the cases below.

```json
{
  "copy": { "bytesPerTask": 1073741824, "filesPerTask": 1000 }
}
```

| Field | Default | Lower it when |
|---|---|---|
| `bytesPerTask` | `1073741824` (1 GB) | Your backup is a few large files and most of the cluster sits idle. Avoid going below 100 MB, as very small units cost more than they save. |
| `filesPerTask` | `1000` | Your backup is hundreds of thousands of small files and tasks are slow despite copying little data. Try `250`. |

A file larger than `bytesPerTask` is copied by a single executor and is never
split, so a run can never finish faster than its largest file. The job logs that
file's size when it starts.

### Limiting bandwidth

The backup copies as fast as your network allows, and that can get in the way of
other work sharing the same connection. If you would rather keep some room free,
configure the maximum speed the job is allowed to use with
`maxBandwidthMbPerSec`:

```json
{
  "copy": { "maxBandwidthMbPerSec": 600 }
}
```

With this setting the job as a whole stays under 600 MB/s, however many
executors it runs on. Treat it as a ceiling rather than a target: the copy can
still be slower than the configured limit, and it usually is when the data is
made up of many small files.

The limit is shared across the executors, so the job also needs the number of
executors it will run with. Configure one of these on the job alongside the
limit:

| Your cluster | Setting |
|---|---|
| Fixed size | `spark.executor.instances` |
| Autoscaling | `spark.dynamicAllocation.maxExecutors` |

If neither is configured, the run fails straight away with the name of the
setting to add, so the problem never surfaces halfway through a backup. And if
you leave `maxBandwidthMbPerSec` out altogether, the copy runs at full speed and
none of this applies. Once a limit is configured, the driver log reports the
speed allowed per executor and the executor count it was calculated from.

A few things worth knowing before you settle on a number:

- **If you are replacing DistCp, the number means something different here.** Its
  `-bandwidth` applied to each map task, so the real load on the link was that
  value multiplied by however many mappers happened to be running, and it had to
  be worked out again every time the cluster changed size. Here you set what the
  whole job may use. Cluster size does not come into it.
- **Every byte crosses the network twice**, once on the way in from the source
  and once on the way out to the target, but the limit only counts it once. So
  if both sides share the same link, set the limit to about half of what you can
  spare on it. If the source and target sit on separate links, say reading from
  Isilon and writing to S3, then each side sees the number you set.
- **On an autoscaling cluster the limit is divided by `maxExecutors`**, so the
  full limit is only reached once the cluster has scaled up to that many
  executors. While it is still scaling, the copy runs slower than the limit
  rather than faster, so the network is never at risk; it just means a run that
  never gets its full allocation stays below the speed you configured. If that
  matters, set `spark.dynamicAllocation.minExecutors` to the same value as
  `maxExecutors`, or use a fixed `spark.executor.instances` instead.
- **Going slower means holding the cluster longer.** A backup that finished in
  an hour at full speed will take roughly two hours at half of it, and the
  instance stays busy for all of that time.

### Run history

Every run is recorded in two Iceberg tables under
`spark_catalog.iomete_system_db`:

| Table | Contents |
|---|---|
| `lakehouse_backup_runs` | One row per run: status, counts, byte totals, stage timings and the settings the run used. |
| `lakehouse_backup_run_file_failures` | One row per entry the run failed to copy, joined to the run by `run_id`. |

Recording is enabled by default and needs no configuration. The tables are
created on the first run. Use them to check whether a scheduled backup
succeeded, find the files it could not copy, and compare a run against earlier
ones. See [docs/run-stats.md](docs/run-stats.md) for the full column reference
and example queries.

To turn recording off or to store the tables elsewhere, add a `stats` block. The
values below are the defaults:

```json
{
  "stats": {
    "enabled": true,
    "database": "spark_catalog.iomete_system_db",
    "maxFailureRows": 1000
  }
}
```

| Field | Default | Description |
|---|---|---|
| `enabled` | `true` | Set to `false` to record nothing. |
| `database` | `spark_catalog.iomete_system_db` | Database holding both tables, optionally catalog-qualified. The table names cannot be changed. |
| `maxFailureRows` | `1000` | Maximum failure rows recorded per run. Set to `0` to record none. The `files_failed` count on the run row is unaffected. |

If a write to these tables fails, the job logs a warning and the backup
continues. A run that copies its data successfully is never failed by a problem
recording its own history.

### HDFS source or target (any HDFS-compatible storage, e.g. Dell Isilon / OneFS)

Use `type: "hdfs"` for either side of a backup or restore. To back up into an
HDFS filesystem, configure the `target`:

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

To restore an HDFS backup to S3, swap the storage types:

```json
{
  "source": {
    "type": "hdfs",
    "namenode": "<host:port>",
    "path": "<backup-path>",
    "user": "<hdfs-user>"
  },
  "target": {
    "type": "s3",
    "bucket": "<target-bucket>",
    "prefix": "<target-prefix>",
    "accessKey": "${TARGET_ACCESS_KEY}",
    "secretKey": "${TARGET_SECRET_KEY}"
  }
}
```

> **Credentials for HDFS.** Simple authentication carries no secret or password
> — only the `user` name — so there is nothing to store in IOMETE Secrets.
> Ensure the run has network reachability to the NameNode and every DataNode in
> the cluster.

## Set up as a job in IOMETE

1. In the IOMETE console, navigate to **Spark → Jobs** and click **Create Job**.
2. Fill in the job form:
   - **Docker Image:** `iomete.azurecr.io/iomete/iomete-lakehouse-backup:x.y.z`
   - **Main Application File:** `spark-internal`
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
