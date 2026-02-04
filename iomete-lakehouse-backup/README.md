# IOMETE Lakehouse Backup

Spark-based data copy utility for IOMETE Marketplace. Serves as the foundation for Iceberg catalog backup and restore workflows.

## Overview

This job provides a low-level file copy mechanism between object storage (S3/ECS) and HDFS-backed storage (Isilon). In V1, it operates as a building block for higher-level backup orchestration. Future versions will add Iceberg-awareness and full restore capabilities.

## V1 Scope

**Sources:**
- S3 (including ECS-style endpoints with custom endpoints)
- HDFS (Isilon via standard HDFS protocol)

**Targets:**
- S3 (including ECS-style endpoints)
- HDFS (Isilon via standard HDFS protocol)

All source/target combinations are supported: S3→S3, S3→HDFS, HDFS→S3, HDFS→HDFS.

**Copy Modes:**
- **Full**: Copy all files from source to target
- **Incremental**: Copy only changed files based on configurable strategy
  - `mtime`: Compare size + modification time (faster)
  - `checksum`: Compare file checksums (more accurate)

**Features:**
- Single source path with recursive crawl
- Separate credentials for source and target
- Configurable parallelism and bandwidth limits
- Metrics output as JSON at destination

## Configuration

The job accepts a JSON configuration. Examples below show the V1 structure.

### S3 → S3 (Full Copy)

```json
{
  "source": {
    "type": "s3",
    "bucket": "source-bucket",
    "prefix": "data/warehouse/",
    "endpoint": "https://s3-source.example.com",
    "pathStyleAccess": true,
    "accessKey": "${SOURCE_ACCESS_KEY}",
    "secretKey": "${SOURCE_SECRET_KEY}"
  },
  "target": {
    "type": "s3",
    "bucket": "backup-bucket",
    "prefix": "backups/warehouse/",
    "endpoint": "https://s3-target.example.com",
    "pathStyleAccess": true,
    "accessKey": "${TARGET_ACCESS_KEY}",
    "secretKey": "${TARGET_SECRET_KEY}"
  },
  "copy": {
    "mode": "full",
    "options": {
      "skipCrcCheck": true,
      "ignoreFailures": false,
      "maxMaps": 150,
      "bandwidthMb": 1024,
      "numListStatusThreads": 30
    }
  }
}
```

### S3 → HDFS/Isilon (Incremental Copy)

```json
{
  "source": {
    "type": "s3",
    "bucket": "source-bucket",
    "prefix": "data/warehouse/",
    "endpoint": "https://s3-source.example.com",
    "pathStyleAccess": true,
    "accessKey": "${SOURCE_ACCESS_KEY}",
    "secretKey": "${SOURCE_SECRET_KEY}"
  },
  "target": {
    "type": "hdfs",
    "path": "/backups/warehouse",
    "namenode": "hdfs://namenode:8020",
    "auth": {
      "type": "kerberos",
      "principal": "${HDFS_PRINCIPAL}",
      "keytabPath": "/etc/security/keytabs/hdfs.keytab"
    }
  },
  "copy": {
    "mode": "incremental",
    "incrementalStrategy": "mtime",
    "options": {
      "skipCrcCheck": false,
      "ignoreFailures": true,
      "maxMaps": 150,
      "bandwidthMb": 1024,
      "numListStatusThreads": 30
    }
  }
}
```

### HDFS → S3 (Full Copy)

```json
{
  "source": {
    "type": "hdfs",
    "path": "/data/warehouse",
    "namenode": "hdfs://namenode:8020",
    "auth": {
      "type": "simple",
      "user": "hdfs"
    }
  },
  "target": {
    "type": "s3",
    "bucket": "backup-bucket",
    "prefix": "backups/warehouse/",
    "endpoint": "https://s3-target.example.com",
    "pathStyleAccess": true,
    "accessKey": "${TARGET_ACCESS_KEY}",
    "secretKey": "${TARGET_SECRET_KEY}"
  },
  "copy": {
    "mode": "full",
    "options": {
      "skipCrcCheck": true,
      "ignoreFailures": false,
      "maxMaps": 150
    }
  }
}
```

### HDFS with HA + Kerberos

```json
{
  "target": {
    "type": "hdfs",
    "path": "/backups/warehouse",
    "ha": {
      "nameservice": "mycluster",
      "namenodes": ["nn1", "nn2"],
      "rpcAddresses": {
        "nn1": "namenode1.example.com:8020",
        "nn2": "namenode2.example.com:8020"
      }
    },
    "auth": {
      "type": "kerberos",
      "principal": "hdfs-backup@EXAMPLE.COM",
      "keytabPath": "/etc/security/keytabs/hdfs-backup.keytab"
    }
  }
}
```

### HDFS with Simple Auth

```json
{
  "target": {
    "type": "hdfs",
    "path": "/backups/warehouse",
    "namenode": "hdfs://namenode:8020",
    "auth": {
      "type": "simple",
      "user": "hdfs"
    }
  }
}
```

## Configuration Reference

### Source/Target Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | string | Yes | `s3` or `hdfs` |
| `bucket` | string | S3 only | S3 bucket name |
| `prefix` | string | S3 only | Path prefix within bucket |
| `endpoint` | string | S3 only | Custom S3/ECS endpoint URL |
| `pathStyleAccess` | boolean | No | Use path-style access (default: false) |
| `accessKey` | string | S3 only | Access key (supports env var substitution) |
| `secretKey` | string | S3 only | Secret key (supports env var substitution) |
| `path` | string | HDFS only | HDFS path |
| `namenode` | string | HDFS only | Namenode URI (if not using HA) |
| `ha` | object | No | HA configuration (see example above) |
| `auth.type` | string | No | `simple` (default) or `kerberos` |
| `auth.user` | string | No | Username for simple auth |
| `auth.principal` | string | Kerberos | Kerberos principal |
| `auth.keytabPath` | string | Kerberos | Path to keytab file |

### Copy Options

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `mode` | string | `full` | `full` or `incremental` |
| `incrementalStrategy` | string | `mtime` | `mtime` (size + modification time) or `checksum` |
| `skipCrcCheck` | boolean | false | Skip CRC verification during copy |
| `ignoreFailures` | boolean | false | Continue on individual file failures |
| `maxMaps` | integer | 20 | Maximum parallel copy tasks |
| `bandwidthMb` | integer | unlimited | Bandwidth limit in MB/s |
| `numListStatusThreads` | integer | 1 | Threads for file listing |

## Metrics Output

The job writes a metrics JSON file to the target location upon completion:

```json
{
  "status": "completed",
  "filesTotal": 1000,
  "filesCopied": 998,
  "filesSkipped": 2,
  "filesFailed": 0,
  "bytesTotal": 10737418240,
  "bytesCopied": 10737418240,
  "startTime": "2024-01-15T10:00:00Z",
  "endTime": "2024-01-15T10:15:00Z",
  "errors": []
}
```

Status values: `completed`, `failed`, `partial`

## Usage in IOMETE

1. Navigate to **Spark Jobs** → **Create New**
2. Configure:
   - **Name:** `lakehouse-backup`
   - **Docker Image:** `iomete/iomete-lakehouse-backup:<version>`
   - **Main application file:** `spark-internal`
   - **Main class:** `com.iomete.backup.App`
3. Click **Add Config**, set path to `/etc/configs/application.conf`, paste HOCON config
4. Add environment variables for secrets (referenced as `${?SOURCE_ACCESS_KEY}` in config)

## Hadoop DistCP Flag Mapping (Supported Subset)

| DistCP Flag | Config Field |
|-------------|--------------|
| `-Dfs.s3a.endpoint` | `source.endpoint` / `target.endpoint` |
| `-Dfs.s3a.access.key` | `source.accessKey` / `target.accessKey` |
| `-Dfs.s3a.secret.key` | `source.secretKey` / `target.secretKey` |
| `-Dfs.s3a.path.style.access` | `source.pathStyleAccess` / `target.pathStyleAccess` |
| `-skipcrccheck` | `copy.options.skipCrcCheck` |
| `-m` | `copy.options.maxMaps` |
| `-bandwidth` | `copy.options.bandwidthMb` |
| `-numListstatusThreads` | `copy.options.numListStatusThreads` |
| `-i` | `copy.options.ignoreFailures` |

## V1 Limitations

- Marketplace job only (no standalone CLI)
- No Iceberg-awareness (treats files as opaque)
- Caller responsible for ensuring consistent file set when backing up Iceberg tables
- No delete sync (files removed from source are not removed from target)
- No dry-run mode

## Future Roadmap

**Restore Functionality:**
- Catalog name as input (auto-discover catalog paths)

**Iceberg-Aware Backup:**
- Snapshot-based copy (only copy files referenced by a specific snapshot)
- Automatic metadata + data file consistency
- Manifest-based incremental (copy only new data files since last backup)

**Restore Enhancements:**
- Automatic catalog re-registration after restore
- Path rewriting for cross-environment restore

**Additional Features:**
- Delete sync (remove target files not in source)
- Dry-run mode (list operations without executing)
- Additional storage backends (GCS, Azure, etc.)

## Open Questions (Future Design)

- **Path rewriting**: Iceberg metadata contains absolute paths. When restoring to a different location, how should path rewriting be handled? Options include using Iceberg's `rewrite_table_path` procedure or building custom path translation.
- **Catalog registration**: Should restore automatically register tables, or provide a separate registration step? Need to evaluate Iceberg's `register_table` procedure integration.
- **Snapshot selection**: For Iceberg-aware backup, how to specify which snapshot to back up (latest, specific ID, by timestamp)?

## References

- [Apache Iceberg Maintenance](https://iceberg.apache.org/docs/latest/maintenance/)
- [IOMETE Iceberg Disaster Recovery](https://iomete.com/resources/blog/iceberg-disaster-recovery)
- [spark-distcp](https://github.com/CoxAutomotiveDataSolutions/spark-distcp) - Reference implementation