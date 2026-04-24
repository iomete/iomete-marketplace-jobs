# IOMETE Lakehouse Backup

Spark-based data copy utility for IOMETE Marketplace. Serves as the foundation for Iceberg catalog backup and restore workflows.

## Overview

This job provides a low-level file copy mechanism between object storages. In V1, it operates as a building block for higher-level backup orchestration. Future versions will add Iceberg-awareness for both backup and restore capabilities.

## V1 Scope

**Supported Protocol**
- S3
- HDFS **TODO**


**Copy Modes:**
- Full
- Incremental **TODO**


**Features:**
- Single source path with recursive crawl
- Separate credentials for source and target**TODO**
- Configurable parallelism and bandwidth limits**TODO**
- Metrics output as JSON at destination**TODO**

## Set Up as a Job in IOMETE

Deploy this utility as a Spark job in IOMETE:

1. In the IOMETE console, navigate to **Spark → Jobs** and click **Create Job**.
2. Fill in the job form with the following values:
   - **Docker Image:** `iomete.azurecr.io/iomete/iomete-lakehouse-backup:1.0.0`
   - **Main Application File:** `local:///opt/spark/jars/iomete-lakehouse-backup-1.0.0.jar`
   - **Main Class:** `com.iomete.backup.App`
   - **Arguments:** `/etc/configs/application.json`
3. Choose the **Instance** size appropriate for the volume of data being copied.
4. Under **Configuration**, paste your `application.json`. IOMETE mounts it at `/etc/configs/application.json`. Example:

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

5. Provide credentials via **Environment Variables** or IOMETE **Secrets** (recommended) — reference them from the config using `${VAR_NAME}` placeholders. Never commit real access/secret keys into the config.
6. Optionally set a **Schedule** (cron) if you want the backup to run periodically, then click **Create**.
7. Trigger a run from the job detail page and monitor progress under **Runs → Logs**.