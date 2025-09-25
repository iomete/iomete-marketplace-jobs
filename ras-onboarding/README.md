# Asset RAS Onboarding Migration Job

A PySpark job that automates migration from **domain-based access control** to the new **bundle-based Resource Access Security (RAS)** system in IOMETE.

This README will guide you through creating and running the job template in the IOMETE Console, following the same flow as the **Create Job Template** page.

---

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step 1 – Job Details](#step-1--job-details)
4. [Step 2 – Docker Image Configuration](#step-2--docker-image-configuration)
5. [Step 3 – Config Map](#step-3--config-map)
6. [Step 4 – Instance Configuration](#step-4--instance-configuration)
7. [Step 5 – Run or Schedule](#step-5--run-or-schedule)
8. [Step 6 – Monitor Execution](#step-6--monitor-execution)
9. [Migration Logic Explained](#migration-logic-explained)
10. [Duplicate Bundle Handling](#duplicate-bundle-handling)
11. [Troubleshooting](#troubleshooting)
12. [Security Notes](#security-notes)
13. [Monitoring and Logging](#monitoring-and-logging)

---

## Overview
This job helps IOMETE customers migrate assets (compute, pipelines, datasets, etc.) into the new **RAS bundle model**.  
It automatically:
- Creates default bundles per domain
- Moves assets into bundles
- Migrates permissions (user/group → bundle permissions)

---

## Prerequisites
- **IOMETE workspace** with Spark job capability
- **Python** 3.12+ (provided by runtime)
- **PySpark** 3.5.5 (provided by runtime)
- **Databases**: PostgreSQL access with proper read/write permissions
- **IAM validation**: Ensure users/groups defined as owners exist in IAM (`iam_user` / `iam_group`)
- **Config Map setup**: Database credentials and migration domains correctly defined

---

## Step 1 – Job Details
In the IOMETE Console:

- Navigate to **Job Templates → New Job Template**
- Fill in:
  ```yaml
  Name: ras-onboarding-migration
  Description: RAS onboarding migration job
  Kubernetes Namespace: <namespace>
  Run as User: <user>
  Application Type: Python
  ```

---

## Step 2 – Docker Image Configuration
Under **Image settings**:
```yaml
Image: iomete.azurecr.io/iomete/ras-onboarding:1.0.1
Main Application File: local:///app/driver.py
```
ℹ️ **Note:** If you are using a self-hosted registry or a mirror, point to the `ras-onboarding` image in your registry/mirror.

---

## Step 3 – Config Map
1. Click **Add Config**
2. Set the file path to: `/etc/configs/application.conf`
3. Copy the below config map and edit DB hosts, credentials, and domain details

```hocon
{
  databases: {
    bundle_db: {
      host: "your-db-host"
      port: 5432
      name: "iomete_iam_db"
      user: ${?DB_USER}
      password: ${?DB_PASSWORD}
      ssl_mode: disable                  # Can be set to require/disable based on how your DB is configured
    }
    assets: {
      COMPUTE: {
        host: "your-db-host"
        port: 5432
        name: "iomete_core_db"
        user: ${?ASSET_DB_USER}
        password: ${?ASSET_DB_PASSWORD}
        ssl_mode: disable                # Can be set to require/disable based on how your DB is configured
      }
    }
  }

  asset_mappings: {
    COMPUTE: {
      table: "lakehouse"
      id_column: "id"
      domain_column: "domain"
    }
  }

  migration: {
    domains: [
      { domain_id: "production", owner_id: "admin_user", owner_type: "USER", asset_type: "COMPUTE" },
      { domain_id: "staging", owner_id: "data_engineering", owner_type: "GROUP", asset_type: "COMPUTE" }
    ]
    batch_size: 1000
    retry_attempts: 3
    validate_before_migration: true
    debug_mode: false
    duplicate_bundle_action: "UPDATE"   # FAIL | SKIP | UPDATE
  }
}
```

ℹ️ **Note:** Keep the `asset_mappings` as-is (for internal use, will be deprecated soon).

---

## Step 4 – Instance Configuration
Choose the instance to run on:
- For small migrations, a **single minimal instance** is sufficient.
- For large domains, allocate more resources.

---

## Step 5 – Run or Schedule
- **Save template**
- To run immediately:
    - Go to **Applications tab** → Select template → **Run**
- To schedule:
    - Configure a schedule in the Job Template page

---

## Step 6 – Monitor Execution
- **Logs:** Check real-time logs in the IOMETE console
- **Verification:**
    - Ensure a default bundle is created per domain
    - Confirm assets are reassigned
    - Validate bundle permissions

Sample log output:
```
INFO: Created default bundle abc-123-def for domain production
INFO: Moved 25 compute assets to bundle abc-123-def
INFO: Set permissions for 12 users, 3 groups
```

---

## Migration Logic Explained
The job performs:
1. **Validation**: Owner, domain, DB connections, bundle existence
2. **Bundle Creation/Update**
3. **Asset Migration**
4. **Permission Migration**
5. **Reporting & Cleanup**

---

## Duplicate Bundle Handling

When a default bundle already exists for a domain, behavior is controlled by `duplicate_bundle_action`:

### FAIL (Recommended for first-time migrations)
```hocon
duplicate_bundle_action: "FAIL"
```
- **Behavior**: Stop execution and fail migration
- **Use Case**: Strict mode where duplicates indicate errors
- **Result**: Migration fails with clear error message

### SKIP (Recommended for incremental migrations)
```hocon
duplicate_bundle_action: "SKIP"
```
- **Behavior**: Skip domains with existing bundles
- **Use Case**: Incremental migrations, partial re-runs
- **Result**: Logs warning, continues with next domain

### UPDATE (Recommended for ownership changes)
```hocon
duplicate_bundle_action: "UPDATE"
```
- **Behavior**: Update existing bundle and reprocess
- **Use Case**: Ownership changes, configuration updates
- **Actions Performed**:
    - Updates bundle ownership (owner_id, owner_type)
    - Clears existing assets of specified type
    - Clears existing permissions for asset type
    - Re-adds assets with current configuration
    - Re-processes permissions with current mappings

---

## Troubleshooting
- **DB connection failures** → Check DB creds, test via `psql`
- **Owner validation errors** → Ensure owner exists in IAM (`USER` or `GROUP`)
- **Duplicate bundle errors** → Adjust `duplicate_bundle_action`
- **No assets found** → Confirm domain has assets, check domain name spelling
- **Permission issues** → Verify IAM role mappings

Enable debug logs:
```bash
export LOG_LEVEL="DEBUG"
```

---

## Security Notes
- Always run in **non-production first**
- Audit bundle creation and permissions after migration
- Use `FAIL` mode initially to catch misconfigurations

---

## Monitoring and Logging

### Log Levels
```bash
# Set log level via environment
export LOG_LEVEL="DEBUG"  # DEBUG, INFO, WARNING, ERROR
```

### Key Log Messages

**Migration Start:**
```
INFO: Starting Asset Onboarding Migration Job
INFO: Migration DB connection successful
INFO: Asset DB connections successful
```

**Per Domain:**
```
INFO: Owner validation successful: USER:admin_user
INFO: Found 25 compute assets in domain production
INFO: Created default bundle abc-123-def for domain production
INFO: Moved 25 compute assets to bundle abc-123-def
INFO: Set permissions for 12 users in domain production
INFO: Set permissions for 3 groups in domain production
INFO: Domain production migrated with 25 COMPUTE assets
```

**Owner Validation Errors:**
```
ERROR: Owner validation failed: Invalid owner_type 'ADMIN'. Must be one of: USER, GROUP
ERROR: Owner validation failed: Owner user 'deleted_user' not found or is deleted
ERROR: Owner validation failed: Owner group 'nonexistent_team' not found or is deleted
```

**Final Summary:**
```
INFO: Migration completed: 3/3 domains successful
```
