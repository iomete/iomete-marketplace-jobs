# Asset RAS Onboarding Migration Job

A dynamic PySpark job that automates migration from **domain-based access control** to the new **bundle-based Resource Access Security (RAS)** system in IOMETE.

**Key Features:**
- **Dynamic Asset Type Support**: Handles multiple asset types (COMPUTE, SPARK_JOB etc.) through configuration
- **Multi-Asset Domain Migration**: Migrates multiple asset types per domain in a single execution
- **Configuration-Driven Architecture**: Add new asset types without code changes
- **Dynamic Query Generation**: Automatically builds queries based on asset type configuration
- **Dynamic Permission Mapping**: Maps permissions based on service types and asset configurations

This README will guide you through creating and running the job template in the IOMETE Console, following the same flow as the **Create Job Template** page.

---

## Table of Contents
1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Step 1 – Job Details](#step-1--job-details)
4. [Step 2 – Docker Image Configuration](#step-2--docker-image-configuration)
5. [Step 3 – Config Map](#step-3--config-map)
6. [Step 4 – Instance Configuration](#step-4--instance-configuration)
7. [Step 5 – Run Job](#step-5--run-job)
8. [Step 6 – Monitor Execution](#step-6--monitor-execution)
9. [Migration Logic Explained](#migration-logic-explained)
10. [Duplicate Bundle Handling](#duplicate-bundle-handling)
11. [Per-Asset-Type Duplicate Handling](#per-asset-type-duplicate-handling)
12. [Troubleshooting](#troubleshooting)
13. [Security Notes](#security-notes)
14. [Monitoring and Logging](#monitoring-and-logging)

---

## Overview
This job helps IOMETE customers migrate assets (compute, spark jobs, pipelines, datasets, notebooks, etc.) into the new **RAS bundle model**.

### Core Capabilities
- **Creates default bundles per domain**
- **Moves assets into bundles** with dynamic asset type support
- **Migrates permissions** (user/group → bundle permissions)
- **Supports multiple asset types per domain** in a single migration run

### Supported Asset Types
- **COMPUTE**: Lakehouse compute resources
- **SPARK_JOB**: Spark job definitions

*Additional asset types can be added through configuration without code changes.*

---

## Prerequisites
- **IOMETE workspace** with Spark job capability
- **Python** 3.12+ (provided by runtime)
- **PySpark** 3.5.5 (provided by runtime)
- **Databases**: PostgreSQL access with proper read/write permissions
- **IAM validation**: Ensure users/groups defined as owners exist in IAM 
- **Config Map setup**: Database credentials, asset type configurations, and migration domains correctly defined
- **Asset Type Configuration**: Properly configured asset mappings for each asset type to migrate

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
Image: iomete.azurecr.io/iomete/ras-onboarding:1.0.2
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
  # Database configuration - will be overridden by environment variables
  databases: {
    bundle_db: {
      host: "your-db-host"
      port: 5432
      name: "iomete_iam_db"
      user: ${?DB_USER}
      password: ${?DB_PASSWORD}
    }

    # asset database
    asset_db: {
      host: "your-db-host"
      port: 5432
      name: "iomete_core_db"
      user: ${?ASSET_DB_USER}
      password: ${?ASSET_DB_PASSWORD}
    }
  }

  # Migration configuration
  migration: {
    # List of domains to migrate with their configurations
    domains: [
        {
            domain_id: "production"
            owner_id: "admin_user"
            owner_type: "USER"  # USER or GROUP
            asset_types: ["COMPUTE", "SPARK_JOB"]  # Multiple asset types - all use same asset_db
        }
        {
            domain_id: "staging"
            owner_id: "data_engineering"
            owner_type: "GROUP"
            asset_types: ["COMPUTE"]  # Different combinations per domain
        }
        {
            domain_id: "development"
            owner_id: "dev_team"
            owner_type: "GROUP"
            asset_types: ["SPARK_JOB"]  # Single asset type (also supported)
        }
        # Add more domains as needed
    ]

    # Transaction settings
    batch_size: 1000
    retry_attempts: 3

    # Validation settings
    validate_before_migration: true
    dry_run: false

    # Debug mode - enables detailed logging and query output
    debug_mode: false

    # Duplicate bundle behavior: FAIL, SKIP, or UPDATE
    # FAIL: Stop execution if default bundle already exists (default behavior)
    # SKIP: Skip migration for domains where default bundle already exists
    # UPDATE: Update existing bundle ownership and re-process assets/permissions
    duplicate_bundle_action: "UPDATE"
  }

  # INTERNAL USE ONLY: Do not edit or change the below CONFIGS WITHOUT CHECKING WITH SUPPORT FIRST
  # Asset type mappings - defines how to query assets for each type
  # All asset types use the same asset_db connection but different tables
  asset_mappings: {
      COMPUTE: {
          table: "lakehouse"
          id_column: "id"
          domain_column: "domain"
          filter_condition: "is_deleted = false"
          service: "lakehouse"
          permission_mappings: {
              list: ["VIEW"]
              view: ["VIEW"]
              manage: ["UPDATE", "DELETE", "EXECUTE", "CONSUME"]
          }
          # Action when asset already exists in bundle: SKIP, UPDATE, ERROR, or RESET
          # SKIP: Skip assets that already exist in the bundle
          # UPDATE: Add new assets, merge permissions for existing ones
          # ERROR: Raise error if any asset already exists
          # RESET: Clear all assets and permissions for this asset type before migration
          asset_action_on_duplicate: "UPDATE"
      }
      SPARK_JOB: {
          table: "spark_job"
          id_column: "id"
          domain_column: "domain"
          filter_condition: "is_deleted = false"
          service: "spark_job"
          permission_mappings: {
              list: ["VIEW"]
              view: ["VIEW"]
              manage: ["UPDATE", "DELETE", "RUN", "CONSUME"]
          }
          # Action when asset already exists in bundle: SKIP, UPDATE, ERROR, or RESET
          # SKIP: Skip assets that already exist in the bundle
          # UPDATE: Add new assets, merge permissions for existing ones
          # ERROR: Raise error if any asset already exists
          # RESET: Clear all assets and permissions for this asset type before migration
          asset_action_on_duplicate: "UPDATE"
      }
  }
}
```
⚠️ **INTERNAL USE ONLY**: The `asset_mappings` section contains critical internal configurations. Do not modify without consulting IOMETE support team.

---

## Step 4 – Instance Configuration
Choose the instance to run on:
- For migrations, a **single minimal instance** is sufficient.

---

## Step 5 – Run Job
- **Save template**
- To run immediately:
    - Select template → **Applications tab** → **Run**

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


### Adding New Asset Types (Reachout to IOMETE support team for this)

To add a new asset type, simply add a new entry to `asset_mappings`:

```hocon
asset_mappings: {
  # ... existing mappings ...

  NEW_ASSET_TYPE: {
    table: "new_asset_table"
    id_column: "id"
    domain_column: "domain"
    filter_condition: "is_active = true"
    service: "new_service"
    permission_mappings: {
      list: ["VIEW"]
      view: ["VIEW"]
      manage: ["UPDATE", "DELETE", "EXECUTE"]
    }
  }
}
```

**No code changes required** - the job will automatically handle the new asset type.

---


### Migration Flow for Multi-Asset Domains

For each domain with multiple asset types, the job will:

1. **Validate** all asset types and configurations
2. **Create/Update** a single default bundle for the domain
3. **Process each asset type sequentially**:
   - Query assets of the current type
   - Move assets to the bundle
   - Migrate permissions for that asset type
4. **Report** results for each asset type separately

### Example Multi-Asset Migration Log Output
```
INFO: Starting migration for domain: production
INFO: Found 25 COMPUTE assets in domain production
INFO: Found 15 SPARK_JOB assets in domain production
INFO: Found 8 PIPELINE assets in domain production
INFO: Created default bundle abc-123-def for domain production
INFO: Processed 25 COMPUTE assets for bundle abc-123-def
INFO: Set permissions for 12 users, 3 groups for COMPUTE assets
INFO: Processed 15 SPARK_JOB assets for bundle abc-123-def
INFO: Set permissions for 8 users, 2 groups for SPARK_JOB assets
INFO: Processed 8 PIPELINE assets for bundle abc-123-def
INFO: Set permissions for 5 users, 2 groups for PIPELINE assets
INFO: Domain production migrated: 25 COMPUTE, 15 SPARK_JOB, 8 PIPELINE assets
```

---

## Migration Logic Explained

### Enhanced Migration Flow

The job performs a **multi-phase migration process** with dynamic asset type support:

#### Phase 1: Validation & Preparation
- **Configuration Validation**: Validates all asset type configurations in `asset_mappings`
- **Owner Validation**: Ensures all domain owners exist in IAM (USER/GROUP)
- **Database Connections**: Tests connections to bundle DB and asset DB
- **Asset Type Discovery**: Parses and validates all asset types for each domain
- **Dry Run Support**: Can validate without making changes when `dry_run: true`

#### Phase 2: Bundle Management (Per Domain)
- **Bundle Detection**: Checks if default bundle already exists for domain
- **Duplicate Handling**: Applies configured `duplicate_bundle_action` (FAIL/SKIP/UPDATE)
- **Bundle Creation/Update**: Creates new bundle or updates existing bundle ownership
- **Transaction Safety**: Uses database transactions for atomicity

#### Phase 3: Multi-Asset Migration (Per Domain, Per Asset Type)
For each domain with multiple asset types, the job processes **sequentially**


#### Phase 4: Reporting & Cleanup
- **Per Asset Type Results**: Detailed counts and success/failure reporting
- **Domain Summary**: Consolidated results across all asset types
- **Transaction Cleanup**: Commits successful migrations, rolls back failures
- **Debug Logging**: Detailed query and operation logging when `debug_mode: true`


---

## Duplicate Bundle Handling

When a default bundle already exists for a domain, behavior is controlled by `duplicate_bundle_action`:

### FAIL (Recommended for strict checks if necessary)
```hocon
duplicate_bundle_action: "FAIL"
```
- **Behavior**: Stop execution and fail migration
- **Use Case**: Strict mode where duplicates indicate errors
- **Result**: Migration fails with clear error message

### SKIP (Recommended for incremental migrations to skip domains if already present and no action needs to be done)
```hocon
duplicate_bundle_action: "SKIP"
```
- **Behavior**: Skip domains with existing bundles
- **Use Case**: Incremental migrations, partial re-runs
- **Result**: Logs warning, continues with next domain

### UPDATE (Recommended for ownership changes and re-migrations)
```hocon
duplicate_bundle_action: "UPDATE"
```
- **Behavior**: Update existing bundle and reprocess assets
- **Use Case**: Ownership changes, configuration updates, re-migrations
- **Actions Performed**:
    - Updates bundle ownership (owner_id, owner_type)
    - Processes each asset type according to its `asset_action_on_duplicate` setting
    - Re-processes permissions based on asset-specific actions

---

## Per-Asset-Type Duplicate Handling

When `duplicate_bundle_action` is set to `UPDATE`, each asset type can have its own behavior for handling duplicate assets and permissions within the bundle. This is controlled by the `asset_action_on_duplicate` parameter in each asset mapping.

### Overview

Different asset types may require different migration strategies:
- **COMPUTE** resources might need a full refresh to remove stale configurations
- **SPARK_JOB** definitions might need incremental updates to preserve existing permissions
- **PIPELINE** configurations might need strict validation to prevent conflicts

The `asset_action_on_duplicate` parameter enables granular control per asset type within the same domain migration.

### Configuration

⚠️ **INTERNAL USE ONLY**: The `asset_action_on_duplicate` parameter is configured in the `asset_mappings` section and should only be modified in consultation with IOMETE support.

Each asset type in `asset_mappings` must specify an `asset_action_on_duplicate` value:

```hocon
asset_mappings: {
  COMPUTE: {
    # ... other configuration ...
    asset_action_on_duplicate: "RESET"  # Full refresh for compute resources
  }
  SPARK_JOB: {
    # ... other configuration ...
    asset_action_on_duplicate: "UPDATE"  # Merge permissions for job definitions
  }
}
```

### Available Actions

#### SKIP - Incremental Asset Addition
```hocon
asset_action_on_duplicate: "SKIP"
```
- **Asset Behavior**: Only insert assets that don't already exist in the bundle
- **Permission Behavior**: Skip permission setting if any permissions exist for this asset type
- **Use Case**: Incremental additions where existing assets should remain unchanged
- **Example**: Adding new compute resources without affecting existing ones

**Example Log Output:**
```
INFO: Skipping 15 existing assets, inserting 5 new COMPUTE assets
INFO: Skipping permission setting for COMPUTE as 12 records already exist
```

#### UPDATE - Merge and Enhance (Recommended Default)
```hocon
asset_action_on_duplicate: "UPDATE"
```
- **Asset Behavior**: Insert new assets, keep existing ones (uses SQL `ON CONFLICT DO NOTHING`)
- **Permission Behavior**: Merge new permissions with existing ones using PostgreSQL array union
- **Use Case**: Additive migrations where new permissions enhance existing ones
- **Permission Merge Logic**: `existing_permissions ∪ new_permissions` (no duplicates)

**Example Log Output:**
```
INFO: Moved 25 SPARK_JOB assets to bundle (action: UPDATE)
INFO: Set permissions for 12 users in domain production (action: UPDATE)
```


#### ERROR - Strict Validation
```hocon
asset_action_on_duplicate: "ERROR"
```
- **Asset Behavior**: Raise exception if any asset already exists in the bundle
- **Permission Behavior**: Raise exception if any permissions exist for this asset type
- **Use Case**: Strict validation to prevent accidental overwrites
- **Result**: Migration fails immediately with detailed error message

**Example Error:**
```
ERROR: Asset action is ERROR and 15 compute assets already exist in bundle abc-123: ['asset-1', 'asset-2', 'asset-3', 'asset-4', 'asset-5']
```

#### RESET - Full Refresh
```hocon
asset_action_on_duplicate: "RESET"
```
- **Asset Behavior**: Clear ALL assets of this type from bundle, then insert fresh
- **Permission Behavior**: Clear ALL permissions for this type, then set fresh
- **Use Case**: Complete refresh to remove stale configurations
- **Important**: Only affects the specific asset type, other asset types in the bundle remain unchanged

**Example Log Output:**
```
INFO: RESET action: clearing COMPUTE assets and permissions from bundle abc-123
INFO: Cleared 20 existing compute assets from bundle abc-123
INFO: Cleared 15 existing permissions for compute from bundle abc-123
INFO: Moved 25 compute assets to bundle abc-123 (action: RESET)
```

### Multi-Asset Type Migration Example

When migrating a domain with multiple asset types and different duplicate handling strategies:

**Configuration:**
```hocon
migration: {
  domains: [
    {
      domain_id: "production"
      owner_id: "admin_user"
      owner_type: "USER"
      asset_types: ["COMPUTE", "SPARK_JOB"]
    }
  ]
  duplicate_bundle_action: "UPDATE"
}

asset_mappings: {
  COMPUTE: {
    # ... config ...
    asset_action_on_duplicate: "RESET"    # Full refresh
  }
  SPARK_JOB: {
    # ... config ...
    asset_action_on_duplicate: "UPDATE"   # Incremental merge
  }
}
```

**Migration Flow:**
```
INFO: Starting migration for domain: production
INFO: Found existing bundle abc-123 for domain production
INFO: Updating bundle ownership to USER:admin_user

# COMPUTE assets with RESET action
INFO: RESET action: clearing COMPUTE assets and permissions from bundle abc-123
INFO: Cleared 20 existing COMPUTE assets from bundle abc-123
INFO: Cleared 15 existing COMPUTE permissions from bundle abc-123
INFO: Moved 25 COMPUTE assets to bundle abc-123 (action: RESET)
INFO: Set permissions for 12 users, 3 groups for COMPUTE (action: RESET)

# SPARK_JOB assets with UPDATE action (merge)
INFO: Moved 15 SPARK_JOB assets to bundle abc-123 (action: UPDATE)
INFO: Set permissions for 8 users, 2 groups for SPARK_JOB (action: UPDATE)

INFO: Domain production migrated: 25 COMPUTE (reset), 15 SPARK_JOB (merged)
```

### Decision Matrix

Choose the appropriate action based on your migration scenario:

| Scenario | Recommended Action | Reason |
|----------|-------------------|---------|
| First-time migration | `UPDATE` or `SKIP` | Safe for initial setup |
| Re-running after failure | `SKIP` | Avoids re-processing completed assets |
| Ownership change only | `UPDATE` | Merges new permissions with existing |
| Configuration cleanup | `RESET` | Removes stale configurations |
| Strict validation mode | `ERROR` | Prevents accidental overwrites |
| Adding new resources | `SKIP` or `UPDATE` | Preserves existing assets |
| Full system refresh | `RESET` | Clean slate for asset type |

### Common Patterns

#### Pattern 1: Safe Incremental Migration
```hocon
asset_mappings: {
  COMPUTE: { asset_action_on_duplicate: "SKIP" }
  SPARK_JOB: { asset_action_on_duplicate: "SKIP" }
}
```
- Adds only new assets
- Never modifies existing assets or permissions
- Safe for repeated executions

#### Pattern 2: Enhanced Permission Merge
```hocon
asset_mappings: {
  COMPUTE: { asset_action_on_duplicate: "UPDATE" }
  SPARK_JOB: { asset_action_on_duplicate: "UPDATE" }
}
```
- Merges new permissions with existing ones
- Ideal for rolling out new access controls
- Preserves existing permissions

#### Pattern 3: Selective Refresh
```hocon
asset_mappings: {
  COMPUTE: { asset_action_on_duplicate: "RESET" }      # Full refresh
  SPARK_JOB: { asset_action_on_duplicate: "UPDATE" }   # Incremental
}
```
- Refreshes compute resources completely
- Merges job permissions incrementally
- Useful for cleaning up specific asset types

#### Pattern 4: Strict Validation
```hocon
asset_mappings: {
  COMPUTE: { asset_action_on_duplicate: "ERROR" }
  SPARK_JOB: { asset_action_on_duplicate: "ERROR" }
}
```
- Fails if any duplicates exist
- Useful for testing configurations
- Ensures clean bundle state

### Troubleshooting Asset Action Issues

**Issue: All assets being skipped**
```
INFO: All 25 compute assets already exist in bundle (action: SKIP)
```
**Solution**: Change action to `UPDATE` or `RESET` if you want to reprocess them.

**Issue: Permission merge not working**
```
ERROR: Asset action is ERROR and 15 permission records already exist
```
**Solution**: Change from `ERROR` to `UPDATE` to enable permission merging.

**Issue: Unexpected asset clearance**
```
INFO: Cleared 100 existing assets from bundle
```
**Solution**: Verify `asset_action_on_duplicate` is not set to `RESET` unless intended.

**Issue: Different asset types using same action**
```
WARNING: All asset types configured with RESET - this will clear the entire bundle
```
**Solution**: Use different actions per asset type for granular control.

### Best Practices

1. **Start Conservative**: Use `SKIP` or `ERROR` for initial migrations to understand existing state
2. **Use UPDATE for Normal Operations**: Most re-migrations benefit from permission merging
3. **Reserve RESET for Cleanup**: Only use when you need to clear stale configurations
4. **Test in Non-Production First**: Validate action combinations before production migrations
5. **Review Logs Carefully**: Monitor per-asset-type results to ensure expected behavior
6. **Document Your Strategy**: Keep track of which actions are used for which asset types

---

## Troubleshooting

### Database Issues
- **DB connection failures** → Check DB creds, test via `psql`
- **Single asset DB connection** → Verify `asset_db` configuration (not per-asset databases)
- **Table not found errors** → Confirm table names in `asset_mappings` match actual database schema

### Configuration Issues
- **Asset type not found** → Verify asset type exists in `asset_mappings` configuration
- **Invalid asset configuration** → Ensure all required fields (table, id_column, domain_column, service) are present
- **Permission mapping errors** → Confirm all permission levels (list, view, manage) have valid permission arrays

### Domain & Owner Issues
- **Owner validation errors** → Ensure owner exists in IAM (`USER` or `GROUP`)
- **Duplicate bundle errors** → Adjust `duplicate_bundle_action` (FAIL/SKIP/UPDATE)
- **Invalid owner_type** → Must be exactly "USER" or "GROUP" (case-sensitive)

### Multi-Asset Issues
- **Asset types array parsing** → Ensure `asset_types` is a proper array: `["COMPUTE", "SPARK_JOB"]`
- **Mixed asset type failures** → Check logs for per-asset-type errors; some may succeed while others fail
- **No assets found for asset type** → Confirm domain contains assets for each specified asset type

### Permission Issues
- **Permission migration failures** → Verify IAM role mappings and service permissions
- **Service mapping errors** → Ensure `service` field matches expected service names in permission system
- **Dynamic permission resolution** → Check that permission_mappings contain valid permission names for each service

### Performance Issues
- **Large domain migrations** → Adjust `batch_size` for better performance
- **Multi-asset timeouts** → Consider migrating fewer asset types per domain execution
- **Memory issues** → Use smaller batch sizes for domains with many assets

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

### Enhanced Logging for Multi-Asset Support

### Key Log Messages

**Migration Start:**
```
INFO: Starting Asset Onboarding Migration Job
INFO: Migration DB connection successful
INFO: Asset DB connection successful
```

**Configuration Validation:**
```
INFO: Asset type configuration validated: 5 asset types configured
DEBUG: Validated asset type COMPUTE: table=lakehouse, service=lakehouse
DEBUG: Validated asset type SPARK_JOB: table=spark_job, service=spark_job
DEBUG: Validated asset type PIPELINE: table=pipeline, service=pipeline
```

**Per Domain (Multi-Asset):**
```
INFO: Starting migration for domain: production
INFO: Owner validation successful: USER:admin_user
INFO: Validating asset types for domain: ['COMPUTE', 'SPARK_JOB', 'PIPELINE']
INFO: Found 25 COMPUTE assets in domain production
INFO: Found 15 SPARK_JOB assets in domain production
INFO: Found 8 PIPELINE assets in domain production
INFO: Created default bundle abc-123-def for domain production

# Per asset type processing
INFO: Processing COMPUTE assets for domain production
INFO: Moved 25 COMPUTE assets to bundle abc-123-def
INFO: Set permissions for 12 users, 3 groups for COMPUTE assets in domain production

INFO: Processing SPARK_JOB assets for domain production
INFO: Moved 15 SPARK_JOB assets to bundle abc-123-def
INFO: Set permissions for 8 users, 2 groups for SPARK_JOB assets in domain production

INFO: Processing PIPELINE assets for domain production
INFO: Moved 8 PIPELINE assets to bundle abc-123-def
INFO: Set permissions for 5 users, 2 groups for PIPELINE assets in domain production

INFO: Domain production migrated: 25 COMPUTE, 15 SPARK_JOB, 8 PIPELINE assets
```

**Dynamic Query Generation (Debug Mode):**
```
DEBUG: Generated asset query for COMPUTE: SELECT id, domain FROM lakehouse WHERE domain = ? AND is_deleted = false
DEBUG: Generated permission subquery for COMPUTE: service = 'lakehouse' AND permission IN ('UPDATE', 'DELETE', 'EXECUTE', 'CONSUME')
DEBUG: Generated asset query for SPARK_JOB: SELECT id, domain FROM spark_job WHERE domain = ? AND is_deleted = false
DEBUG: Generated permission subquery for SPARK_JOB: service = 'spark_job' AND permission IN ('UPDATE', 'DELETE', 'RUN', 'CONSUME')
```

**Single Asset Type Domain:**
```
INFO: Starting migration for domain: simple-domain
INFO: Owner validation successful: GROUP:data_team
INFO: Validating asset types for domain: ['NOTEBOOK']
INFO: Found 12 NOTEBOOK assets in domain simple-domain
INFO: Created default bundle def-456-ghi for domain simple-domain
INFO: Processing NOTEBOOK assets for domain simple-domain
INFO: Moved 12 NOTEBOOK assets to bundle def-456-ghi
INFO: Set permissions for 6 users, 1 groups for NOTEBOOK assets in domain simple-domain
INFO: Domain simple-domain migrated: 12 NOTEBOOK assets
```

**Configuration Validation Errors:**
```
ERROR: Asset type INVALID_TYPE not found in asset_mappings configuration
ERROR: Invalid asset configuration for COMPUTE: missing required field 'table'
ERROR: Invalid permission mapping for SPARK_JOB: 'manage' permissions cannot be empty
```

**Owner Validation Errors:**
```
ERROR: Owner validation failed: Invalid owner_type 'ADMIN'. Must be one of: USER, GROUP
ERROR: Owner validation failed: Owner user 'deleted_user' not found or is deleted
ERROR: Owner validation failed: Owner group 'nonexistent_team' not found or is deleted
```

**Multi-Asset Migration Errors:**
```
ERROR: Failed to migrate COMPUTE assets for domain production: Database connection lost
WARNING: Skipping SPARK_JOB migration for domain production: No assets found
ERROR: Permission migration failed for PIPELINE assets in domain production: Invalid service mapping
```

**Final Summary (Enhanced):**
```
INFO: Migration completed: 3/3 domains successful
INFO: Total assets migrated: 125 (45 COMPUTE, 35 SPARK_JOB, 25 PIPELINE, 20 DATASET)
INFO: Total bundles created: 3
INFO: Total permissions migrated: 156 (users: 89, groups: 67)
```

### Troubleshooting Multi-Asset Issues

**Asset Type Not Found:**
```
ERROR: Asset type INVALID_TYPE not found in asset_mappings configuration
```
**Solution:** Add the asset type to `asset_mappings` or remove from domain configuration.

**Database Table Not Found:**
```
ERROR: Table 'nonexistent_table' doesn't exist for asset type CUSTOM_ASSET
```
**Solution:** Verify table name in asset_mappings configuration.

**Permission Mapping Issues:**
```
ERROR: Invalid permission mapping for CUSTOM_ASSET: manage permissions list is empty
```
**Solution:** Ensure all permission mappings (list, view, manage) are properly configured.
