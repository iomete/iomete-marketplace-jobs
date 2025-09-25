# Asset RAS Onboarding Migration Job

A PySpark-based job that automates the Asset RAS (Resource Access Security) onboarding migration process for the IOMETE platform. This job migrates domain assets (compute resources, pipelines, datasets, etc.) from traditional domain-based access control to the new bundle-based RAS system with proper role-based permissions.

## Table of Contents

- [Overview](#overview)
- [What This Job Does](#what-this-job-does)
- [Supported Asset Types](#supported-asset-types)
- [Configuration Reference](#configuration-reference)
- [Running on IOMETE Platform](#running-on-iomete-platform)
- [Duplicate Bundle Handling](#duplicate-bundle-handling)
- [Migration Process](#migration-process)
- [Monitoring and Logging](#monitoring-and-logging)
- [Troubleshooting](#troubleshooting)

## Overview

This job is designed for IOMETE customers migrating from domain-based asset management to the new bundle-based Resource Access Security (RAS) system. It automates the complex process of:

1. **Creating default bundles** for each domain
2. **Moving assets** from domains to appropriate bundles
3. **Migrating permissions** based on existing role mappings


## What This Job Does

### Migration Steps (Per Domain)

1. **Validation Phase**
   - **Owner Validation**: Verifies that the specified owner exists and owner type is valid
   - **Domain Validation**: Checks if domain exists and contains assets
   - **Database Validation**: Validates database connections
   - **Bundle Validation**: Handles existing bundles based on configuration

2. **Bundle Creation**
   - Creates a default bundle named `{domain_id}_default`
   - Assigns ownership to specified user or group
   - Generates unique bundle ID

3. **Asset Migration**
   - Identifies all assets of specified type in domain
   - Moves assets to the new bundle
   - Maintains asset relationships and metadata

4. **Permission Migration**
   - Analyzes existing role mappings for users and groups
   - Creates corresponding bundle permissions
   - Maps permission levels (view, manage → VIEW, UPDATE, DELETE, etc.)


## Supported Asset Types

The job supports multiple asset types through configurable mappings:

| Asset Type | Default Table | Description                                           |
|------------|---------------|-------------------------------------------------------|
| **COMPUTE** | `lakehouse` | Lakehouse compute resources                           |
| **Custom Types** | Configurable | Any asset type via configuration **(for future use)** |

## Configuration Reference

### Complete Configuration Example

```hocon
{
    # Database Configurations
    databases: {
        # Bundle migration database (IOMETE IAM DB)
        bundle_db: {
            host: "your-db-host"
            port: 5432
            name: "iomete_iam_db"
            user: ${?DB_USER}
            password: ${?DB_PASSWORD}
        }

        # Asset databases (by asset type)
        assets: {
            COMPUTE: {
                host: "your-db-host"
                port: 5432
                name: "iomete_core_db"
                user: ${?ASSET_DB_USER}
                password: ${?ASSET_DB_PASSWORD}
            }
            # Add more asset databases as needed (for future use)
        }
    }

    # Asset Type Mappings - (Please keep this section as is without any change)
    asset_mappings: {
        COMPUTE: {
            table: "lakehouse"           # Table containing compute assets
            id_column: "id"              # Primary key column
            domain_column: "domain"      # Domain association column
        }
    }

    # Migration Configuration
    migration: {
        # Domains to migrate
        domains: [
            {
                domain_id: "production"         # Domain identifier
                owner_id: "admin_user"          # Bundle owner (IMPORTANT keep the username as seen on IOMETE platform)
                owner_type: "USER"              # USER or GROUP (IMPORTANT Ensure this is set accordingly to the above Owner ID either a USER or GROUP)
                asset_type: "COMPUTE"           # Asset type to migrate
            },
            {
                domain_id: "staging"
                owner_id: "data_engineering"
                owner_type: "GROUP"
                asset_type: "COMPUTE"
            },
            {
                domain_id: "analytics"
                owner_id: "analytics_team"
                owner_type: "GROUP"
                asset_type: "COMPUTE"
            }
        ]

        # Processing Configuration
        batch_size: 1000                    # Assets per batch
        retry_attempts: 3                   # Retry failed operations

        # Validation Settings
        validate_before_migration: true     # Pre-migration validation

        # Debug Settings
        debug_mode: false                   # Enable detailed logging

        # Duplicate Bundle Handling
        duplicate_bundle_action: "UPDATE"   # FAIL, SKIP, or UPDATE
    }
}
```

### Asset Mappings Explained

Each asset type needs a mapping configuration:

```hocon
asset_mappings: {
    YOUR_ASSET_TYPE: {
        table: "your_table_name"           # Database table containing assets
        id_column: "your_id_column"        # Primary key column name
        domain_column: "your_domain_col"   # Column linking to domain
    }
}
```
**use the asset mapping shared in the above configuration example for COMPUTE as is without any changes, this is for internal use and will be deprecated in the next release**


### Optional Configuration Overrides


## Running on IOMETE Platform

### Step 1: Create Job Configuration

1. **Log into IOMETE Console**
   - Navigate to your IOMETE domain
   - Go to **Job Templates** → **New Job Template**

2. **Job Details**
   ```yaml
   Name: ras-onboarding-migration
   Description: {any description you feel is right for your reference}
   Kubernetes Namespace: Select the Kubernetes namespace you want to run the job on 
   Run as User: Select the user you want to run the job as
   Application Type: Select Python
   ```

3. **Docker Image Configuration**
   ```yaml
   Image: iomete.azurecr.io/iomete/ras-onboarding:1.0.1
   Main Application File: local:///app/driver.py
   ```


### Step 2: Configure Config Map

1. **Under ConfigMap**
   - Select Add config
   - copy the example config provided above and edit the DB host and credentials and other parameters if required
   - enter `/etc/configs/application.conf` in the File Path 


### Step 4: Configure Instance

1. **Configure instance to run on**
    - You can run on single instance with minimum configuration
   

### Step 5: Schedule or Run Job

1. **Save the template**
2. **One-time Execution**
    - Select Template you just created →  Select **Applications** tab → Click **Run** for immediate execution
    - Monitor progress in job logs


### Step 6: Monitor Execution

1. **Job Logs**
   - View real-time logs in IOMETE console
   - Look for migration progress messages
   - Check for any errors or warnings

2. **Verification**
   - Verify default bundle was created in the domain you ran migration for
   - Check asset assignments in the default bundle created
   - Validate permission mappings in the bundle


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

## Migration Process

### Detailed Flow

```
1. Initialize Connections
   ├── Bundle DB (IAM database)
   ├── Asset DBs (by type)
   └── Test connectivity

2. For Each Domain:
   ├── Validation Phase
   │   ├── Validate owner exists (USER in iam_user or GROUP in iam_group)
   │   ├── Validate owner_type is 'USER' or 'GROUP'
   │   ├── Check domain exists
   │   ├── Count assets in domain
   │   ├── Check for existing bundle
   │   └── Apply duplicate_bundle_action
   │
   ├── Bundle Management
   │   ├── Create new bundle OR
   │   └── Update existing bundle
   │
   ├── Asset Migration
   │   ├── Query assets from asset DB
   │   ├── Move to bundle
   │   └── Log asset counts
   │
   └── Permission Migration
       ├── Query user role mappings
       ├── Query group role mappings
       ├── Map permissions
       └── Insert bundle permissions

3. Cleanup & Reporting
   ├── Commit transactions
   ├── Log final statistics
   └── Report success/failures
```



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
INFO: Found 25 compute assets in domain production
INFO: Created default bundle abc-123-def for domain production
INFO: Moved 25 compute assets to bundle abc-123-def
INFO: Set permissions for 12 users in domain production
INFO: Set permissions for 3 groups in domain production
INFO: Domain production migrated with 25 COMPUTE assets
```

**Final Summary:**
```
INFO: Migration completed: 3/3 domains successful
```


## Troubleshooting

### Common Issues

#### 1. Database Connection Failures

**Error:** `Failed to connect to migration database`

**Solutions:**
- Verify database credentials in config map
- Check network connectivity to database host
- Verify database name exists and is accessible

```bash
# Test connection manually
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -c "SELECT 1"
```


#### 2. Owner Validation Failures

**Error:** `Owner validation failed: Invalid owner_type 'ADMIN'. Must be one of: USER, GROUP`

**Solutions:**
- Check the `owner_type` in your configuration
- Ensure it's exactly `"USER"` or `"GROUP"` (case-sensitive)
- Fix configuration and restart migration

**Error:** `Owner validation failed: Owner user 'nonexistent_user' not found or is deleted`

**Solutions:**
- Verify the username exists in IOMETE IAM
- Check the user hasn't been deleted
- Ensure username matches exactly (case-sensitive)
- For groups, verify group name exists and is active

**Error:** `Owner validation failed: Owner group 'old_team' not found or is deleted`

**Solutions:**
- Verify the group exists in IOMETE IAM
- Check the group hasn't been deleted or archived
- Ensure group name matches exactly (case-sensitive)
- Create the group in IAM if it should exist

**Error:** `Owner validation failed: Database error while validating owner`

**Solutions:**
- Check database connectivity to IAM database
- Verify database permissions for reading iam_user/iam_group tables
- Check database logs for connection issues

#### 3. Duplicate Bundle Issues

**Error:** `Default bundle already exists for domain`

**Solutions:**
- Set appropriate `duplicate_bundle_action`
- Use `UPDATE` to refresh existing bundles
- Use `SKIP` for incremental migrations
- Use `FAIL` to investigate duplicates

#### 4. No Assets Found

**Warning:** `No compute assets found in domain xyz`

**Possible Causes:**
- Domain does not have any specified asset types (eg: No COMPUTE clusters are present in the domain)
- Domain name mismatch


#### 5. Permission Migration Issues

**Warning:** `Set permissions for 0 users in domain`

**Possible Causes:**
- No users have roles in the domain



1. **Check Logs First**
   - Enable debug mode for detailed output
   - Look for specific error messages
   - Check database connectivity

2. **Verify Configuration**
   - Validate CONFIG MAP syntax
   - Test database credentials separately
   - Confirm asset type mappings present in config map

3**Contact Support**
   - Include full error logs
   - Provide configuration (without credentials)
   - Describe expected vs actual behavior

## Prerequisites

- **Platform**: IOMETE workspace with Spark job capability
- **Python**: 3.12+
- **PySpark**: 3.5.5 (provided by IOMETE runtime)
- **Database**: PostgreSQL DB access with appropriate permissions
- **Permissions**: Read access and write access to database
- **Config Map database credentials**: Ensure database credentials are set properly in the config map
- **Config Map domain migration details**: **IMPORTANT** Ensure domains that need to be migrated are set in the migration > domains list with proper Owner and Owner Type set

## Security Notes
- Audit bundle creation and permission assignments after migration
- Test migrations in non-production environment first