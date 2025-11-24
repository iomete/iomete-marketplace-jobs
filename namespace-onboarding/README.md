# Namespace Onboarding Migration Job

A PySpark job that automates namespace permission assignment based on resource usage patterns in IOMETE. This job grants namespace permissions to users who have resources (lakehouse, spark jobs, jupyter containers) running in those namespaces.

## Purpose

This job solves the namespace permission migration problem by:
- **Deriving permissions from resource usage**: Users who have resources in a namespace automatically get USE permission
- **Querying resource tables**: Scans lakehouse, spark_job, and jupyter_container tables to find active users
- **Creating resource bundles**: Creates or updates resource bundles with namespace permissions

## How It Works

### Permission Derivation Logic

For each namespace in a domain, the job:

1. **Finds all namespaces** from `domain_namespace_mapping` table
2. **For each namespace**, queries resource tables to find users:
   - `lakehouse.created_by` where `namespace = X`
   - `spark_job.created_by` and `spark_job.job_user` where `namespace = X`
   - `jupyter_container.created_by` where `namespace = X`
3. **Grants USE permission** to those users on the namespace in the resource bundle

### Key Difference from RAS Onboarding

| Feature | RAS Onboarding | Namespace Onboarding |
|---------|----------------|---------------------|
| Permission Source | IAM roles | Resource usage |
| Query Pattern | Role mappings | Resource tables |
| Asset Types | Dynamic (COMPUTE, SPARK_JOB, etc.) | Fixed (NAMESPACE) |
| Purpose | Migrate existing assets to bundles | Grant namespace access based on usage |

---

## Prerequisites

- **IOMETE workspace** with Spark job capability
- **Python** 3.12+ (provided by runtime)
- **PySpark** 3.5.5 (provided by runtime)
- **Databases**: PostgreSQL access with read/write permissions
  - `bundle_db`: IAM database (bundle, bundle_permission tables)
  - `asset_db`: Core database (domain_namespace_mapping, lakehouse, spark_job, jupyter_container)

---

## Setup Instructions

### Step 1 – Job Details

In the IOMETE Console:

- Navigate to **Job Templates → New Job Template**
- Fill in:
  ```yaml
  Name: namespace-onboarding-migration
  Description: Namespace permission migration based on resource usage
  Kubernetes Namespace: <namespace>
  Run as User: <user>
  Application Type: Python
  ```

---

### Step 2 – Docker Image Configuration

Under **Image settings**:
```yaml
Image: iomete.azurecr.io/iomete/namespace-onboarding:1.0.0
Main Application File: local:///app/driver.py
```

---

### Step 3 – Config Map

1. Click **Add Config**
2. Set the file path to: `/etc/configs/application.conf`
3. Copy the below config and edit DB hosts, credentials, and domain details

```hocon
{
    # Database configuration - will be overridden by environment variables
    databases: {
      # Bundle database - where resource bundles and permissions are stored
      bundle_db: {
        host: "your-db-host"
        port: 5432
        name: "iomete_iam_db"
        user: ${?DB_USER}
        password: ${?DB_PASSWORD}
      }

      # Asset database - where namespaces and resources are stored
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
        # List of domains to migrate
        domains: ["default", "production", "staging"]

        # Resource tables to scan for namespace usage
        resource_tables: [
            {
                table: "lakehouse"
                namespace_column: "namespace"
                user_columns: ["created_by"]
            },
            {
                table: "spark_job"
                namespace_column: "namespace"
                user_columns: ["created_by", "job_user"]
            },
            {
                table: "jupyter_container"
                namespace_column: "namespace"
                user_columns: ["created_by"]
            }
        ]

        # Permissions to grant to users on namespaces they use
        namespace_permissions: ["USE"]

        # Transaction settings
        batch_size: 1000
        retry_attempts: 3

        # Validation settings
        validate_before_migration: true
        dry_run: false

        # Debug mode - enables detailed logging and query output
        debug_mode: false

        # Duplicate bundle behavior: FAIL, SKIP, or UPDATE
        duplicate_bundle_action: "UPDATE"
    }

    # Namespace table configuration
    namespace_config: {
        table: "domain_namespace_mapping"
        id_column: "id"
        namespace_column: "namespace"
        domain_column: "domain_id"
    }
}
```

---

### Step 4 – Instance Configuration

Choose the instance to run on:
- For migrations, a **single minimal instance** is sufficient

---

### Step 5 – Run Job

- **Save template**
- To run immediately:
  - Select template → **Applications tab** → **Run**

---

### Step 6 – Monitor Execution

- **Logs:** Check real-time logs in the IOMETE console
- **Verification:**
  - Ensure resource bundle is created/updated per domain
  - Confirm namespace permissions are assigned
  - Validate users have USE permission on namespaces where they have resources

Sample log output:
```
INFO: Starting Namespace Onboarding Migration Job
INFO: Starting namespace migration for domain: production
INFO: Found existing resource bundle abc-123 for domain production
INFO: Found 15 namespaces for domain production
INFO: Processing namespace: data-engineering (ID: ns-123)
INFO: Total 8 unique users found for namespace data-engineering in domain production
INFO: Granted permissions to 8 users on namespace ns-123 (errors: 0)
INFO: Successfully migrated namespace permissions for domain production
```

---

## Migration Logic Explained

### Phase 1: Bundle Management

For each domain:
1. **Check for resource bundle**: Queries bundle table for bundle named 'resource'
2. **Create or update**: Creates new bundle if missing, or updates existing based on `duplicate_bundle_action`
3. **Handle duplicates**: FAIL, SKIP, or UPDATE based on configuration

### Phase 2: Namespace Discovery

For each domain:
1. **Query namespaces**: Fetches all records from `domain_namespace_mapping` for the domain
2. **Process each namespace**: Iterates through namespaces to find users

### Phase 3: User Discovery (Resource-Based)

For each namespace:
1. **Query lakehouse**: Find users with `created_by` where `namespace = X` and `is_deleted = false`
2. **Query spark_job**: Find users from `created_by` and `job_user` columns
3. **Query jupyter_container**: Find users from `created_by` column
4. **Deduplicate**: Combine all users into a unique set

### Phase 4: Permission Assignment

For each namespace with users:
1. **Insert permissions**: Create bundle_permission records for each user
2. **Handle conflicts**: Uses `ON CONFLICT DO UPDATE` to merge permissions
3. **Grant USE permission**: Assigns configured permissions (default: USE)

---

## Duplicate Bundle Handling

### FAIL (Strict Mode)
```hocon
duplicate_bundle_action: "FAIL"
```
- Stops execution if resource bundle already exists
- Use for strict validation

### SKIP (Incremental Mode)
```hocon
duplicate_bundle_action: "SKIP"
```
- Skips domains with existing bundles
- Use for partial re-runs

### UPDATE (Merge Mode) - **Recommended**
```hocon
duplicate_bundle_action: "UPDATE"
```
- Updates existing bundle and reprocesses namespace permissions
- Merges new permissions with existing ones
- Safe for repeated executions

---

## Configuration Reference

### Resource Tables

Each resource table configuration requires:
- `table`: Table name (e.g., "lakehouse", "spark_job")
- `namespace_column`: Column linking to namespace (e.g., "namespace")
- `user_columns`: Array of user columns to check (e.g., ["created_by", "job_user"])

### Namespace Config

Defines the namespace table structure:
- `table`: Namespace mapping table (default: "domain_namespace_mapping")
- `id_column`: Primary key column (default: "id")
- `namespace_column`: Namespace name column (default: "namespace")
- `domain_column`: Domain reference column (default: "domain_id")

---

## Troubleshooting

### Database Issues
- **Connection failures** → Check DB credentials via `psql`
- **Table not found** → Verify table names match your schema
- **No namespaces found** → Check `domain_namespace_mapping` table has data

### Permission Issues
- **No users found for namespace** → Verify resources exist with that namespace
- **Permission insertion failures** → Check bundle_permission table schema
- **Duplicate key errors** → Use `UPDATE` mode for duplicate_bundle_action

### Resource Query Issues
- **Empty user lists** → Verify `is_deleted = false` filter is appropriate
- **Missing user columns** → Check resource tables have expected columns
- **NULL users being included** → Job filters out NULL values automatically

---

## Security Notes

- Always test in **non-production** first
- Review granted permissions after migration
- Use `dry_run: true` to validate without making changes
- Audit namespace access after migration completes

---

## Debug Mode

Enable detailed logging:
```hocon
migration: {
    debug_mode: true
}
```

Debug mode outputs:
- SQL queries for namespace and user discovery
- Query parameters for all database operations
- Row counts for each query result
- Detailed permission assignment operations

---

## Environment Variables

Override database configuration via environment variables:

**Bundle DB:**
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_SSL_MODE`

**Asset DB:**
- `ASSET_DB_HOST`
- `ASSET_DB_PORT`
- `ASSET_DB_NAME`
- `ASSET_DB_USER`
- `ASSET_DB_PASSWORD`
- `ASSET_DB_SSL_MODE`

---

## Dry Run Mode

Test migration without making changes:
```hocon
migration: {
    dry_run: true
}
```

In dry run mode:
- All queries execute normally
- Changes are rolled back instead of committed
- Logs show what would have been migrated
- Safe for testing configuration

---

## Comparison with RAS Onboarding

| Aspect | RAS Onboarding | Namespace Onboarding |
|--------|----------------|---------------------|
| **Purpose** | Migrate assets to bundles with role-based permissions | Grant namespace access based on resource usage |
| **Permission Source** | IAM roles and user_role_mapping | Resource tables (lakehouse, spark_job, jupyter_container) |
| **Asset Types** | Dynamic (COMPUTE, SPARK_JOB, JUPYTER_CONTAINER, etc.) | Fixed (NAMESPACE only) |
| **Bundle Type** | Creates 'default' bundles | Creates 'resource' bundles |
| **Query Pattern** | Joins with IAM role tables | Scans resource tables for users |
| **Configuration** | Complex asset_mappings with service definitions | Simple resource_tables array |
| **Use Case** | Initial RAS migration | Namespace permission derivation |

---

## FAQ

**Q: Why a separate job instead of extending RAS Onboarding?**
A: Different permission models (role-based vs. resource-based) require different query patterns. Separation keeps both jobs focused and maintainable.

**Q: What if a user has resources in multiple namespaces?**
A: They will get USE permission on all namespaces where they have resources.

**Q: What if a namespace has no active resources?**
A: No permissions are granted for that namespace (no users found = no permissions).

**Q: Can I add more resource tables?**
A: Yes! Add entries to `resource_tables` configuration with table name, namespace column, and user columns.

**Q: What happens if I run the job multiple times?**
A: With `UPDATE` mode, it merges permissions. Existing permissions are preserved and new ones are added.

**Q: Can I grant different permissions per namespace?**
A: Not currently. All users get the same permissions (configured in `namespace_permissions`). For custom permissions, modify the configuration.

---

## Support

For issues or questions:
- Check logs in IOMETE console
- Enable `debug_mode` for detailed query output
- Review this README's troubleshooting section
- Contact IOMETE support team
