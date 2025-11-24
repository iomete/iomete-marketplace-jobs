# Namespace Migration

Resource-based namespace permission migration for IOMETE. This module automatically grants namespace permissions to users based on their actual resource usage patterns.

## Overview

Unlike the asset migration (which uses IAM role-based permissions), namespace migration derives permissions by analyzing which users have active resources (lakehouse, spark jobs, jupyter containers) running in each namespace.

### How It Works

```
For each namespace in a domain:
  1. Query domain_namespace_mapping → Find all namespaces
  2. For each namespace:
     a. Query lakehouse table → Find users with created_by where namespace = X
     b. Query spark_job table → Find users from created_by and job_user where namespace = X
     c. Query jupyter_container table → Find users with created_by where namespace = X
  3. Deduplicate users
  4. Grant USE permission to those users on the namespace
```

## Key Differences from Asset Migration

| Aspect | Asset Migration | Namespace Migration |
|--------|----------------|---------------------|
| **Permission Source** | IAM roles (user_role_mapping_v2) | Resource tables (lakehouse, spark_job, etc.) |
| **Bundle Name** | `{domain}_default` | `resource` |
| **Query Pattern** | Joins with IAM tables | Scans resource tables |
| **Use Case** | Migrate existing assets to RAS | Grant namespace access based on usage |
| **Asset Types** | Dynamic (COMPUTE, SPARK_JOB, etc.) | Fixed (NAMESPACE only) |

## Configuration

### Minimal Configuration

```hocon
{
    databases: {
      bundle_db: {
        host: "your-bundle-db-host"
        port: 5432
        name: "iomete_iam_db"
        user: ${?DB_USER}
        password: ${?DB_PASSWORD}
      }

      asset_db: {
        host: "your-asset-db-host"
        port: 5432
        name: "iomete_core_db"
        user: ${?ASSET_DB_USER}
        password: ${?ASSET_DB_PASSWORD}
      }
    }

    migration: {
        # IMPORTANT: Set migration type to namespace
        migration_type: "namespace"

        # Simple list of domains to migrate
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
                user_columns: ["created_by", "job_user"]  # job_user is run_as_user
            },
            {
                table: "jupyter_container"
                namespace_column: "namespace"
                user_columns: ["created_by"]
            }
        ]

        # Permissions to grant on namespaces
        namespace_permissions: ["USE"]

        # Duplicate bundle behavior
        duplicate_bundle_action: "UPDATE"  # or FAIL, SKIP

        # Debug and dry run options
        debug_mode: false
        dry_run: false
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

## Resource Tables Configuration

### Adding New Resource Tables

To scan additional resource types for namespace usage:

```hocon
resource_tables: [
    # Existing tables
    { table: "lakehouse", namespace_column: "namespace", user_columns: ["created_by"] },
    { table: "spark_job", namespace_column: "namespace", user_columns: ["created_by", "job_user"] },
    { table: "jupyter_container", namespace_column: "namespace", user_columns: ["created_by"] },

    # Add new resource types
    {
        table: "new_resource_type"
        namespace_column: "k8s_namespace"  # Column linking to namespace
        user_columns: ["owner", "executor"]  # User columns to check
    }
]
```

### Table Requirements

Each resource table must have:
- ✅ A namespace column (linking to namespace name)
- ✅ A `domain` column (for filtering by domain)
- ✅ An `is_deleted` column (for filtering active resources)
- ✅ One or more user columns (created_by, job_user, owner, etc.)

## Permission Configuration

### Default Permission

```hocon
namespace_permissions: ["USE"]
```

This grants users the ability to use namespaces where they have resources.

### Multiple Permissions

```hocon
namespace_permissions: ["USE", "VIEW", "MANAGE"]
```

Grants multiple permissions to all users who have resources in the namespace.

## Migration Flow

### Phase 1: Bundle Management

```
For each domain:
  1. Check if 'resource' bundle exists
  2. If exists:
     - FAIL mode: Stop execution
     - SKIP mode: Skip this domain
     - UPDATE mode: Continue with existing bundle
  3. If not exists: Create 'resource' bundle
```

### Phase 2: Namespace Discovery

```
For each domain:
  1. Query domain_namespace_mapping table
  2. Get all namespaces for the domain
  3. If no namespaces found: Log warning and continue
```

### Phase 3: User Discovery (Resource-Based)

```
For each namespace:
  1. Query lakehouse table:
     SELECT DISTINCT created_by WHERE namespace = X AND domain = Y AND is_deleted = false

  2. Query spark_job table:
     SELECT DISTINCT created_by WHERE namespace = X AND domain = Y AND is_deleted = false
     UNION
     SELECT DISTINCT job_user WHERE namespace = X AND domain = Y AND is_deleted = false

  3. Query jupyter_container table:
     SELECT DISTINCT created_by WHERE namespace = X AND domain = Y AND is_deleted = false

  4. Combine and deduplicate all users
  5. Filter out NULL values
```

### Phase 4: Permission Assignment

```
For each user in the namespace:
  1. Insert into bundle_permission:
     - bundle_id: Resource bundle ID
     - asset_type: NAMESPACE
     - asset_id: namespace.id (from domain_namespace_mapping)
     - actor_type: USER
     - actor_id: username
     - permissions: ["USE"]

  2. Use ON CONFLICT DO UPDATE to handle duplicates
```

## Example Scenarios

### Scenario 1: User with Multiple Resources

```
User: john.doe
Resources:
  - Lakehouse "analytics-lh" in namespace "data-engineering"
  - Spark Job "etl-job" in namespace "data-engineering"
  - Jupyter Container "notebook-1" in namespace "data-science"

Result:
  john.doe gets USE permission on:
    - namespace "data-engineering"
    - namespace "data-science"
```

### Scenario 2: Spark Job with run_as_user

```
Spark Job: "daily-etl"
  - created_by: alice
  - job_user: bob  (run_as_user)
  - namespace: "production"

Result:
  Both alice and bob get USE permission on namespace "production"
```

### Scenario 3: Namespace with No Active Resources

```
Namespace: "archived-project"
Active Resources: 0

Result:
  No permissions granted (no users found)
```

## Duplicate Bundle Handling

### FAIL Mode (Strict)

```hocon
duplicate_bundle_action: "FAIL"
```

- Stops execution if resource bundle already exists
- Use for first-time migrations or strict validation

**Log Output:**
```
ERROR: Resource bundle already exists for domain production. Use UPDATE or SKIP mode.
```

### SKIP Mode (Incremental)

```hocon
duplicate_bundle_action: "SKIP"
```

- Skips domains with existing resource bundles
- Use for partial re-runs or testing

**Log Output:**
```
INFO: Found existing resource bundle abc-123 for domain production
INFO: Skipping migration for domain production - bundle already exists
```

### UPDATE Mode (Merge) - **Recommended**

```hocon
duplicate_bundle_action: "UPDATE"
```

- Updates existing bundle and reprocesses namespace permissions
- Merges new permissions with existing ones
- Safe for repeated executions

**Log Output:**
```
INFO: Found existing resource bundle abc-123 for domain production
INFO: Found 15 namespaces for domain production
INFO: Processing namespace: data-engineering (ID: ns-456)
INFO: Total 8 unique users found for namespace data-engineering
INFO: Granted permissions to 8 users on namespace ns-456
```

## Dry Run Mode

Test migration without making changes:

```hocon
migration: {
    dry_run: true
}
```

**Behavior:**
- Executes all queries normally
- Processes namespace and user discovery
- **Rolls back** transaction instead of committing
- Shows what would be migrated

**Log Output:**
```
INFO: Starting namespace migration for domain: production
WARNING: DRY RUN MODE - No changes will be committed
INFO: Found 15 namespaces for domain production
INFO: Total 8 unique users found for namespace data-engineering
INFO: Granted permissions to 8 users on namespace ns-456 (errors: 0)
INFO: DRY RUN: Changes rolled back for domain production
```

## Debug Mode

Enable detailed logging:

```hocon
migration: {
    debug_mode: true
}
```

**Debug Output Includes:**
- SQL queries with full syntax
- Query parameters for all operations
- Row counts from each query
- Per-table user discovery results

**Example Debug Output:**
```
DEBUG: Namespace query: SELECT id, namespace, domain_id FROM domain_namespace_mapping WHERE domain_id = %s
DEBUG: Parameters: ('production',)
DEBUG: Query returned 15 rows

DEBUG: User query for lakehouse: SELECT DISTINCT created_by as username FROM lakehouse WHERE namespace = %s AND domain = %s AND is_deleted = false AND created_by IS NOT NULL
DEBUG: Parameters: ('data-engineering', 'production')
DEBUG: Found 5 users in lakehouse for namespace data-engineering

DEBUG: User query for spark_job: SELECT DISTINCT created_by as username ... UNION SELECT DISTINCT job_user ...
DEBUG: Parameters: ('data-engineering', 'production', 'data-engineering', 'production')
DEBUG: Found 3 users in spark_job for namespace data-engineering

DEBUG: Granting ['USE'] to user john.doe on namespace ns-456
```

## Troubleshooting

### No Namespaces Found

**Issue:**
```
WARNING: No namespaces found for domain production
```

**Solutions:**
1. Verify `namespace_config.table` is correct
2. Check `namespace_config.domain_column` matches your schema
3. Ensure domain_namespace_mapping has data for this domain

**Debug:**
```sql
SELECT * FROM domain_namespace_mapping WHERE domain_id = 'production';
```

---

### No Users Found for Namespace

**Issue:**
```
INFO: Total 0 unique users found for namespace data-engineering
INFO: No users to grant permissions for namespace ns-123
```

**Solutions:**
1. Verify resource tables have data with this namespace
2. Check `namespace_column` name is correct
3. Ensure `is_deleted = false` filter is appropriate
4. Check user columns exist and have non-NULL values

**Debug:**
```sql
-- Check lakehouse
SELECT created_by FROM lakehouse
WHERE namespace = 'data-engineering' AND domain = 'production' AND is_deleted = false;

-- Check spark_job
SELECT created_by, job_user FROM spark_job
WHERE namespace = 'data-engineering' AND domain = 'production' AND is_deleted = false;
```

---

### Permission Insertion Failures

**Issue:**
```
ERROR: Error granting permissions to user john.doe on namespace ns-456: ...
INFO: Granted permissions to 5 users on namespace ns-456 (errors: 3)
```

**Solutions:**
1. Check bundle_permission table schema
2. Verify user exists in iam_user table
3. Check namespace ID exists
4. Review database constraints

**Debug:**
```sql
-- Verify user exists
SELECT username FROM iam_user WHERE username = 'john.doe';

-- Check bundle exists
SELECT id FROM bundle WHERE name = 'resource' AND domain = 'production';

-- Test manual insert
INSERT INTO bundle_permission
(bundle_id, asset_type, asset_id, actor_type, actor_id, permissions, created_at, created_by, updated_at, updated_by)
VALUES ('bundle-id', 'NAMESPACE', 'ns-456', 'USER', 'john.doe', ARRAY['USE'], current_timestamp, 'system', current_timestamp, 'system');
```

---

### Table Not Found

**Issue:**
```
ERROR: Error querying users from lakehouse for namespace data-engineering: relation "lakehouse" does not exist
```

**Solutions:**
1. Verify table name in `resource_tables` configuration
2. Check database connection (asset_db)
3. Ensure schema is correct

**Debug:**
```sql
-- List tables in asset database
SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';
```

---

### Column Not Found

**Issue:**
```
ERROR: Error querying users from spark_job: column "job_user" does not exist
```

**Solutions:**
1. Verify column names in `user_columns` configuration
2. Check actual schema of the table

**Debug:**
```sql
-- List columns in spark_job table
SELECT column_name, data_type FROM information_schema.columns
WHERE table_name = 'spark_job';
```

---

### Bundle Already Exists

**Issue:**
```
ERROR: Resource bundle already exists for domain production. Use UPDATE or SKIP mode.
```

**Solutions:**
1. Change to `duplicate_bundle_action: "UPDATE"` to merge permissions
2. Use `duplicate_bundle_action: "SKIP"` to skip this domain
3. Manually delete the bundle if needed (not recommended)

---

## Monitoring and Logging

### Success Logs

```
INFO: Starting RAS Onboarding Migration Job - Type: namespace
INFO: Bundle DB connection successful
INFO: Asset DB connection successful
INFO: Running namespace migration (resource-based permissions)
INFO: Starting namespace migration for 2 domain(s)

INFO: Starting namespace migration for domain: production
INFO: Found existing resource bundle abc-123 for domain production
INFO: Found 15 namespaces for domain production

INFO: Processing namespace: data-engineering (ID: ns-456)
INFO: Total 8 unique users found for namespace data-engineering in domain production
INFO: Granted permissions to 8 users on namespace ns-456 (errors: 0)

INFO: Processing namespace: data-science (ID: ns-789)
INFO: Total 5 unique users found for namespace data-science in domain production
INFO: Granted permissions to 5 users on namespace ns-789 (errors: 0)

INFO: Successfully migrated namespace permissions for domain production
INFO: Migration completed: 2 succeeded, 0 failed
INFO: Migration completed successfully
```

### Performance Metrics

The logs show:
- **Total domains processed**
- **Namespaces per domain**
- **Users per namespace**
- **Success/error counts**
- **Transaction timing**

Track these to monitor:
- Migration completeness
- User coverage
- Error patterns
- Performance issues

---

## Best Practices

### 1. Start with Dry Run

```hocon
migration: {
    dry_run: true
    debug_mode: true
}
```

Always test first to validate:
- Configuration is correct
- Expected namespaces are found
- Expected users are discovered
- No unexpected errors

### 2. Use UPDATE Mode for Production

```hocon
duplicate_bundle_action: "UPDATE"
```

Benefits:
- Safe for repeated executions
- Merges permissions automatically
- Handles new resources gracefully
- No manual cleanup needed

### 3. Monitor Resource Tables

Regularly check that:
- `is_deleted` flag is correctly maintained
- User columns contain valid usernames
- Namespace column values match domain_namespace_mapping

### 4. Incremental Migration

For large environments:

```hocon
# Week 1: Production only
domains: ["production"]

# Week 2: Add staging
domains: ["production", "staging"]

# Week 3: Add all
domains: ["production", "staging", "dev", "qa"]
```

### 5. Validate After Migration

```sql
-- Check permissions granted
SELECT
    bp.actor_id as username,
    dnm.namespace,
    bp.permissions
FROM bundle_permission bp
JOIN domain_namespace_mapping dnm ON bp.asset_id = dnm.id
WHERE bp.asset_type = 'NAMESPACE'
    AND bp.bundle_id = (SELECT id FROM bundle WHERE name = 'resource' AND domain = 'production')
ORDER BY dnm.namespace, bp.actor_id;
```

---

## FAQ

**Q: What happens if a user is deleted from a resource but still has permission?**

A: The migration only adds permissions based on current active resources. To remove stale permissions, you'll need a cleanup job or set `duplicate_bundle_action: "RESET"` (not recommended for incremental updates).

**Q: Can I grant different permissions per namespace?**

A: Not directly. All users get the same permissions defined in `namespace_permissions`. For custom permissions, you'd need to modify the code or run separate migrations with different configs.

**Q: What if a user has resources in multiple namespaces?**

A: They'll get USE permission on all namespaces where they have resources. This is the expected behavior.

**Q: How often should I run this migration?**

A: Depends on your needs:
- **One-time:** Initial setup only
- **Scheduled:** Daily/weekly to sync new resources
- **On-demand:** After major resource creation events

**Q: Can I run asset and namespace migrations together?**

A: Not simultaneously. Run them separately by changing `migration_type` in the config. They create different bundles (`default` vs `resource`) so they don't conflict.

**Q: What's the performance impact?**

A: Depends on:
- Number of namespaces per domain
- Number of resources per namespace
- Database query performance

For 1000s of namespaces, consider:
- Batch processing (one domain at a time)
- Database indexes on namespace and domain columns
- Running during off-peak hours

---

## Schema Requirements

### domain_namespace_mapping Table

```sql
CREATE TABLE domain_namespace_mapping (
    id          CHAR(36) PRIMARY KEY,
    domain_id   VARCHAR(58) NOT NULL,
    namespace   VARCHAR(255) NOT NULL
);

-- Recommended indexes
CREATE INDEX idx_dnm_domain ON domain_namespace_mapping(domain_id);
CREATE INDEX idx_dnm_namespace ON domain_namespace_mapping(namespace);
```

### Resource Tables (lakehouse, spark_job, jupyter_container)

Required columns:
- `namespace` (VARCHAR) - Links to domain_namespace_mapping.namespace
- `domain` (VARCHAR) - Domain identifier
- `is_deleted` (BOOLEAN) - Soft delete flag
- User columns: `created_by`, `job_user`, etc. (VARCHAR)

**Recommended indexes:**
```sql
CREATE INDEX idx_lakehouse_namespace_domain ON lakehouse(namespace, domain) WHERE is_deleted = false;
CREATE INDEX idx_spark_job_namespace_domain ON spark_job(namespace, domain) WHERE is_deleted = false;
CREATE INDEX idx_jupyter_container_namespace_domain ON jupyter_container(namespace, domain) WHERE is_deleted = false;
```

### bundle_permission Table

```sql
CREATE TABLE bundle_permission (
    bundle_id    UUID NOT NULL,
    asset_type   VARCHAR(50) NOT NULL,
    asset_id     CHAR(36) NOT NULL,
    actor_type   VARCHAR(10) NOT NULL,
    actor_id     VARCHAR(255) NOT NULL,
    permissions  TEXT[] NOT NULL,
    created_at   TIMESTAMP,
    created_by   VARCHAR(255),
    updated_at   TIMESTAMP,
    updated_by   VARCHAR(255),

    PRIMARY KEY (bundle_id, asset_type, asset_id, actor_type, actor_id)
);
```

---

## Support

For issues or questions:
1. Enable `debug_mode: true` and review detailed logs
2. Check this troubleshooting section
3. Verify database schema matches requirements
4. Contact IOMETE support team

---

## Version History

- **v1.0.0** - Initial namespace migration implementation
  - Resource-based permission derivation
  - Support for lakehouse, spark_job, jupyter_container
  - Configurable resource tables and permissions
