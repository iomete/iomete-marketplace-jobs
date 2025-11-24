# Namespace Migration Configuration Guide

## Overview

The Namespace Migration tool grants namespace permissions to users based on their actual resource usage. It automatically discovers users who have resources in specific namespaces and grants them appropriate permissions.

## Configuration File

**File:** `application-namespace.conf`

This configuration is specifically designed for namespace-based permission migration.

## Quick Start

### 1. Set Environment Variable

```bash
export MIGRATION_TYPE=namespace
```

### 2. Configure Your Settings

Edit `application-namespace.conf`:

```hocon
migration: {
    migration_type: "namespace"
    domains: ["default", "production"]
    namespace_permissions: ["USE"]
}
```

### 3. Run Migration

```bash
python driver.py
```

## Configuration Structure

### Complete Example

```hocon
{
    # Database connections
    databases: {
      bundle_db: {
        host: "localhost"
        port: 5432
        name: "iomete_iam_db"
        user: ${?DB_USER}
        password: ${?DB_PASSWORD}
      }

      asset_db: {
        host: "localhost"
        port: 5432
        name: "iomete_core_db"
        user: ${?ASSET_DB_USER}
        password: ${?ASSET_DB_PASSWORD}
      }
    }

    # Migration settings
    migration: {
        # MUST be "namespace" for this config
        migration_type: "namespace"

        # List of domains to process
        domains: ["default", "production", "staging"]

        # Debug and safety settings
        debug_mode: false
        dry_run: false
        duplicate_bundle_action: "UPDATE"

        # Resource tables to scan for user-namespace relationships
        resource_tables: [
            {
                table: "lakehouse"
                namespace_column: "lakehouse_namespace"
                user_columns: ["created_by", "updated_by", "owner"]
            },
            {
                table: "spark_job"
                namespace_column: "namespace"
                user_columns: ["created_by", "updated_by", "owner"]
            },
            {
                table: "spark_connect"
                namespace_column: "namespace"
                user_columns: ["created_by", "updated_by"]
            }
        ]

        # Permissions to grant on namespaces
        namespace_permissions: ["USE"]
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

## Configuration Sections

### 1. Databases

```hocon
databases: {
  bundle_db: {
    host: "localhost"
    port: 5432
    name: "iomete_iam_db"
    user: ${?DB_USER}              # From environment
    password: ${?DB_PASSWORD}       # From environment
  }

  asset_db: {
    host: "localhost"
    port: 5432
    name: "iomete_core_db"
    user: ${?ASSET_DB_USER}
    password: ${?ASSET_DB_PASSWORD}
  }
}
```

**Purpose:**
- `bundle_db` - IAM database where bundles and permissions are stored
- `asset_db` - Resource database containing assets (lakehouses, jobs, etc.)

**Best Practice:** Use environment variables for credentials, never hardcode passwords.

### 2. Domain List

```hocon
# Option 1: Simple string list (recommended)
domains: ["default", "production", "staging"]

# Option 2: Domain objects (also supported)
domains: [
    { domain_id: "default" },
    { domain_id: "production" },
    { domain_id: "staging" }
]
```

**Purpose:** Specifies which domains to process. The migration will run for each domain.

### 3. Resource Tables

```hocon
resource_tables: [
    {
        table: "lakehouse"
        namespace_column: "lakehouse_namespace"
        user_columns: ["created_by", "updated_by", "owner"]
    },
    {
        table: "spark_job"
        namespace_column: "namespace"
        user_columns: ["created_by", "updated_by", "owner"]
    }
]
```

**Purpose:** Defines which tables to scan for user-namespace relationships.

**Fields:**
- `table` - Name of the resource table
- `namespace_column` - Column containing namespace identifier
- `user_columns` - Columns containing usernames (checked for each namespace)

**Example:** If user "alice" created a lakehouse in namespace "analytics", she'll get namespace permissions on "analytics".

### 4. Namespace Permissions

```hocon
namespace_permissions: ["USE"]
```

**Purpose:** List of permissions to grant users on their namespaces.

**Common Values:**
- `["USE"]` - Basic namespace access
- `["USE", "READ"]` - Read access in namespace
- `["USE", "READ", "WRITE"]` - Read and write access
- `["USE", "READ", "WRITE", "EXECUTE"]` - Full access

### 5. Namespace Config

```hocon
namespace_config: {
    table: "domain_namespace_mapping"
    id_column: "id"
    namespace_column: "namespace"
    domain_column: "domain_id"
}
```

**Purpose:** Defines the namespace mapping table structure.

**Fields:**
- `table` - Table containing namespace-to-domain mappings
- `id_column` - Primary key column (used for permissions)
- `namespace_column` - Column with namespace name
- `domain_column` - Column with domain identifier

### 6. Control Settings

```hocon
# Debug mode - detailed logging
debug_mode: false

# Dry run - test without committing
dry_run: false

# Duplicate bundle handling
duplicate_bundle_action: "UPDATE"  # FAIL, SKIP, or UPDATE
```

**debug_mode:**
- `false` - Normal logging (recommended for production)
- `true` - Verbose logging with SQL queries and parameters

**dry_run:**
- `false` - Commit changes (production mode)
- `true` - Rollback all changes (testing mode)

**duplicate_bundle_action:**
- `"FAIL"` - Stop if namespace bundle exists (safest)
- `"SKIP"` - Skip existing bundles, process new ones
- `"UPDATE"` - Update existing bundles with new permissions (recommended)

## How It Works

### Migration Flow

1. **For each domain:**
   - Query domain owner from asset database
   - Get all namespaces in the domain

2. **For each namespace:**
   - Create namespace-specific bundle (e.g., `analytics_Resource_bundle`)
   - Scan resource tables to find users with resources in this namespace
   - Grant namespace permissions to discovered users

3. **User Discovery:**
   - Queries each resource table
   - Checks all specified user_columns
   - Deduplicates users across tables
   - Filters out NULL/empty usernames

4. **Permission Assignment:**
   - Creates bundle_permission entries
   - Uses UPSERT (insert or update on conflict)
   - Continues even if individual permission fails

## Examples

### Example 1: Basic Configuration

```hocon
migration: {
    migration_type: "namespace"
    domains: ["default"]

    resource_tables: [
        {
            table: "lakehouse"
            namespace_column: "lakehouse_namespace"
            user_columns: ["created_by"]
        }
    ]

    namespace_permissions: ["USE"]
}
```

**Use Case:** Simple setup - grant USE permission to lakehouse creators.

### Example 2: Multiple Resource Types

```hocon
migration: {
    migration_type: "namespace"
    domains: ["analytics", "ml_workloads"]

    resource_tables: [
        {
            table: "lakehouse"
            namespace_column: "lakehouse_namespace"
            user_columns: ["created_by", "owner"]
        },
        {
            table: "spark_job"
            namespace_column: "namespace"
            user_columns: ["created_by", "owner", "job_user"]
        },
        {
            table: "jupyter_container"
            namespace_column: "namespace"
            user_columns: ["created_by"]
        }
    ]

    namespace_permissions: ["USE", "READ"]
}
```

**Use Case:** Grant permissions to users who created or own resources across multiple tables.

### Example 3: Testing with Dry Run

```hocon
migration: {
    migration_type: "namespace"
    domains: ["production"]

    debug_mode: true      # See everything
    dry_run: true         # Don't commit

    resource_tables: [ ... ]
    namespace_permissions: ["USE"]
}
```

**Use Case:** Test migration on production domain without making changes.

### Example 4: Incremental Updates

```hocon
migration: {
    migration_type: "namespace"
    domains: ["default", "production", "staging"]

    duplicate_bundle_action: "UPDATE"  # Update existing bundles

    resource_tables: [ ... ]
    namespace_permissions: ["USE", "READ", "WRITE"]
}
```

**Use Case:** Re-run migration to add new users or update permissions.

## Running the Migration

### Environment Setup

```bash
# Set migration type
export MIGRATION_TYPE=namespace

# Set database credentials
export DB_USER=iam_user
export DB_PASSWORD=secure_password
export ASSET_DB_USER=asset_user
export ASSET_DB_PASSWORD=secure_password
```

### Local Execution

```bash
python driver.py
```

### Spark Submit

```bash
spark-submit \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  driver.py
```

### Docker/Kubernetes

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: namespace-migration
spec:
  template:
    spec:
      containers:
      - name: migration
        image: ras-onboarding:latest
        env:
        - name: MIGRATION_TYPE
          value: "namespace"
        - name: DB_USER
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: username
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: password
```

## Best Practices

### 1. Always Test First

```hocon
migration: {
    dry_run: true
    debug_mode: true
}
```

Run with dry_run enabled to verify behavior before committing changes.

### 2. Include All User Columns

```hocon
resource_tables: [
    {
        table: "lakehouse"
        namespace_column: "lakehouse_namespace"
        user_columns: ["created_by", "updated_by", "owner", "shared_with"]
        # ↑ Include all columns where users might appear
    }
]
```

### 3. Use UPDATE for Iterative Development

```hocon
duplicate_bundle_action: "UPDATE"
```

Allows re-running migration to pick up new users.

### 4. Monitor Logs

```bash
# Watch for errors
python driver.py 2>&1 | grep -i error

# Count permissions granted
python driver.py 2>&1 | grep "Granted permissions"
```

### 5. Secure Credentials

```hocon
databases: {
  bundle_db: {
    user: ${?DB_USER}        # From environment only
    password: ${?DB_PASSWORD}
  }
}
```

Never commit credentials to version control.

## Troubleshooting

### Issue: No Users Found

**Symptom:** Migration succeeds but no permissions granted

**Solutions:**
1. Verify `resource_tables` configuration matches database schema
2. Check `namespace_column` and `user_columns` are correct
3. Ensure resources exist with `is_deleted = false`
4. Enable `debug_mode: true` to see queries

### Issue: Namespace Mapping Not Found

**Error:** `Namespace mapping not found for {namespace}`

**Solutions:**
1. Ensure `domain_namespace_mapping` table is populated
2. Verify `namespace_config` table name is correct
3. Check namespaces exist in the mapping table before migration

### Issue: Bundle Already Exists

**Error:** `Namespace bundle already exists`

**Solutions:**
1. Change `duplicate_bundle_action` to `"UPDATE"` or `"SKIP"`
2. If using FAIL mode, this is expected behavior

### Issue: Domain Has No Owner

**Error:** `Domain has no owners`

**Solutions:**
1. Verify domain exists in asset_db
2. Check domain.owners column is populated
3. Ensure owners field contains valid JSON array

### Issue: Permission Errors for Some Users

**Symptom:** Some users get permissions, others fail

**Solution:** This is expected behavior. Migration continues processing even if individual users fail. Check logs for specific error messages.

## Migration Outputs

### Success Messages

```
INFO: Starting namespace migration for domain: production
INFO: Found 3 namespaces for domain production
INFO: Processing namespace: analytics in domain production
INFO: Total 5 unique users found for namespace analytics
INFO: Created namespace bundle abc-123 for namespace analytics
INFO: Granted permissions to 5 users on namespace xyz-456
INFO: Successfully migrated namespace permissions for domain production
```

### What Gets Created

1. **Bundles** - One per namespace per domain
   - Name: `iomete-namespace-{namespace}`
   - Owner: First owner from domain.owners
   - Type: Regular bundle (not archived)

2. **Permissions** - One per user per namespace
   - Asset Type: `NAMESPACE`
   - Asset ID: Namespace mapping ID
   - Actor Type: `USER`
   - Actor ID: Username
   - Permissions: As configured in `namespace_permissions`

## Related Documentation

- [README.md](README.md) - Namespace migration overview
- [QUICK_START.md](QUICK_START.md) - Quick start guide
- [../../tests/README.md](../../tests/README.md) - Testing documentation

## Support

For issues or questions:
1. Check this guide and related documentation
2. Review logs with `debug_mode: true`
3. Test with `dry_run: true` first
4. Check test cases in `tests/namespace/` for examples
