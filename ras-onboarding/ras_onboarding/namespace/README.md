# Namespace Migration Module

## Overview

The Namespace Migration module automatically grants namespace permissions to users based on their resource usage. It scans resource tables (lakehouses, spark jobs, etc.) to discover which users have resources in each namespace, then creates namespace-specific bundles and grants appropriate permissions.

## Key Features

- 🔍 **Automatic User Discovery** - Finds users from resource tables automatically
- 📦 **Namespace Bundles** - Creates one bundle per namespace per domain
- 🔐 **Resource-Based Permissions** - Grants permissions based on actual usage
- ⚡ **Efficient Processing** - Handles multiple domains and namespaces
- 🛡️ **Safe Operations** - Supports dry-run and duplicate handling
- 📊 **Comprehensive Logging** - Debug mode for detailed insights

## How It Works

### 1. User Discovery
Scans configured resource tables to find users who have resources in each namespace:
```
lakehouse table → users with lakehouses in "analytics" namespace
spark_job table → users with jobs in "analytics" namespace
→ Deduplicated list of users for "analytics"
```

### 2. Bundle Creation
Creates namespace-specific resource bundles:
```
Namespace: "analytics"
→ Bundle: "analytics_Resource_bundle"
→ Owner: Domain owner (from domain.owners)
```

### 3. Permission Assignment
Grants namespace permissions to discovered users:
```
User "alice" has lakehouse in "analytics"
→ Grant ["USE"] permission on namespace "analytics" to "alice"
```

## Quick Start

### 1. Set Environment Variable
```bash
export MIGRATION_TYPE=namespace
```

### 2. Configure Settings
Edit `application-namespace.conf`:
```hocon
migration: {
    migration_type: "namespace"
    domains: ["default"]
    resource_tables: [
        {
            table: "lakehouse"
            namespace_column: "lakehouse_namespace"
            user_columns: ["created_by", "owner"]
        }
    ]
    namespace_permissions: ["USE"]
}
```

### 3. Run Migration
```bash
python driver.py
```

## Module Structure

```
namespace/
├── __init__.py               # Module exports
├── migration.py              # Main migration orchestration
├── permission_assignment.py  # Permission logic
├── queries.py                # SQL query definitions
├── README.md                 # This file
├── QUICK_START.md           # Quick start guide
└── CONFIGURATION_GUIDE.md   # Detailed configuration reference
```

## Key Components

### `NamespaceMigration`
Main orchestration class that coordinates the migration process.

**Key Methods:**
- `migrate_domain(domain_id)` - Migrate a single domain
- `run_migration()` - Migrate all configured domains
- `get_namespace_mapping_id()` - Lookup namespace mapping
- `get_domain_owner()` - Get domain owner for bundle creation
- `get_or_create_namespace_bundle()` - Manage namespace bundles
- `get_namespaces_for_domain()` - Query namespaces in a domain

### `PermissionAssignment`
Handles user discovery and permission granting.

**Key Methods:**
- `get_users_for_namespace()` - Find users with resources in a namespace
- `set_namespace_permissions()` - Grant permissions to users

### SQL Queries (`queries.py`)
Pre-defined SQL queries for:
- Namespace mapping lookup
- Bundle creation
- Permission assignment (with upsert)
- Domain and namespace queries

## Configuration

See [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md) for comprehensive configuration documentation.

### Minimal Configuration

```hocon
{
    databases: {
      bundle_db: { ... }
      asset_db: { ... }
    }

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

    namespace_config: {
        table: "domain_namespace_mapping"
        id_column: "id"
        namespace_column: "namespace"
        domain_column: "domain_id"
    }
}
```

## Usage Examples

### Example 1: Basic Migration
```bash
export MIGRATION_TYPE=namespace
python driver.py
```

### Example 2: Dry Run (Test Mode)
Edit `application-namespace.conf`:
```hocon
migration: {
    dry_run: true
    debug_mode: true
    # ... other settings
}
```

### Example 3: Update Existing Bundles
```hocon
migration: {
    duplicate_bundle_action: "UPDATE"
    # ... other settings
}
```

## Migration Flow

```
Start Migration
    ↓
For each domain:
    ├─ Get domain owner
    ├─ Get all namespaces in domain
    ├─ For each namespace:
    │   ├─ Get namespace mapping ID
    │   ├─ Create/get namespace bundle
    │   ├─ Scan resource tables for users
    │   └─ Grant permissions to users
    └─ Commit transaction
    ↓
End Migration
```

## Database Operations

### Tables Read
- `domain` - Get domain owners
- `domain_namespace_mapping` - Namespace mappings
- `lakehouse`, `spark_job`, etc. - Find users with resources

### Tables Written
- `bundle` - Create namespace bundles
- `bundle_permission` - Grant permissions to users

### Transaction Management
- Uses database transactions for safety
- Rolls back on errors
- Supports dry-run mode (rollback without errors)

## Error Handling

The migration is designed to be resilient:

- ✅ Continues if one namespace fails
- ✅ Continues if one user permission fails
- ✅ Continues if one resource table query fails
- ✅ Logs all errors for investigation
- ✅ Returns overall success/failure status

## Testing

Comprehensive test suite available in `tests/namespace/`:
- 64+ test functions
- Unit and integration tests
- Mock-based (no database required)

```bash
# Run namespace tests
pytest tests/namespace/ -v

# Run with coverage
pytest tests/namespace/ --cov=ras_onboarding.namespace
```

See [tests documentation](../../tests/README.md) for details.

## Common Scenarios

### Scenario 1: Initial Setup
First time granting namespace permissions to existing users.
```hocon
duplicate_bundle_action: "FAIL"  # Safe - will error if bundles exist
```

### Scenario 2: Adding New Users
Re-run migration to pick up new users who created resources.
```hocon
duplicate_bundle_action: "UPDATE"  # Updates existing bundles
```

### Scenario 3: Testing Changes
Test configuration changes without affecting database.
```hocon
dry_run: true
debug_mode: true
```

## Performance Considerations

- **Batch Processing:** Processes namespaces sequentially within each domain
- **Query Optimization:** Uses UNION queries for multiple user columns
- **User Deduplication:** Efficiently deduplicates users across tables
- **Transaction Scope:** One transaction per domain for safety

## Troubleshooting

### No Users Found
- Check `resource_tables` configuration
- Verify namespace_column names match database
- Ensure resources exist with `is_deleted = false`
- Enable debug_mode to see queries

### Bundle Already Exists
- Change `duplicate_bundle_action` to "UPDATE" or "SKIP"
- Or delete existing bundles if starting fresh

### Namespace Mapping Not Found
- Ensure `domain_namespace_mapping` table is populated
- Verify namespace names match between tables

### Permission Errors
- Check bundle_db connection and credentials
- Verify user exists in IAM database
- Review error logs for specific issues

## Best Practices

1. **Always test first:** Use `dry_run: true` before production
2. **Enable debug logging:** Set `debug_mode: true` during development
3. **Include all user columns:** Check all columns where users might appear
4. **Use UPDATE mode:** For iterative development and re-runs
5. **Monitor logs:** Watch for errors and permission grants
6. **Secure credentials:** Use environment variables, never hardcode

## Related Documentation

- [QUICK_START.md](QUICK_START.md) - Quick start guide
- [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md) - Detailed configuration
- [../../tests/README.md](../../tests/README.md) - Testing guide

## Support

For issues:
1. Check [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md)
2. Enable `debug_mode: true` and review logs
3. Run with `dry_run: true` to test safely
4. Check test cases in `tests/namespace/` for examples
