# Namespace Migration - Quick Start Guide

## 5-Minute Setup

### Step 1: Set Environment Variable

```bash
export MIGRATION_TYPE=namespace
```

### Step 2: Configure Databases

Set database credentials:

```bash
export DB_USER=your_iam_user
export DB_PASSWORD=your_iam_password
export ASSET_DB_USER=your_asset_user
export ASSET_DB_PASSWORD=your_asset_password
```

### Step 3: Edit Configuration

Edit `application-namespace.conf` with your settings:

```hocon
migration: {
    migration_type: "namespace"

    # Add your domains
    domains: ["default", "production"]

    # Configure resource tables to scan
    resource_tables: [
        {
            table: "lakehouse"
            namespace_column: "lakehouse_namespace"
            user_columns: ["created_by", "updated_by", "owner"]
        },
        {
            table: "spark_job"
            namespace_column: "namespace"
            user_columns: ["created_by", "owner"]
        }
    ]

    # Permissions to grant
    namespace_permissions: ["USE"]
}
```

### Step 4: Test First (Dry Run)

Enable dry run mode in config:
```hocon
migration: {
    dry_run: true
    debug_mode: true
    # ... other settings
}
```

Run the migration:
```bash
python driver.py
```

Check the logs - no changes will be committed.

### Step 5: Run for Real

Disable dry run in config:
```hocon
migration: {
    dry_run: false
    debug_mode: false
    # ... other settings
}
```

Run the migration:
```bash
python driver.py
```

## What Happens?

1. **Discovers Users:** Scans your resource tables to find users with resources in each namespace
2. **Creates Bundles:** Creates one `{namespace}_Resource_bundle` per namespace
3. **Grants Permissions:** Gives namespace permissions to users who have resources there

## Example Output

```
INFO: Starting namespace migration for domain: default
INFO: Found 3 namespaces for domain default
INFO: Processing namespace: analytics in domain default
INFO: Total 5 unique users found for namespace analytics
INFO: Created namespace bundle for namespace analytics
INFO: Granted permissions to 5 users on namespace analytics
INFO: Successfully migrated namespace permissions for domain default
```

## Common Configurations

### Minimal Setup
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

### Production Setup
```hocon
migration: {
    migration_type: "namespace"
    domains: ["production"]
    dry_run: false
    debug_mode: false
    duplicate_bundle_action: "UPDATE"

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

    namespace_permissions: ["USE", "READ"]
}
```

### Development/Testing
```hocon
migration: {
    migration_type: "namespace"
    domains: ["dev"]
    dry_run: true           # Don't commit
    debug_mode: true        # Verbose logging

    resource_tables: [ ... ]
    namespace_permissions: ["USE"]
}
```

## Quick Commands

```bash
# Test without changes
export MIGRATION_TYPE=namespace
# Set dry_run: true in config
python driver.py

# Run for real
export MIGRATION_TYPE=namespace
# Set dry_run: false in config
python driver.py

# With Spark
spark-submit driver.py
```

## Troubleshooting

### "No users found"
✅ Check that resource tables exist and have data
✅ Verify namespace_column and user_columns match your schema
✅ Enable debug_mode to see queries

### "Namespace mapping not found"
✅ Ensure domain_namespace_mapping table is populated
✅ Verify namespaces exist before running migration

### "Bundle already exists"
✅ Change duplicate_bundle_action to "UPDATE" or "SKIP"
✅ This is normal if re-running migration

## Next Steps

- Read [CONFIGURATION_GUIDE.md](CONFIGURATION_GUIDE.md) for detailed options
- See [README.md](README.md) for architecture details
- Check [../../tests/README.md](../../tests/README.md) for testing guide
