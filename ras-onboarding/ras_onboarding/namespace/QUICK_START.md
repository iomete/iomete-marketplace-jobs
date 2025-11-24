# Namespace Migration - Quick Start

## 5-Minute Setup

### 1. Set Migration Type

Edit `application.conf`:

```hocon
migration: {
    migration_type: "namespace"  # ← Change this!
}
```

### 2. Configure Domains

Replace complex domain config with simple list:

```hocon
# Before (asset migration):
domains: [
    {
        domain_id: "production"
        owner_id: "admin"
        owner_type: "USER"
        asset_types: ["COMPUTE"]
    }
]

# After (namespace migration):
domains: ["production", "staging", "dev"]  # ← Just domain IDs!
```

### 3. Verify Resource Tables

Default config scans these tables:

```hocon
resource_tables: [
    { table: "lakehouse", namespace_column: "namespace", user_columns: ["created_by"] },
    { table: "spark_job", namespace_column: "namespace", user_columns: ["created_by", "job_user"] },
    { table: "jupyter_container", namespace_column: "namespace", user_columns: ["created_by"] }
]
```

✅ **No changes needed** if your tables match this structure.

### 4. Run Dry Run

Test without making changes:

```hocon
migration: {
    migration_type: "namespace"
    dry_run: true
    debug_mode: true
}
```

Check logs to verify:
- ✅ Namespaces are found
- ✅ Users are discovered
- ✅ No errors

### 5. Run For Real

```hocon
migration: {
    migration_type: "namespace"
    dry_run: false
    duplicate_bundle_action: "UPDATE"
}
```

Deploy and run the job!

---

## Common Configurations

### Default Configuration (Most Common)

```hocon
{
    databases: { /* your DB config */ }

    migration: {
        migration_type: "namespace"
        domains: ["production", "staging"]

        resource_tables: [
            { table: "lakehouse", namespace_column: "namespace", user_columns: ["created_by"] },
            { table: "spark_job", namespace_column: "namespace", user_columns: ["created_by", "job_user"] },
            { table: "jupyter_container", namespace_column: "namespace", user_columns: ["created_by"] }
        ]

        namespace_permissions: ["USE"]
        duplicate_bundle_action: "UPDATE"
        dry_run: false
        debug_mode: false
    }

    namespace_config: {
        table: "domain_namespace_mapping"
        id_column: "id"
        namespace_column: "namespace"
        domain_column: "domain_id"
    }
}
```

### Custom Table Names

If your tables have different names:

```hocon
resource_tables: [
    { table: "compute_clusters", namespace_column: "k8s_namespace", user_columns: ["owner"] },
    { table: "jobs", namespace_column: "k8s_namespace", user_columns: ["creator", "runner"] },
    { table: "notebooks", namespace_column: "k8s_namespace", user_columns: ["user_id"] }
]

namespace_config: {
    table: "namespace_domain_map"  # ← Your table name
    id_column: "uuid"
    namespace_column: "ns_name"
    domain_column: "domain"
}
```

### Multiple Permissions

Grant more than just USE:

```hocon
namespace_permissions: ["USE", "VIEW", "MANAGE"]
```

---

## Verification

After migration, check results:

```sql
-- See all namespace permissions granted
SELECT
    b.domain,
    dnm.namespace,
    bp.actor_id as username,
    bp.permissions
FROM bundle b
JOIN bundle_permission bp ON b.id = bp.bundle_id
JOIN domain_namespace_mapping dnm ON bp.asset_id = dnm.id
WHERE b.name = 'resource'
    AND bp.asset_type = 'NAMESPACE'
ORDER BY b.domain, dnm.namespace, bp.actor_id;
```

Expected results:
```
domain     | namespace         | username  | permissions
-----------|-------------------|-----------|------------
production | data-engineering  | alice     | {USE}
production | data-engineering  | bob       | {USE}
production | data-science      | carol     | {USE}
```

---

## Troubleshooting Quick Fixes

| Issue | Quick Fix |
|-------|-----------|
| No namespaces found | Check `namespace_config.table` and `domain_column` |
| No users found | Verify `resource_tables` table names and column names |
| Bundle already exists | Change to `duplicate_bundle_action: "UPDATE"` |
| Permission errors | Check user exists in `iam_user` table |
| Table not found | Verify table names in `resource_tables` config |

---

## When to Use Namespace Migration

✅ **Use namespace migration when:**
- You want permissions based on actual resource usage
- Users should access namespaces where they have resources
- No IAM roles are configured yet
- You need automatic permission derivation

❌ **Use asset migration instead when:**
- You have IAM roles defined
- You want role-based permissions (viewer, editor, admin)
- You're migrating assets (COMPUTE, SPARK_JOB) to bundles

---

## Migration Flow Summary

```
1. Read config → migration_type = "namespace"
2. Connect to bundle_db and asset_db
3. For each domain:
   a. Get/create 'resource' bundle
   b. Query domain_namespace_mapping → find namespaces
   c. For each namespace:
      - Query lakehouse, spark_job, jupyter_container
      - Find users with resources in this namespace
      - Grant USE permission to those users
4. Commit or rollback (based on dry_run)
```

---

## Need Help?

1. **Detailed docs:** See [README.md](./README.md)
2. **Debug mode:** Set `debug_mode: true` for detailed logs
3. **Dry run:** Test with `dry_run: true` first
4. **Support:** Contact IOMETE support team
