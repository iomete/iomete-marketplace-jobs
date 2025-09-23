# Asset RAS Onboarding Migration Job

A PySpark job that automates the asset RAS (Resource Access Security) onboarding migration process for the IOMETE platform. This job migrates domain assets (compute, pipelines, datasets, etc.) to bundles with proper permissions based on existing role mappings.

## Overview

This job performs the following migration steps for each configured domain:

1. **Create Default Bundle**: Creates a default bundle for the domain
2. **Move Assets**: Moves all assets of the specified type from the domain to the new bundle
3. **Set Permissions**: Configures user and group permissions based on existing role mappings

All operations are performed within database transactions to ensure data consistency.

## Supported Asset Types

The job supports multiple asset types through configurable mappings:

- **COMPUTE**: Lakehouse compute resources
- **Additional Asset Types**: Easily extensible via configuration

## Configuration

### Asset Type Mappings

Configure how different asset types are queried in `application.conf`:

```hocon
asset_mappings: {
    COMPUTE: {
        table: "lakehouse"
        id_column: "id"
        domain_column: "domain"
        filter_condition: "is_deleted = false"
    }
    PIPELINE: {
        table: "pipeline"
        id_column: "id"
        domain_column: "domain"
        filter_condition: "is_deleted = false"
    }
    DATASET: {
        table: "dataset"
        id_column: "id"
        domain_column: "domain"
        filter_condition: "is_deleted = false"
    }
}
```

### Database Configuration

Database credentials are configured via environment variables:

```bash
export DB_HOST="your-db-host"
export DB_PORT="5432"
export DB_NAME="iomete_core_db"
export DB_USER="your-username"
export DB_PASSWORD="your-password"
export DB_SSL_MODE="require"
```

### Migration Configuration

Configure domains to migrate in `application.conf`:

```hocon
{
    migration: {
        domains: [
            {
                domain_id: "production_domain"
                owner_id: "admin_user"
                owner_type: "USER"  # USER or GROUP
                asset_type: "COMPUTE"
            },
            {
                domain_id: "staging_domain"
                owner_id: "data_team"
                owner_type: "GROUP"
                asset_type: "PIPELINE"
            },
            {
                domain_id: "analytics_domain"
                owner_id: "analytics_group"
                owner_type: "GROUP"
                asset_type: "DATASET"
            }
        ]

        # Optional settings
        validate_before_migration: true
        dry_run: false

        # Duplicate bundle behavior: FAIL, SKIP, or UPDATE
        duplicate_bundle_action: "FAIL"
    }
}
```

## Duplicate Bundle Handling

When a default bundle already exists for a domain, the job behavior is controlled by the `duplicate_bundle_action` configuration:

### FAIL (Default)
- **Behavior**: Stop execution and fail the migration for that domain
- **Use Case**: Strict mode where duplicate bundles should not exist
- **Result**: Migration fails with error message

```hocon
duplicate_bundle_action: "FAIL"
```

### SKIP
- **Behavior**: Skip migration for domains with existing bundles
- **Use Case**: Incremental migrations where some domains may already be processed
- **Result**: Migration succeeds but skips the domain, logs warning

```hocon
duplicate_bundle_action: "SKIP"
```

### UPDATE
- **Behavior**: Update existing bundle ownership and re-process assets/permissions
- **Use Case**: Ownership changes or re-running migrations with updated configurations
- **Result**: Updates bundle metadata, clears and re-adds assets/permissions

```hocon
duplicate_bundle_action: "UPDATE"
```

**UPDATE Behavior Details:**
- Updates bundle `owner_id` and `owner_type` if different
- Clears existing assets of the specified type from bundle
- Clears existing permissions for the asset type
- Re-processes assets and permissions with current configuration
- Maintains transaction safety - all changes are atomic

## Usage

### Development Setup

```bash
# Create virtual environment
python3.12 -m venv .env
source .env/bin/activate

# Install dependencies
make install-dev-requirements
```

### Running the Job

```bash
# Set database credentials
export DB_HOST="your-db-host"
export DB_USER="your-username"
export DB_PASSWORD="your-password"

# Run the job
make run
```

### Testing

```bash
# Run tests
make test
```

### Docker

```bash
# Build and push Docker image
make docker-push
```

## Features

### Multi-Asset Support
- Support for any asset type via configurable mappings
- Extensible to new resource types without code changes
- Type-specific validation and logging

### Transaction Safety
- Each domain migration runs in its own database transaction
- Automatic rollback on any failure
- No partial data state on errors

### Validation
- Pre-migration validation to check for existing bundles
- Asset existence verification per type
- Configurable validation options

### Dry Run Mode
- Test migration logic without making changes
- Useful for validation and testing across different asset types

### Duplicate Bundle Handling
- **FAIL**: Strict mode - stop on duplicate bundles
- **SKIP**: Incremental mode - skip domains with existing bundles
- **UPDATE**: Update mode - refresh bundle ownership and assets
- Transaction-safe updates with atomic operations

### Flexible Configuration
- Support for multiple domains and asset types in single run
- Configurable owner types (USER or GROUP)
- Environment-based credential management

## Database Schema

The job interacts with the following tables:

- `bundle`: Bundle definitions
- `bundle_asset`: Asset-to-bundle mappings
- `bundle_permission`: Bundle permissions
- **Asset Tables**: Configurable (lakehouse, pipeline, dataset, etc.)
- `iam_user`, `iam_group`, `iam_role`: Identity and access management
- `user_role_mapping_v2`, `group_role_mapping_v2`: Role mappings

## Error Handling

- Comprehensive logging with structured messages
- Graceful handling of missing data
- Transaction rollback on failures
- Detailed error reporting per asset type

## Migration Process Details

### Step 1: Create Default Bundle
Creates a bundle with name pattern `{domain_id}_default` owned by the specified user or group.

### Step 2: Move Assets to Bundle
- Identifies all non-deleted assets of the specified type in the domain
- Uses configurable table mappings for different asset types
- Associates them with the new bundle via `bundle_asset` table

### Step 3: Set Permissions
Analyzes existing role mappings for users and groups in the domain and creates corresponding bundle permissions:

- `list`/`view` permissions → `VIEW`
- `manage` permissions → `UPDATE`, `DELETE`, `EXECUTE`, `CONSUME`

## Adding New Asset Types

To support a new asset type, simply add its mapping to the configuration:

```hocon
asset_mappings: {
    NEW_ASSET_TYPE: {
        table: "your_asset_table"
        id_column: "asset_id"
        domain_column: "domain_id"
        filter_condition: "status = 'active'"
    }
}
```

Then configure domains to use the new asset type:

```hocon
migration: {
    domains: [
        {
            domain_id: "your_domain"
            owner_id: "owner"
            owner_type: "USER"
            asset_type: "NEW_ASSET_TYPE"
        }
    ]
}
```

## Monitoring

The job provides structured logging for monitoring:

- Migration progress per domain and asset type
- Success/failure counts
- Detailed error messages
- Transaction status

## Prerequisites

- Python 3.12+
- PySpark 3.5.5
- PostgreSQL database access
- IOMETE platform environment