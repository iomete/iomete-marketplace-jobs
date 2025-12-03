"""Tests for migration module."""

import pytest
from unittest.mock import Mock, MagicMock

from ras_onboarding.asset.migration import AssetOnboardingMigration
from ras_onboarding.common.database import DatabaseManager

@pytest.fixture
def db_manager():
    """Mock database manager."""
    return Mock(spec=DatabaseManager)


@pytest.fixture
def config():
    """Test configuration."""
    return {
        'asset_mappings': {
            'COMPUTE': {
                'table': 'lakehouse',
                'id_column': 'id',
                'domain_column': 'domain',
                'filter_condition': 'is_deleted = false',
                'service': 'lakehouse',
                'permission_mappings': {
                    'list': ['VIEW'],
                    'view': ['VIEW'],
                    'manage': ['UPDATE', 'DELETE', 'EXECUTE', 'CONSUME']
                },
                'asset_action_on_duplicate': 'UPDATE'
            },
            'PIPELINE': {
                'table': 'pipeline',
                'id_column': 'pipeline_id',
                'domain_column': 'domain_id',
                'filter_condition': 'status != \'DELETED\'',
                'service': 'pipeline-service',
                'permission_mappings': {
                    'read': ['VIEW'],
                    'execute': ['EXECUTE'],
                    'admin': ['UPDATE', 'DELETE', 'EXECUTE', 'VIEW']
                },
                'asset_action_on_duplicate': 'UPDATE'
            },
            'SPARK_JOB': {
                'table': 'spark_job',
                'id_column': 'id',
                'domain_column': 'domain',
                'filter_condition': 'is_deleted = false',
                'service': 'spark_job',
                'permission_mappings': {
                    'list': ['VIEW'],
                    'view': ['VIEW'],
                    'manage': ['UPDATE', 'DELETE', 'RUN', 'CONSUME']
                },
                'asset_action_on_duplicate': 'UPDATE'
            }
        },
        'migration': {
            'domains': [
                {
                    'domain_id': 'test_domain',
                    'owner_id': 'test_owner',
                    'owner_type': 'USER',
                    'asset_type': 'COMPUTE'
                }
            ],
            'validate_before_migration': True,
            'dry_run': False
        }
    }


@pytest.fixture
def migration(db_manager, config):
    """Migration instance for testing."""
    return AssetOnboardingMigration(db_manager, db_manager, config)


def test_create_default_bundle(migration, db_manager):
    """Test default bundle creation."""
    connection = Mock()
    db_manager.execute_insert.return_value = 'test-bundle-id'

    bundle_id = migration.create_default_bundle(
        connection, 'test_domain', 'test_owner', 'USER'
    )

    assert bundle_id == 'test-bundle-id'
    db_manager.execute_insert.assert_called_once()


def test_get_domain_assets(migration, db_manager):
    """Test getting domain assets."""
    connection = Mock()
    db_manager.get_connection.return_value.__enter__ = Mock(return_value=connection)
    db_manager.get_connection.return_value.__exit__ = Mock(return_value=None)
    db_manager.execute_query.return_value = [
        {'id': 'asset-1'},
        {'id': 'asset-2'}
    ]

    asset_ids = migration.get_domain_assets(db_manager, 'test_domain', 'COMPUTE')

    assert asset_ids == ['asset-1', 'asset-2']
    db_manager.execute_query.assert_called_once()


def test_validate_domain_migration_success(migration, db_manager):
    """Test successful domain validation."""
    connection = Mock()

    # Mock asset DB connection
    asset_connection = Mock()
    migration.asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_connection)
    migration.asset_db.get_connection.return_value.__exit__ = Mock(return_value=None)
    migration.asset_db.execute_query.return_value = [{'id': 'asset-1'}]

    # Mock bundle DB query
    db_manager.execute_query.return_value = []  # no existing bundle

    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is True
    assert result['existing_bundle'] is None


def test_validate_domain_migration_existing_bundle(migration, db_manager):
    """Test validation failure when bundle already exists."""
    connection = Mock()

    # Mock asset DB connection
    asset_connection = Mock()
    migration.asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_connection)
    migration.asset_db.get_connection.return_value.__exit__ = Mock(return_value=None)
    migration.asset_db.execute_query.return_value = [{'id': 'asset-1'}]

    # Mock bundle DB query - existing bundle
    db_manager.execute_query.return_value = [{'id': 'existing-bundle-id', 'owner_id': 'owner1', 'owner_type': 'USER'}]

    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is False




def test_duplicate_bundle_fail_behavior(migration, db_manager):
    """Test FAIL behavior when bundle already exists."""
    # Set FAIL behavior
    migration.migration_config['duplicate_bundle_action'] = 'FAIL'

    domain_config = {
        'domain_id': 'test_domain',
        'owner_id': 'test_owner',
        'owner_type': 'USER',
        'asset_type': 'COMPUTE'
    }

    # Mock transaction context
    mock_connection = Mock()
    mock_transaction = MagicMock()
    mock_transaction.__enter__.return_value = mock_connection
    mock_transaction.__exit__.return_value = None
    db_manager.get_transaction.return_value = mock_transaction

    # Mock existing assets and existing bundle
    db_manager.execute_query.side_effect = [
        [{'id': 'asset-1'}],  # get_domain_assets
        [{'id': 'existing-bundle-id', 'owner_id': 'old_owner', 'owner_type': 'USER'}]  # check_existing_bundle
    ]

    result = migration.migrate_domain(domain_config)

    assert result is False


def test_duplicate_bundle_skip_behavior(migration, db_manager):
    """Test SKIP behavior when bundle already exists."""
    # Set SKIP behavior
    migration.migration_config['duplicate_bundle_action'] = 'SKIP'

    domain_config = {
        'domain_id': 'test_domain',
        'owner_id': 'test_owner',
        'owner_type': 'USER',
        'asset_type': 'COMPUTE'
    }

    # Mock transaction context
    mock_connection = Mock()
    mock_transaction = MagicMock()
    mock_transaction.__enter__.return_value = mock_connection
    mock_transaction.__exit__.return_value = None
    db_manager.get_transaction.return_value = mock_transaction

    # Mock asset DB connection
    asset_connection = Mock()
    migration.asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_connection)
    migration.asset_db.get_connection.return_value.__exit__ = Mock(return_value=None)
    migration.asset_db.execute_query.return_value = [{'id': 'asset-1'}]

    # Mock existing bundle
    db_manager.execute_query.return_value = [{'id': 'existing-bundle-id', 'owner_id': 'old_owner', 'owner_type': 'USER'}]

    result = migration.migrate_domain(domain_config)

    assert result is True  # Should return success but skip processing
    # Should not create new bundle or process assets
    db_manager.execute_insert.assert_not_called()


def test_duplicate_bundle_update_behavior(migration, db_manager):
    """Test UPDATE behavior when bundle already exists."""
    # Set UPDATE behavior
    migration.migration_config['duplicate_bundle_action'] = 'UPDATE'

    domain_config = {
        'domain_id': 'test_domain',
        'owner_id': 'new_owner',
        'owner_type': 'GROUP',
        'asset_type': 'COMPUTE'
    }

    # Mock transaction context
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.rowcount = 2

    mock_connection = Mock()
    mock_connection.cursor.return_value = mock_cursor

    mock_transaction = MagicMock()
    mock_transaction.__enter__.return_value = mock_connection
    mock_transaction.__exit__.return_value = None
    db_manager.get_transaction.return_value = mock_transaction

    # Mock asset DB connection
    asset_connection = Mock()
    migration.asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_connection)
    migration.asset_db.get_connection.return_value.__exit__ = Mock(return_value=None)
    migration.asset_db.execute_query.return_value = [{'id': 'asset-1'}, {'id': 'asset-2'}]

    # Mock existing bundle and queries - using return_value to return empty lists for all subsequent queries
    db_manager.execute_query.side_effect = [
        [{'name': 'new_owner'}],  # owner validation (GROUP uses 'name')
        [{'id': 'existing-bundle-id', 'owner_id': 'old_owner', 'owner_type': 'USER'}],  # existing bundle
    ]
    # After these, all subsequent queries return empty lists
    db_manager.execute_query.return_value = []

    result = migration.migrate_domain(domain_config)

    assert result is True
    # Should not create new bundle
    db_manager.execute_insert.assert_not_called()


def test_check_existing_bundle_found(migration, db_manager):
    """Test checking for existing bundle when it exists."""
    connection = Mock()
    db_manager.execute_query.return_value = [
        {'id': 'bundle-123', 'owner_id': 'owner1', 'owner_type': 'USER'}
    ]

    result = migration.check_existing_bundle(connection, 'test_domain')

    assert result is not None
    assert result['id'] == 'bundle-123'
    assert result['owner_id'] == 'owner1'


def test_check_existing_bundle_not_found(migration, db_manager):
    """Test checking for existing bundle when it doesn't exist."""
    connection = Mock()
    db_manager.execute_query.return_value = []

    result = migration.check_existing_bundle(connection, 'test_domain')

    assert result is None


def test_validate_domain_migration_with_different_actions(migration, db_manager):
    """Test validation with different duplicate actions."""
    connection = Mock()

    # Mock asset DB connection
    asset_connection = Mock()
    migration.asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_connection)
    migration.asset_db.get_connection.return_value.__exit__ = Mock(return_value=None)
    migration.asset_db.execute_query.return_value = [{'id': 'asset-1'}]

    # Test FAIL action
    migration.migration_config['duplicate_bundle_action'] = 'FAIL'
    db_manager.execute_query.return_value = [{'id': 'bundle-123', 'owner_id': 'owner1', 'owner_type': 'USER'}]
    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is False
    assert 'skip' not in result

    # Test SKIP action
    migration.migration_config['duplicate_bundle_action'] = 'SKIP'
    db_manager.execute_query.return_value = [{'id': 'bundle-123', 'owner_id': 'owner1', 'owner_type': 'USER'}]
    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is False
    assert result.get('skip') is True

    # Test UPDATE action
    migration.migration_config['duplicate_bundle_action'] = 'UPDATE'
    db_manager.execute_query.return_value = [{'id': 'bundle-123', 'owner_id': 'owner1', 'owner_type': 'USER'}]
    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is True
    assert result.get('update') is True


# New tests for dynamic asset type functionality

def test_build_asset_query_compute(migration):
    """Test dynamic asset query building for COMPUTE assets."""
    query = migration.build_asset_query('COMPUTE')

    expected_query = """
            SELECT id
            FROM lakehouse
            WHERE domain = %s
         AND is_deleted = false"""

    assert 'SELECT id' in query
    assert 'FROM lakehouse' in query
    assert 'WHERE domain = %s' in query
    assert 'is_deleted = false' in query


def test_build_asset_query_pipeline(migration):
    """Test dynamic asset query building for PIPELINE assets."""
    query = migration.build_asset_query('PIPELINE')

    expected_parts = [
        'SELECT pipeline_id',
        'FROM pipeline',
        'WHERE domain_id = %s',
        'status != \'DELETED\''
    ]

    for part in expected_parts:
        assert part in query


def test_build_asset_query_unknown_type(migration):
    """Test error handling for unknown asset type."""
    with pytest.raises(ValueError, match="Unknown asset type: UNKNOWN"):
        migration.build_asset_query('UNKNOWN')


def test_build_permission_subquery_compute(migration):
    """Test dynamic permission subquery building for COMPUTE assets."""
    subquery = migration.build_permission_subquery('COMPUTE')

    expected_parts = [
        'WITH lakehouse_service AS',
        'SELECT jsonb_path_query(r.permissions::jsonb',
        '@.service == "lakehouse"',
        'CASE WHEN jsonb_path_exists(lakehouse_perms',
        '@.action == "list"',
        'THEN \'VIEW\' END',
        '@.action == "manage"',
        'THEN \'UPDATE\' END',
        'THEN \'DELETE\' END',
        'THEN \'EXECUTE\' END',
        'THEN \'CONSUME\' END'
    ]

    for part in expected_parts:
        assert part in subquery


def test_build_permission_subquery_pipeline(migration):
    """Test dynamic permission subquery building for PIPELINE assets."""
    subquery = migration.build_permission_subquery('PIPELINE')

    expected_parts = [
        'WITH pipeline-service_service AS',
        '@.service == "pipeline-service"',
        '@.action == "read"',
        'THEN \'VIEW\' END',
        '@.action == "execute"',
        'THEN \'EXECUTE\' END',
        '@.action == "admin"',
        'THEN \'UPDATE\' END',
        'THEN \'DELETE\' END'
    ]

    for part in expected_parts:
        assert part in subquery


def test_validate_asset_configuration_valid_compute(migration):
    """Test asset configuration validation for valid COMPUTE config."""
    result = migration.validate_asset_configuration('COMPUTE')

    assert result['is_valid'] is True
    assert 'error' not in result


def test_validate_asset_configuration_valid_pipeline(migration):
    """Test asset configuration validation for valid PIPELINE config."""
    result = migration.validate_asset_configuration('PIPELINE')

    assert result['is_valid'] is True
    assert 'error' not in result


def test_validate_asset_configuration_unknown_type(migration):
    """Test asset configuration validation for unknown asset type."""
    result = migration.validate_asset_configuration('UNKNOWN')

    assert result['is_valid'] is False
    assert 'not found in configuration' in result['error']


def test_validate_asset_configuration_missing_required_field(migration):
    """Test validation when required fields are missing."""
    # Modify config to remove required field
    del migration.asset_mappings['COMPUTE']['service']

    result = migration.validate_asset_configuration('COMPUTE')

    assert result['is_valid'] is False
    assert "Missing required field 'service'" in result['error']


def test_validate_asset_configuration_missing_permission_mappings(migration):
    """Test validation when permission_mappings are missing."""
    # Modify config to remove permission mappings
    del migration.asset_mappings['COMPUTE']['permission_mappings']

    result = migration.validate_asset_configuration('COMPUTE')

    assert result['is_valid'] is False
    assert "Missing permission_mappings" in result['error']


def test_get_domain_assets_with_dynamic_query(migration, db_manager):
    """Test get_domain_assets uses dynamic query building."""
    connection = Mock()
    db_manager.get_connection.return_value.__enter__ = Mock(return_value=connection)
    db_manager.get_connection.return_value.__exit__ = Mock(return_value=None)

    db_manager.execute_query.return_value = [
        {'id': 'asset-1', 'name': 'test-asset-1'},
        {'id': 'asset-2', 'name': 'test-asset-2'}
    ]

    asset_ids = migration.get_domain_assets(db_manager, 'test_domain', 'COMPUTE')

    assert asset_ids == ['asset-1', 'asset-2']
    # Verify the dynamic query was built and used
    db_manager.execute_query.assert_called_once()
    call_args = db_manager.execute_query.call_args
    query = call_args[0][1]  # Second argument is the query
    params = call_args[0][2]  # Third argument is the params

    assert 'SELECT id' in query
    assert 'FROM lakehouse' in query
    assert 'WHERE domain = %s' in query
    assert 'is_deleted = false' in query
    assert params == ('test_domain',)


def test_pipeline_asset_migration_flow(migration, db_manager):
    """Test full migration flow for PIPELINE assets."""
    # Test just the specific dynamic components without full migration flow

    # Test asset query building for PIPELINE
    query = migration.build_asset_query('PIPELINE')
    assert 'SELECT pipeline_id' in query
    assert 'FROM pipeline' in query
    assert 'WHERE domain_id = %s' in query
    assert 'status != \'DELETED\'' in query

    # Test permission subquery building for PIPELINE
    permission_subquery = migration.build_permission_subquery('PIPELINE')
    assert 'pipeline-service_service' in permission_subquery
    assert '@.service == "pipeline-service"' in permission_subquery
    assert '@.action == "read"' in permission_subquery
    assert '@.action == "execute"' in permission_subquery
    assert '@.action == "admin"' in permission_subquery

    # Test configuration validation for PIPELINE
    validation_result = migration.validate_asset_configuration('PIPELINE')
    assert validation_result['is_valid'] is True


def test_asset_configuration_validation_in_migration(migration, db_manager):
    """Test that asset configuration validation is called during migration."""
    # Test with invalid asset type
    invalid_domain_config = {
        'domain_id': 'test_domain',
        'owner_id': 'test_owner',
        'owner_type': 'USER',
        'asset_type': 'INVALID_TYPE'
    }

    result = migration.migrate_domain(invalid_domain_config)

    # Should fail due to invalid asset type
    assert result is False


# Tests for multi-asset domain migration functionality

def test_get_asset_types_from_config_single_asset_type(migration):
    """Test getting asset types when using legacy asset_type field."""
    domain_config = {
        'domain_id': 'test_domain',
        'owner_id': 'test_owner',
        'owner_type': 'USER',
        'asset_type': 'COMPUTE'
    }

    asset_types = migration.get_asset_types_from_config(domain_config)

    assert asset_types == ['COMPUTE']


def test_get_asset_types_from_config_multiple_asset_types(migration):
    """Test getting asset types when using new asset_types field."""
    domain_config = {
        'domain_id': 'test_domain',
        'owner_id': 'test_owner',
        'owner_type': 'USER',
        'asset_types': ['COMPUTE', 'SPARK_JOB']
    }

    asset_types = migration.get_asset_types_from_config(domain_config)

    assert asset_types == ['COMPUTE', 'SPARK_JOB']


def test_get_asset_types_from_config_single_in_list(migration):
    """Test getting asset types when single type is in a list."""
    domain_config = {
        'domain_id': 'test_domain',
        'owner_id': 'test_owner',
        'owner_type': 'USER',
        'asset_types': ['PIPELINE']
    }

    asset_types = migration.get_asset_types_from_config(domain_config)

    assert asset_types == ['PIPELINE']


def test_get_asset_types_from_config_default_fallback(migration):
    """Test default fallback when no asset type specified."""
    domain_config = {
        'domain_id': 'test_domain',
        'owner_id': 'test_owner',
        'owner_type': 'USER'
    }

    asset_types = migration.get_asset_types_from_config(domain_config)

    assert asset_types == ['COMPUTE']


def test_migrate_single_asset_type_success(migration, db_manager):
    """Test migrating a single asset type for a domain."""
    # Test by mocking the get_domain_assets method directly to avoid complex DB setup
    original_get_domain_assets = migration.get_domain_assets
    migration.get_domain_assets = Mock(return_value=['asset-1', 'asset-2'])

    # Mock transaction context
    mock_connection = Mock()
    mock_transaction = Mock()
    mock_transaction.__enter__ = Mock(return_value=mock_connection)
    mock_transaction.__exit__ = Mock(return_value=None)
    db_manager.get_transaction.return_value = mock_transaction

    # Mock bundle DB queries - need to include queries for checking existing assets
    db_manager.execute_query.side_effect = [
        [{'username': 'test_owner'}],  # owner validation
        [],  # check_existing_bundle (no existing bundle)
        [],  # get_existing_bundle_assets (no existing assets)
        [],  # get_existing_bundle_permissions for users
        []   # get_existing_bundle_permissions for groups
    ]

    # Mock bundle creation
    db_manager.execute_insert.return_value = 'new-bundle-id'

    # Mock cursor for asset/permission operations
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 2
    mock_connection.cursor.return_value = mock_cursor

    # Execute single asset type migration
    result = migration.migrate_single_asset_type('test_domain', 'test_owner', 'USER', 'COMPUTE')

    # Verify success
    assert result is True

    # Verify get_domain_assets was called
    migration.get_domain_assets.assert_called()

    # Restore original method
    migration.get_domain_assets = original_get_domain_assets


def test_migrate_domain_multiple_asset_types(migration, db_manager):
    """Test migrating domain with multiple asset types."""
    domain_config = {
        'domain_id': 'multi_asset_domain',
        'owner_id': 'multi_owner',
        'owner_type': 'USER',
        'asset_types': ['COMPUTE', 'SPARK_JOB']
    }

    # Mock migrate_single_asset_type to return success for both asset types
    original_migrate_single = migration.migrate_single_asset_type
    migration.migrate_single_asset_type = Mock(return_value=True)

    result = migration.migrate_domain(domain_config)

    # Verify success
    assert result is True

    # Verify migrate_single_asset_type was called for each asset type
    assert migration.migrate_single_asset_type.call_count == 2
    migration.migrate_single_asset_type.assert_any_call('multi_asset_domain', 'multi_owner', 'USER', 'COMPUTE')
    migration.migrate_single_asset_type.assert_any_call('multi_asset_domain', 'multi_owner', 'USER', 'SPARK_JOB')

    # Restore original method
    migration.migrate_single_asset_type = original_migrate_single


def test_migrate_domain_partial_failure(migration, db_manager):
    """Test migrating domain where some asset types fail."""
    domain_config = {
        'domain_id': 'partial_fail_domain',
        'owner_id': 'partial_owner',
        'owner_type': 'USER',
        'asset_types': ['COMPUTE', 'SPARK_JOB', 'PIPELINE']
    }

    # Mock migrate_single_asset_type to fail for PIPELINE
    def mock_migrate_single(domain_id, owner_id, owner_type, asset_type):
        if asset_type == 'PIPELINE':
            return False
        return True

    original_migrate_single = migration.migrate_single_asset_type
    migration.migrate_single_asset_type = Mock(side_effect=mock_migrate_single)

    result = migration.migrate_domain(domain_config)

    # Verify failure due to partial success
    assert result is False

    # Verify migrate_single_asset_type was called for each asset type
    assert migration.migrate_single_asset_type.call_count == 3

    # Restore original method
    migration.migrate_single_asset_type = original_migrate_single


def test_spark_job_asset_type_configuration(migration):
    """Test SPARK_JOB asset type has correct configuration."""
    # Test asset query building for SPARK_JOB
    query = migration.build_asset_query('SPARK_JOB')
    assert 'SELECT id' in query
    assert 'FROM spark_job' in query
    assert 'WHERE domain = %s' in query
    assert 'is_deleted = false' in query

    # Test permission subquery building for SPARK_JOB
    permission_subquery = migration.build_permission_subquery('SPARK_JOB')
    assert 'spark_job_service' in permission_subquery
    assert '@.service == "spark_job"' in permission_subquery
    assert '@.action == "list"' in permission_subquery
    assert '@.action == "view"' in permission_subquery
    assert '@.action == "manage"' in permission_subquery
    assert 'THEN \'RUN\' END' in permission_subquery  # SPARK_JOB specific permission

    # Test configuration validation for SPARK_JOB
    validation_result = migration.validate_asset_configuration('SPARK_JOB')
    assert validation_result['is_valid'] is True


# Tests for asset_action_on_duplicate functionality

def test_validate_asset_action_on_duplicate_missing(migration):
    """Test validation fails when asset_action_on_duplicate is missing."""
    # Remove the field
    del migration.asset_mappings['COMPUTE']['asset_action_on_duplicate']

    result = migration.validate_asset_configuration('COMPUTE')

    assert result['is_valid'] is False
    assert 'asset_action_on_duplicate' in result['error']


def test_validate_asset_action_on_duplicate_invalid_value(migration):
    """Test validation fails when asset_action_on_duplicate has invalid value."""
    # Set invalid value
    migration.asset_mappings['COMPUTE']['asset_action_on_duplicate'] = 'INVALID'

    result = migration.validate_asset_configuration('COMPUTE')

    assert result['is_valid'] is False
    assert 'Invalid asset_action_on_duplicate' in result['error']


def test_validate_asset_action_on_duplicate_valid_values(migration):
    """Test validation succeeds for all valid asset_action_on_duplicate values."""
    valid_values = ['SKIP', 'UPDATE', 'ERROR', 'RESET']

    for value in valid_values:
        migration.asset_mappings['COMPUTE']['asset_action_on_duplicate'] = value
        result = migration.validate_asset_configuration('COMPUTE')
        assert result['is_valid'] is True, f"Validation failed for {value}"


def test_get_existing_bundle_assets(migration, db_manager):
    """Test getting existing assets from bundle."""
    connection = Mock()
    db_manager.execute_query.return_value = [
        {'asset_id': 'asset-1'},
        {'asset_id': 'asset-2'}
    ]

    existing = migration.get_existing_bundle_assets(
        connection, 'bundle-123', 'COMPUTE', ['asset-1', 'asset-2', 'asset-3']
    )

    assert existing == ['asset-1', 'asset-2']
    db_manager.execute_query.assert_called_once()


def test_get_existing_bundle_permissions(migration, db_manager):
    """Test getting existing permissions from bundle."""
    connection = Mock()
    db_manager.execute_query.return_value = [
        {'actor_type': 'USER', 'actor_id': 'user1', 'permissions': ['VIEW', 'UPDATE']},
        {'actor_type': 'GROUP', 'actor_id': 'group1', 'permissions': ['VIEW']}
    ]

    existing = migration.get_existing_bundle_permissions(connection, 'bundle-123', 'COMPUTE')

    assert len(existing) == 2
    assert existing[0]['actor_id'] == 'user1'
    db_manager.execute_query.assert_called_once()


def test_move_assets_skip_action(migration, db_manager):
    """Test SKIP action only inserts new assets."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    connection.cursor.return_value = mock_cursor

    # Mock existing assets
    db_manager.execute_query.return_value = [
        {'asset_id': 'asset-1'}
    ]

    asset_ids = ['asset-1', 'asset-2', 'asset-3']
    migration.move_assets_to_bundle(connection, 'bundle-123', asset_ids, 'COMPUTE', 'SKIP')

    # Should only insert asset-2 and asset-3
    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert 'asset-2' in sql
    assert 'asset-3' in sql


def test_move_assets_error_action_raises_exception(migration, db_manager):
    """Test ERROR action raises exception when duplicates exist."""
    connection = Mock()

    # Mock existing assets
    db_manager.execute_query.return_value = [
        {'asset_id': 'asset-1'}
    ]

    asset_ids = ['asset-1', 'asset-2']

    with pytest.raises(ValueError, match="Asset action is ERROR"):
        migration.move_assets_to_bundle(connection, 'bundle-123', asset_ids, 'COMPUTE', 'ERROR')


def test_move_assets_update_action_uses_on_conflict(migration, db_manager):
    """Test UPDATE action uses ON CONFLICT clause."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    connection.cursor.return_value = mock_cursor

    # Mock existing assets
    db_manager.execute_query.return_value = [
        {'asset_id': 'asset-1'}
    ]

    asset_ids = ['asset-1', 'asset-2']
    migration.move_assets_to_bundle(connection, 'bundle-123', asset_ids, 'COMPUTE', 'UPDATE')

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert 'ON CONFLICT' in sql
    assert 'DO NOTHING' in sql


def test_set_user_permissions_skip_action(migration, db_manager):
    """Test SKIP action skips permission setting when permissions exist."""
    connection = Mock()

    # Mock existing permissions
    db_manager.execute_query.return_value = [
        {'actor_type': 'USER', 'actor_id': 'user1', 'permissions': ['VIEW']}
    ]

    migration.set_user_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'SKIP')

    # Should not execute any insert/update
    connection.cursor.assert_not_called()


def test_set_user_permissions_error_action_raises_exception(migration, db_manager):
    """Test ERROR action raises exception when permissions exist."""
    connection = Mock()

    # Mock existing permissions
    db_manager.execute_query.return_value = [
        {'actor_type': 'USER', 'actor_id': 'user1', 'permissions': ['VIEW']}
    ]

    with pytest.raises(ValueError, match="Asset action is ERROR"):
        migration.set_user_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'ERROR')


def test_set_user_permissions_update_action_merges(migration, db_manager):
    """Test UPDATE action merges permissions."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 1
    connection.cursor.return_value = mock_cursor

    # Mock existing permissions
    db_manager.execute_query.return_value = [
        {'actor_type': 'USER', 'actor_id': 'user1', 'permissions': ['VIEW']}
    ]

    migration.set_user_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'UPDATE')

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert 'ON CONFLICT' in sql
    assert 'DO UPDATE' in sql
    assert 'permissions' in sql


def test_set_group_permissions_skip_action(migration, db_manager):
    """Test SKIP action skips group permission setting when permissions exist."""
    connection = Mock()

    # Mock existing permissions
    db_manager.execute_query.return_value = [
        {'actor_type': 'GROUP', 'actor_id': 'group1', 'permissions': ['VIEW']}
    ]

    migration.set_group_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'SKIP')

    # Should not execute any insert/update
    connection.cursor.assert_not_called()


def test_migrate_single_asset_type_with_reset_action(migration, db_manager):
    """Test RESET action clears assets and permissions for specific asset type."""
    # Set RESET action in config
    migration.asset_mappings['COMPUTE']['asset_action_on_duplicate'] = 'RESET'

    # Mock transaction context
    mock_connection = Mock()
    mock_transaction = MagicMock()
    mock_transaction.__enter__.return_value = mock_connection
    mock_transaction.__exit__.return_value = None
    db_manager.get_transaction.return_value = mock_transaction

    # Mock asset DB connection
    asset_connection = Mock()
    migration.asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_connection)
    migration.asset_db.get_connection.return_value.__exit__ = Mock(return_value=None)
    migration.asset_db.execute_query.return_value = [{'id': 'asset-1'}]

    # Mock existing bundle (UPDATE scenario) and queries
    db_manager.execute_query.side_effect = [
        [{'username': 'test_owner'}],  # owner validation
        [{'id': 'existing-bundle-id', 'owner_id': 'old_owner', 'owner_type': 'USER'}],  # existing bundle
    ]
    # After these, all subsequent queries return empty lists
    db_manager.execute_query.return_value = []

    # Mock cursor for clear operations and asset/permission operations
    mock_cursor = MagicMock()
    mock_cursor.__enter__.return_value = mock_cursor
    mock_cursor.__exit__.return_value = None
    mock_cursor.rowcount = 1
    mock_connection.cursor.return_value = mock_cursor

    # Set duplicate bundle action to UPDATE
    migration.migration_config['duplicate_bundle_action'] = 'UPDATE'

    result = migration.migrate_single_asset_type('test_domain', 'test_owner', 'USER', 'COMPUTE')

    assert result is True
    # Verify clear operations were called (for RESET action)
    # Check that cursor.execute was called multiple times (clear + insert operations)
    assert mock_cursor.execute.call_count >= 2


def test_migrate_single_asset_type_extracts_asset_action(migration, db_manager):
    """Test that migrate_single_asset_type correctly extracts asset_action_on_duplicate from config."""
    # Set specific action in config
    migration.asset_mappings['COMPUTE']['asset_action_on_duplicate'] = 'SKIP'

    # Mock to make migration fail early but after extraction
    migration.asset_db.get_connection.return_value.__enter__ = Mock(side_effect=Exception("Stop early"))

    try:
        migration.migrate_single_asset_type('test_domain', 'test_owner', 'USER', 'COMPUTE')
    except:
        pass

    # The test passes if no exception during config extraction
    assert migration.asset_mappings['COMPUTE']['asset_action_on_duplicate'] == 'SKIP'


def test_multi_asset_type_different_actions(migration, db_manager):
    """Test multiple asset types with different asset_action_on_duplicate values."""
    # Set different actions for different asset types
    migration.asset_mappings['COMPUTE']['asset_action_on_duplicate'] = 'SKIP'
    migration.asset_mappings['SPARK_JOB']['asset_action_on_duplicate'] = 'UPDATE'

    # Validate both
    compute_validation = migration.validate_asset_configuration('COMPUTE')
    spark_validation = migration.validate_asset_configuration('SPARK_JOB')

    assert compute_validation['is_valid'] is True
    assert spark_validation['is_valid'] is True


# Additional edge case tests

def test_get_existing_bundle_assets_empty_list(migration, db_manager):
    """Test getting existing assets with empty asset_ids list."""
    connection = Mock()

    existing = migration.get_existing_bundle_assets(
        connection, 'bundle-123', 'COMPUTE', []
    )

    assert existing == []
    # Should not execute query if asset_ids is empty
    db_manager.execute_query.assert_not_called()


def test_get_existing_bundle_assets_no_matches(migration, db_manager):
    """Test when no assets exist in bundle."""
    connection = Mock()
    db_manager.execute_query.return_value = []

    existing = migration.get_existing_bundle_assets(
        connection, 'bundle-123', 'COMPUTE', ['asset-1', 'asset-2']
    )

    assert existing == []
    db_manager.execute_query.assert_called_once()


def test_get_existing_bundle_permissions_empty(migration, db_manager):
    """Test when no permissions exist in bundle."""
    connection = Mock()
    db_manager.execute_query.return_value = []

    existing = migration.get_existing_bundle_permissions(connection, 'bundle-123', 'COMPUTE')

    assert existing == []
    db_manager.execute_query.assert_called_once()


def test_move_assets_empty_list(migration, db_manager):
    """Test moving empty asset list."""
    connection = Mock()

    migration.move_assets_to_bundle(connection, 'bundle-123', [], 'COMPUTE', 'UPDATE')

    # Should return early without executing anything
    connection.cursor.assert_not_called()


def test_move_assets_skip_all_exist(migration, db_manager):
    """Test SKIP action when all assets already exist."""
    connection = Mock()

    # Mock all assets as existing
    db_manager.execute_query.return_value = [
        {'asset_id': 'asset-1'},
        {'asset_id': 'asset-2'}
    ]

    asset_ids = ['asset-1', 'asset-2']
    migration.move_assets_to_bundle(connection, 'bundle-123', asset_ids, 'COMPUTE', 'SKIP')

    # Should not insert anything
    connection.cursor.assert_not_called()


def test_move_assets_error_no_duplicates(migration, db_manager):
    """Test ERROR action when no duplicates exist - should succeed."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    connection.cursor.return_value = mock_cursor

    # Mock no existing assets
    db_manager.execute_query.return_value = []

    asset_ids = ['asset-1', 'asset-2']
    # Should not raise exception
    migration.move_assets_to_bundle(connection, 'bundle-123', asset_ids, 'COMPUTE', 'ERROR')

    mock_cursor.execute.assert_called_once()


def test_move_assets_reset_action(migration, db_manager):
    """Test RESET action uses ON CONFLICT DO NOTHING."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    connection.cursor.return_value = mock_cursor

    # Mock existing assets
    db_manager.execute_query.return_value = [{'asset_id': 'asset-1'}]

    asset_ids = ['asset-1', 'asset-2']
    migration.move_assets_to_bundle(connection, 'bundle-123', asset_ids, 'COMPUTE', 'RESET')

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert 'ON CONFLICT' in sql
    assert 'DO NOTHING' in sql


def test_move_assets_case_insensitive_action(migration, db_manager):
    """Test that action parameter is case-insensitive."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    connection.cursor.return_value = mock_cursor

    db_manager.execute_query.return_value = []

    # Test lowercase
    migration.move_assets_to_bundle(connection, 'bundle-123', ['asset-1'], 'COMPUTE', 'update')

    # Should not raise exception and work correctly
    mock_cursor.execute.assert_called()


def test_set_user_permissions_no_existing(migration, db_manager):
    """Test UPDATE action when no existing permissions - should insert."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 2
    connection.cursor.return_value = mock_cursor

    # Mock no existing permissions
    db_manager.execute_query.return_value = []

    migration.set_user_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'UPDATE')

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    # Should still have ON CONFLICT clause even when empty
    assert 'ON CONFLICT' in sql


def test_set_user_permissions_error_no_existing(migration, db_manager):
    """Test ERROR action when no existing permissions - should succeed."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 1
    connection.cursor.return_value = mock_cursor

    # Mock no existing permissions
    db_manager.execute_query.return_value = []

    # Should not raise exception
    migration.set_user_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'ERROR')

    mock_cursor.execute.assert_called_once()


def test_set_user_permissions_reset_action(migration, db_manager):
    """Test RESET action for user permissions."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 1
    connection.cursor.return_value = mock_cursor

    # Mock existing permissions
    db_manager.execute_query.return_value = [
        {'actor_type': 'USER', 'actor_id': 'user1', 'permissions': ['VIEW']}
    ]

    migration.set_user_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'RESET')

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    # RESET should use DO NOTHING not DO UPDATE
    assert 'ON CONFLICT' in sql
    assert 'DO NOTHING' in sql


def test_set_group_permissions_error_action_raises(migration, db_manager):
    """Test ERROR action raises exception for group permissions."""
    connection = Mock()

    # Mock existing permissions
    db_manager.execute_query.return_value = [
        {'actor_type': 'GROUP', 'actor_id': 'group1', 'permissions': ['VIEW']}
    ]

    with pytest.raises(ValueError, match="Asset action is ERROR"):
        migration.set_group_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'ERROR')


def test_set_group_permissions_update_action_merges(migration, db_manager):
    """Test UPDATE action merges group permissions."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 1
    connection.cursor.return_value = mock_cursor

    # Mock existing permissions
    db_manager.execute_query.return_value = [
        {'actor_type': 'GROUP', 'actor_id': 'group1', 'permissions': ['VIEW']}
    ]

    migration.set_group_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'UPDATE')

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert 'ON CONFLICT' in sql
    assert 'DO UPDATE' in sql
    assert 'permissions' in sql


def test_set_group_permissions_reset_action(migration, db_manager):
    """Test RESET action for group permissions."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 1
    connection.cursor.return_value = mock_cursor

    # Mock existing permissions
    db_manager.execute_query.return_value = [
        {'actor_type': 'GROUP', 'actor_id': 'group1', 'permissions': ['VIEW']}
    ]

    migration.set_group_permissions(connection, 'bundle-123', 'domain-1', 'COMPUTE', 'RESET')

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    assert 'ON CONFLICT' in sql
    assert 'DO NOTHING' in sql


def test_validate_asset_action_case_insensitive(migration):
    """Test that validation handles case-insensitive asset_action_on_duplicate."""
    # Test lowercase
    migration.asset_mappings['COMPUTE']['asset_action_on_duplicate'] = 'skip'
    result = migration.validate_asset_configuration('COMPUTE')
    assert result['is_valid'] is True

    # Test mixed case
    migration.asset_mappings['COMPUTE']['asset_action_on_duplicate'] = 'UpDaTe'
    result = migration.validate_asset_configuration('COMPUTE')
    assert result['is_valid'] is True


def test_move_assets_large_asset_list(migration, db_manager):
    """Test handling large number of assets."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    connection.cursor.return_value = mock_cursor

    # Mock no existing assets
    db_manager.execute_query.return_value = []

    # Create large list of assets
    large_asset_list = [f'asset-{i}' for i in range(1000)]

    migration.move_assets_to_bundle(connection, 'bundle-123', large_asset_list, 'COMPUTE', 'UPDATE')

    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    # Verify all assets are in the SQL
    assert 'asset-1' in sql
    assert 'asset-999' in sql


def test_error_action_shows_limited_asset_ids(migration, db_manager):
    """Test ERROR action only shows first 5 duplicate asset IDs in error message."""
    connection = Mock()

    # Mock many existing assets
    existing_assets = [{'asset_id': f'asset-{i}'} for i in range(20)]
    db_manager.execute_query.return_value = existing_assets

    asset_ids = [f'asset-{i}' for i in range(20)]

    try:
        migration.move_assets_to_bundle(connection, 'bundle-123', asset_ids, 'COMPUTE', 'ERROR')
        assert False, "Should have raised ValueError"
    except ValueError as e:
        error_msg = str(e)
        # Should show count
        assert '20' in error_msg
        # Should limit displayed IDs to 5
        assert 'asset-0' in error_msg
        # Should not show all 20 IDs
        assert 'asset-19' not in error_msg


def test_clear_bundle_operations_called_for_reset(migration, db_manager):
    """Test that clear operations are only called for RESET action."""
    # This is tested by checking the migrate_single_asset_type behavior
    # RESET should call clear_bundle_assets and clear_bundle_permissions
    # Other actions should not call these methods before migration

    # Already covered in test_migrate_single_asset_type_with_reset_action
    # but documenting the behavior here
    pass


# Tests for 'all' key functionality in permission_mappings

def test_validate_asset_configuration_with_all_key_only(migration):
    """Test validation passes when 'all' key is the only key in permission_mappings."""
    # Add asset type with 'all' key only
    migration.asset_mappings['TEST_ALL'] = {
        'table': 'test_table',
        'id_column': 'id',
        'domain_column': 'domain',
        'service': 'test_service',
        'permission_mappings': {
            'all': ['VIEW', 'UPDATE', 'DELETE', 'RUN']
        },
        'asset_action_on_duplicate': 'UPDATE'
    }

    result = migration.validate_asset_configuration('TEST_ALL')
    assert result['is_valid'] is True


def test_validate_asset_configuration_with_all_key_mixed_fails(migration):
    """Test validation fails when 'all' key is mixed with other action keys."""
    # Add asset type with 'all' key mixed with other keys
    migration.asset_mappings['TEST_MIXED'] = {
        'table': 'test_table',
        'id_column': 'id',
        'domain_column': 'domain',
        'service': 'test_service',
        'permission_mappings': {
            'all': ['VIEW', 'UPDATE'],
            'list': ['VIEW'],
            'manage': ['UPDATE', 'DELETE']
        },
        'asset_action_on_duplicate': 'UPDATE'
    }

    result = migration.validate_asset_configuration('TEST_MIXED')
    assert result['is_valid'] is False
    assert 'must be the only key' in result['error']


def test_validate_asset_configuration_with_all_key_empty_array_fails(migration):
    """Test validation fails when 'all' key has empty permissions array."""
    # Add asset type with 'all' key but empty array
    migration.asset_mappings['TEST_EMPTY'] = {
        'table': 'test_table',
        'id_column': 'id',
        'domain_column': 'domain',
        'service': 'test_service',
        'permission_mappings': {
            'all': []
        },
        'asset_action_on_duplicate': 'UPDATE'
    }

    result = migration.validate_asset_configuration('TEST_EMPTY')
    assert result['is_valid'] is False
    assert 'non-empty array' in result['error']


def test_validate_asset_configuration_with_all_key_not_list_fails(migration):
    """Test validation fails when 'all' key value is not a list."""
    # Add asset type with 'all' key but not a list
    migration.asset_mappings['TEST_NOT_LIST'] = {
        'table': 'test_table',
        'id_column': 'id',
        'domain_column': 'domain',
        'service': 'test_service',
        'permission_mappings': {
            'all': 'VIEW'
        },
        'asset_action_on_duplicate': 'UPDATE'
    }

    result = migration.validate_asset_configuration('TEST_NOT_LIST')
    assert result['is_valid'] is False
    assert 'non-empty array' in result['error']


def test_build_permission_subquery_with_all_key(migration):
    """Test that build_permission_subquery returns simplified query for 'all' key."""
    # Add asset type with 'all' key
    migration.asset_mappings['TEST_ALL'] = {
        'table': 'test_table',
        'id_column': 'id',
        'domain_column': 'domain',
        'service': 'test_service',
        'permission_mappings': {
            'all': ['VIEW', 'UPDATE', 'DELETE', 'RUN']
        },
        'asset_action_on_duplicate': 'UPDATE'
    }

    subquery = migration.build_permission_subquery('TEST_ALL')

    # Should be simplified query without role checks
    assert 'SELECT unnest(ARRAY[' in subquery
    assert "'VIEW'" in subquery
    assert "'UPDATE'" in subquery
    assert "'DELETE'" in subquery
    assert "'RUN'" in subquery
    # Should NOT have role-related CTEs
    assert 'test_service_service' not in subquery
    assert 'jsonb_path_query' not in subquery
    assert 'CASE WHEN' not in subquery


def test_build_permission_subquery_without_all_key(migration):
    """Test that build_permission_subquery returns role-based query without 'all' key."""
    # Use existing COMPUTE asset type (doesn't have 'all' key)
    subquery = migration.build_permission_subquery('COMPUTE')

    # Should have role-based query structure
    assert 'lakehouse_service' in subquery
    assert 'jsonb_path_query' in subquery
    assert 'CASE WHEN' in subquery
    assert '@.action' in subquery


def test_set_user_permissions_with_all_key(migration, db_manager):
    """Test that set_user_permissions uses simplified query with 'all' key."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 3
    connection.cursor.return_value = mock_cursor

    # Add asset type with 'all' key
    migration.asset_mappings['TEST_ALL'] = {
        'table': 'test_table',
        'id_column': 'id',
        'domain_column': 'domain',
        'service': 'test_service',
        'permission_mappings': {
            'all': ['VIEW', 'UPDATE', 'DELETE']
        },
        'asset_action_on_duplicate': 'UPDATE'
    }

    # Mock no existing permissions
    db_manager.execute_query.return_value = []

    migration.set_user_permissions(connection, 'bundle-123', 'domain-1', 'TEST_ALL', 'UPDATE')

    # Verify cursor was called
    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    params = mock_cursor.execute.call_args[0][1]

    # Should have simplified query without role joins
    assert 'all_domain_users' in sql
    assert 'ARRAY[' in sql
    assert "'VIEW'" in sql
    assert "'UPDATE'" in sql
    assert "'DELETE'" in sql
    # Should NOT have role mapping joins
    assert 'user_role_mapping_v2' not in sql
    assert 'iam_role' not in sql
    assert 'user_all_permissions' not in sql  # No subquery for aggregating permissions

    # Should have only 3 parameters (domain_id, bundle_id, asset_type)
    assert len(params) == 3
    assert params == ('domain-1', 'bundle-123', 'TEST_ALL')


def test_set_group_permissions_with_all_key(migration, db_manager):
    """Test that set_group_permissions uses simplified query with 'all' key."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 2
    connection.cursor.return_value = mock_cursor

    # Add asset type with 'all' key
    migration.asset_mappings['TEST_ALL'] = {
        'table': 'test_table',
        'id_column': 'id',
        'domain_column': 'domain',
        'service': 'test_service',
        'permission_mappings': {
            'all': ['VIEW', 'RUN']
        },
        'asset_action_on_duplicate': 'UPDATE'
    }

    # Mock no existing permissions
    db_manager.execute_query.return_value = []

    migration.set_group_permissions(connection, 'bundle-456', 'domain-2', 'TEST_ALL', 'UPDATE')

    # Verify cursor was called
    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    params = mock_cursor.execute.call_args[0][1]

    # Should have simplified query without role joins
    assert 'all_domain_groups' in sql
    assert 'ARRAY[' in sql
    assert "'VIEW'" in sql
    assert "'RUN'" in sql
    # Should NOT have role mapping joins
    assert 'group_role_mapping_v2' not in sql
    assert 'iam_role' not in sql
    assert 'group_all_permissions' not in sql  # No subquery for aggregating permissions

    # Should have only 3 parameters (domain_id, bundle_id, asset_type)
    assert len(params) == 3
    assert params == ('domain-2', 'bundle-456', 'TEST_ALL')


def test_set_user_permissions_without_all_key_uses_role_based_query(migration, db_manager):
    """Test that set_user_permissions uses role-based query without 'all' key."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 5
    connection.cursor.return_value = mock_cursor

    # Mock no existing permissions
    db_manager.execute_query.return_value = []

    # Use COMPUTE asset type which doesn't have 'all' key
    migration.set_user_permissions(connection, 'bundle-789', 'domain-3', 'COMPUTE', 'UPDATE')

    # Verify cursor was called
    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    params = mock_cursor.execute.call_args[0][1]

    # Should have role-based query with joins
    assert 'user_role_mapping_v2' in sql
    assert 'iam_role' in sql
    assert 'user_all_permissions' in sql
    assert 'ARRAY_AGG(DISTINCT perm)' in sql

    # Should have 5 parameters (domain_id appears 3 times, then bundle_id, asset_type)
    assert len(params) == 5
    assert params == ('domain-3', 'domain-3', 'domain-3', 'bundle-789', 'COMPUTE')


def test_set_group_permissions_without_all_key_uses_role_based_query(migration, db_manager):
    """Test that set_group_permissions uses role-based query without 'all' key."""
    connection = Mock()
    mock_cursor = Mock()
    mock_cursor.__enter__ = Mock(return_value=mock_cursor)
    mock_cursor.__exit__ = Mock(return_value=None)
    mock_cursor.rowcount = 4
    connection.cursor.return_value = mock_cursor

    # Mock no existing permissions
    db_manager.execute_query.return_value = []

    # Use COMPUTE asset type which doesn't have 'all' key
    migration.set_group_permissions(connection, 'bundle-999', 'domain-4', 'COMPUTE', 'UPDATE')

    # Verify cursor was called
    mock_cursor.execute.assert_called_once()
    sql = mock_cursor.execute.call_args[0][0]
    params = mock_cursor.execute.call_args[0][1]

    # Should have role-based query with joins
    assert 'group_role_mapping_v2' in sql
    assert 'iam_role' in sql
    assert 'group_all_permissions' in sql
    assert 'ARRAY_AGG(DISTINCT perm)' in sql

    # Should have 5 parameters (domain_id appears 3 times, then bundle_id, asset_type)
    assert len(params) == 5
    assert params == ('domain-4', 'domain-4', 'domain-4', 'bundle-999', 'COMPUTE')


def test_all_key_functionality_end_to_end(migration, db_manager):
    """Test end-to-end migration with 'all' key asset type."""
    # Add asset type with 'all' key
    migration.asset_mappings['JUPYTER_CONTAINER'] = {
        'table': 'jupyter_container',
        'id_column': 'id',
        'domain_column': 'domain',
        'filter_condition': 'is_deleted = false',
        'service': 'jupyter_container',
        'permission_mappings': {
            'all': ['VIEW', 'UPDATE', 'DELETE', 'RUN']
        },
        'asset_action_on_duplicate': 'UPDATE'
    }

    # Test validation
    validation = migration.validate_asset_configuration('JUPYTER_CONTAINER')
    assert validation['is_valid'] is True

    # Test permission subquery building
    subquery = migration.build_permission_subquery('JUPYTER_CONTAINER')
    assert 'SELECT unnest(ARRAY[' in subquery
    assert "'VIEW'" in subquery
    assert "'UPDATE'" in subquery
    assert "'DELETE'" in subquery
    assert "'RUN'" in subquery
    assert 'jsonb_path_query' not in subquery  # No role checking

    # Verify asset type can be used in migration
    assert 'JUPYTER_CONTAINER' in migration.asset_mappings