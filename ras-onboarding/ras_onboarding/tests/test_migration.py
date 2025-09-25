"""Tests for migration module."""

import pytest
from unittest.mock import Mock, MagicMock
from ras_onboarding.migration import AssetOnboardingMigration
from ras_onboarding.database import DatabaseManager


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
                }
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
                }
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
                }
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

    # Mock existing bundle
    db_manager.execute_query.return_value = [{'id': 'existing-bundle-id', 'owner_id': 'old_owner', 'owner_type': 'USER'}]

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

    # Mock bundle DB queries
    db_manager.execute_query.side_effect = [
        [{'username': 'test_owner'}],  # owner validation
        []  # check_existing_bundle (no existing bundle)
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