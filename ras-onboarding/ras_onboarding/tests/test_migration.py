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
                'filter_condition': 'is_deleted = false'
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
    return AssetOnboardingMigration(db_manager, config)


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
    db_manager.execute_query.return_value = [
        {'id': 'asset-1'},
        {'id': 'asset-2'}
    ]

    asset_ids = migration.get_domain_assets(connection, 'test_domain', 'COMPUTE')

    assert asset_ids == ['asset-1', 'asset-2']
    db_manager.execute_query.assert_called_once()


def test_validate_domain_migration_success(migration, db_manager):
    """Test successful domain validation."""
    connection = Mock()

    # Mock assets exist
    db_manager.execute_query.side_effect = [
        [{'id': 'asset-1'}],  # get_domain_assets query
        []  # existing bundle query (no existing bundle)
    ]

    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is True
    assert result['existing_bundle'] is None


def test_validate_domain_migration_existing_bundle(migration, db_manager):
    """Test validation failure when bundle already exists."""
    connection = Mock()

    # Mock assets exist and bundle exists
    db_manager.execute_query.side_effect = [
        [{'id': 'asset-1'}],  # get_domain_assets query
        [{'id': 'existing-bundle-id', 'owner_id': 'owner1', 'owner_type': 'USER'}]  # existing bundle query
    ]

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

    # Mock existing assets and existing bundle
    db_manager.execute_query.side_effect = [
        [{'id': 'asset-1'}],  # get_domain_assets
        [{'id': 'existing-bundle-id', 'owner_id': 'old_owner', 'owner_type': 'USER'}]  # check_existing_bundle
    ]

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

    # Mock existing assets and existing bundle
    db_manager.execute_query.side_effect = [
        [{'id': 'asset-1'}, {'id': 'asset-2'}],  # get_domain_assets
        [{'id': 'existing-bundle-id', 'owner_id': 'old_owner', 'owner_type': 'USER'}],  # check_existing_bundle
        [{'id': 'asset-1'}, {'id': 'asset-2'}],  # get_domain_assets again for processing
    ]

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

    # Mock assets exist
    db_manager.execute_query.side_effect = [
        [{'id': 'asset-1'}],  # get_domain_assets
        [{'id': 'bundle-123', 'owner_id': 'owner1', 'owner_type': 'USER'}]  # check_existing_bundle
    ]

    # Test FAIL action
    migration.migration_config['duplicate_bundle_action'] = 'FAIL'
    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is False
    assert 'skip' not in result

    # Reset mock
    db_manager.execute_query.side_effect = [
        [{'id': 'asset-1'}],  # get_domain_assets
        [{'id': 'bundle-123', 'owner_id': 'owner1', 'owner_type': 'USER'}]  # check_existing_bundle
    ]

    # Test SKIP action
    migration.migration_config['duplicate_bundle_action'] = 'SKIP'
    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is False
    assert result.get('skip') is True

    # Reset mock
    db_manager.execute_query.side_effect = [
        [{'id': 'asset-1'}],  # get_domain_assets
        [{'id': 'bundle-123', 'owner_id': 'owner1', 'owner_type': 'USER'}]  # check_existing_bundle
    ]

    # Test UPDATE action
    migration.migration_config['duplicate_bundle_action'] = 'UPDATE'
    result = migration.validate_domain_migration(connection, 'test_domain', 'COMPUTE')
    assert result['can_proceed'] is True
    assert result.get('update') is True