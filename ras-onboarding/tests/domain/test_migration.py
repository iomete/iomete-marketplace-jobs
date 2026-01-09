"""Test cases for domain migration module."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from ras_onboarding.domain.migration import DomainMigration


@pytest.fixture
def mock_iam_db():
    """Mock IAM database manager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    db.execute_insert = Mock()
    db.get_connection = Mock()
    db.get_transaction = Mock()
    return db


@pytest.fixture
def mock_core_db():
    """Mock core database manager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    db.get_connection = Mock()
    return db


@pytest.fixture
def sample_config():
    """Sample configuration for tests."""
    return {
        "migration": {
            "debug_mode": False,
            "dry_run": False,
            "duplicate_bundle_action": "FAIL",
            "domains": [
                {
                    "domain_id": "test-domain",
                    "owner_id": "admin-user",
                    "owner_type": "USER"
                }
            ]
        }
    }


@pytest.fixture
def migration(mock_iam_db, mock_core_db, sample_config):
    """Create DomainMigration instance with mocks."""
    return DomainMigration(mock_iam_db, mock_core_db, sample_config)


class TestGetOrCreateDomainBundle:
    """Test get_or_create_domain_bundle method."""

    def test_create_new_bundle(self, migration, mock_iam_db):
        """Test creating a new domain bundle."""
        mock_connection = Mock()
        mock_connection.cursor = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.return_value = []  # Bundle doesn't exist

        bundle_id, bundle_existed = migration.get_or_create_domain_bundle(
            mock_connection, "test-domain", "admin-user", "USER"
        )

        assert bundle_id is not None
        assert isinstance(bundle_id, str)
        assert bundle_existed is False
        mock_cursor.execute.assert_called_once()

    def test_existing_bundle_fail_mode(self, migration, mock_iam_db):
        """Test FAIL mode when bundle already exists."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"id": "bundle-123", "bundle_type": "DOMAIN"}]

        migration.migration_config["duplicate_bundle_action"] = "FAIL"

        with pytest.raises(ValueError) as exc_info:
            migration.get_or_create_domain_bundle(
                mock_connection, "test-domain", "admin-user", "USER"
            )

        assert "already exists" in str(exc_info.value)

    def test_existing_bundle_skip_mode(self, migration, mock_iam_db):
        """Test SKIP mode when bundle already exists."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"id": "bundle-123", "bundle_type": "DOMAIN"}]

        migration.migration_config["duplicate_bundle_action"] = "SKIP"

        bundle_id, bundle_existed = migration.get_or_create_domain_bundle(
            mock_connection, "test-domain", "admin-user", "USER"
        )

        assert bundle_id is None
        assert bundle_existed is False

    def test_existing_bundle_update_mode(self, migration, mock_iam_db):
        """Test UPDATE mode when bundle already exists."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"id": "bundle-123", "bundle_type": "DOMAIN"}]

        migration.migration_config["duplicate_bundle_action"] = "UPDATE"

        bundle_id, bundle_existed = migration.get_or_create_domain_bundle(
            mock_connection, "test-domain", "admin-user", "USER"
        )

        assert bundle_id == "bundle-123"
        assert bundle_existed is True

        mock_connection.cursor.assert_not_called()

    def test_existing_bundle_overwrite_mode(self, migration, mock_iam_db):
        """Test OVERWRITE mode when bundle already exists."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.return_value = [{"id": "bundle-123", "bundle_type": "DOMAIN"}]

        migration.migration_config["duplicate_bundle_action"] = "OVERWRITE"

        bundle_id, bundle_existed = migration.get_or_create_domain_bundle(
            mock_connection, "test-domain", "admin-user", "USER"
        )

        assert bundle_id is not None
        assert isinstance(bundle_id, str)
        assert bundle_existed is False

        # Should have deleted permissions, assets, and bundle, then created new bundle
        assert mock_cursor.execute.call_count >= 4

    def test_bundle_name_format(self, migration, mock_iam_db):
        """Test that bundle name is domain_id (not {domain_id}_default)."""
        mock_connection = Mock()
        mock_connection.cursor = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.return_value = []

        migration.get_or_create_domain_bundle(
            mock_connection, "my-domain", "admin-user", "USER"
        )

        # Check that query was called with domain_id (not my-domain_default)
        query_call_args = mock_iam_db.execute_query.call_args[0]
        assert "my-domain" in query_call_args[2]  # Should be in parameters


class TestAddDomainAsset:
    """Test add_domain_asset method."""

    def test_add_asset_success(self, migration):
        """Test successfully adding a domain asset."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        migration.add_domain_asset(mock_connection, "bundle-123", "test-domain")

        mock_cursor.execute.assert_called_once()
        # Verify parameters: bundle_id, domain_id
        call_args = mock_cursor.execute.call_args[0]
        assert "bundle-123" in call_args[1]
        assert "test-domain" in call_args[1]


class TestSetDomainPermissions:
    """Test set_domain_permissions method."""

    def test_set_permissions_success(self, migration):
        """Test successfully setting domain permissions."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5  # 5 actors affected
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        migration.set_domain_permissions(mock_connection, "bundle-123", "test-domain")

        mock_cursor.execute.assert_called_once()
        # Verify parameters: domain_id (5x), bundle_id (1x)
        call_args = mock_cursor.execute.call_args[0]
        assert call_args[1] == ("test-domain", "test-domain", "test-domain", "test-domain", "test-domain", "bundle-123")


class TestUpdateResourceBundlesParent:
    """Test update_resource_bundles_parent method."""

    def test_update_with_resource_bundles(self, migration, mock_iam_db):
        """Test updating parent_bundle_id when RESOURCE bundles exist."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3  # 3 bundles updated
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.return_value = [{"count": 3}]

        migration.update_resource_bundles_parent(mock_connection, "domain-bundle-123", "test-domain")

        mock_cursor.execute.assert_called_once()
        # Verify parameters: bundle_id, domain_id
        call_args = mock_cursor.execute.call_args[0]
        assert "domain-bundle-123" in call_args[1]
        assert "test-domain" in call_args[1]

    def test_update_no_resource_bundles(self, migration, mock_iam_db):
        """Test when no RESOURCE bundles exist."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"count": 0}]

        migration.update_resource_bundles_parent(mock_connection, "domain-bundle-123", "test-domain")

        # Should not update if no RESOURCE bundles exist
        mock_connection.cursor.assert_not_called()


class TestValidateDomainConfig:
    """Test validate_domain_config method."""

    def test_valid_config(self, migration):
        """Test validation with valid config."""
        config = {
            "domain_id": "test-domain",
            "owner_id": "admin",
            "owner_type": "USER"
        }
        assert migration.validate_domain_config(config) is True

    def test_missing_domain_id(self, migration):
        """Test validation with missing domain_id."""
        config = {
            "owner_id": "admin",
            "owner_type": "USER"
        }
        assert migration.validate_domain_config(config) is False

    def test_missing_owner_id(self, migration):
        """Test validation with missing owner_id."""
        config = {
            "domain_id": "test-domain",
            "owner_type": "USER"
        }
        assert migration.validate_domain_config(config) is False

    def test_invalid_owner_type(self, migration):
        """Test validation with invalid owner_type."""
        config = {
            "domain_id": "test-domain",
            "owner_id": "admin",
            "owner_type": "INVALID"
        }
        assert migration.validate_domain_config(config) is False


class TestMigrateDomain:
    """Test migrate_domain method."""

    def test_migrate_domain_success(self, migration, mock_iam_db):
        """Test successful domain migration."""
        mock_bundle_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        # Mock get_or_create_domain_bundle
        mock_bundle_conn.cursor = Mock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5
        mock_bundle_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_bundle_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.side_effect = [
            [],  # get_or_create_domain_bundle: bundle doesn't exist
            [{"count": 2}]  # update_resource_bundles_parent: 2 RESOURCE bundles exist
        ]

        domain_config = {
            "domain_id": "test-domain",
            "owner_id": "admin",
            "owner_type": "USER"
        }

        result = migration.migrate_domain(domain_config)

        assert result is True

    def test_migrate_domain_skip_mode(self, migration, mock_iam_db):
        """Test migration in SKIP mode with existing bundle."""
        mock_bundle_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.return_value = [{"id": "bundle-123", "bundle_type": "DOMAIN"}]
        migration.migration_config["duplicate_bundle_action"] = "SKIP"

        domain_config = {
            "domain_id": "test-domain",
            "owner_id": "admin",
            "owner_type": "USER"
        }

        result = migration.migrate_domain(domain_config)

        assert result is True

    def test_migrate_domain_invalid_config(self, migration):
        """Test migration with invalid config."""
        domain_config = {
            "domain_id": "test-domain"
            # Missing owner_id and owner_type
        }

        result = migration.migrate_domain(domain_config)

        assert result is False

    def test_migrate_domain_handles_errors(self, migration, mock_iam_db):
        """Test that errors during migration are handled."""
        mock_bundle_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        # Simulate error
        mock_iam_db.execute_query.side_effect = Exception("Database error")

        domain_config = {
            "domain_id": "test-domain",
            "owner_id": "admin",
            "owner_type": "USER"
        }

        result = migration.migrate_domain(domain_config)

        assert result is False


class TestRunMigration:
    """Test run_migration method."""

    def test_run_migration_multiple_domains(self, migration):
        """Test running migration for multiple domains."""
        migration.migration_config["domains"] = [
            {"domain_id": "domain-1", "owner_id": "user-1", "owner_type": "USER"},
            {"domain_id": "domain-2", "owner_id": "user-2", "owner_type": "USER"},
            {"domain_id": "domain-3", "owner_id": "user-3", "owner_type": "USER"}
        ]

        with patch.object(migration, 'migrate_domain', return_value=True) as mock_migrate:
            result = migration.run_migration()

            assert result is True
            assert mock_migrate.call_count == 3

    def test_run_migration_with_failures(self, migration):
        """Test migration when some domains fail."""
        migration.migration_config["domains"] = [
            {"domain_id": "domain-1", "owner_id": "user-1", "owner_type": "USER"},
            {"domain_id": "domain-2", "owner_id": "user-2", "owner_type": "USER"},
            {"domain_id": "domain-3", "owner_id": "user-3", "owner_type": "USER"}
        ]

        with patch.object(migration, 'migrate_domain', side_effect=[True, False, True]) as mock_migrate:
            result = migration.run_migration()

            assert result is False
            assert mock_migrate.call_count == 3
