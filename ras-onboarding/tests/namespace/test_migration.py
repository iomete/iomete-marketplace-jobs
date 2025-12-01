"""Test cases for namespace migration module."""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch, call
from ras_onboarding.namespace.migration import NamespaceMigration


@pytest.fixture
def mock_bundle_db():
    """Mock bundle database manager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    db.execute_insert = Mock()
    db.get_connection = Mock()
    db.get_transaction = Mock()
    return db


@pytest.fixture
def mock_asset_db():
    """Mock asset database manager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    db.get_connection = Mock()
    return db


@pytest.fixture
def mock_domain_db():
    """Mock domain database manager."""
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
            "namespace_permissions": ["READ", "WRITE"],
            "domains": ["domain-123"],
            "resource_tables": [
                {
                    "table": "lakehouse",
                    "namespace_column": "lakehouse_namespace",
                    "user_columns": ["created_by", "updated_by"]
                }
            ]
        },
        "namespace_config": {
            "table": "lakehouse_namespace",
            "id_column": "namespace_id",
            "namespace_column": "name",
            "domain_column": "domain"
        }
    }


@pytest.fixture
def migration(mock_bundle_db, mock_asset_db, mock_domain_db, sample_config):
    """Create NamespaceMigration instance with mocks."""
    return NamespaceMigration(mock_bundle_db, mock_asset_db, mock_domain_db, sample_config)


class TestNamespaceMigrationInit:
    """Test NamespaceMigration initialization."""

    def test_init_with_valid_config(self, mock_bundle_db, mock_asset_db, mock_domain_db, sample_config):
        """Test initialization with valid configuration."""
        nm = NamespaceMigration(mock_bundle_db, mock_asset_db, mock_domain_db, sample_config)

        assert nm.bundle_db == mock_bundle_db
        assert nm.asset_db == mock_asset_db
        assert nm.domain_db == mock_domain_db
        assert nm.config == sample_config
        assert nm.migration_config == sample_config["migration"]
        assert nm.namespace_config == sample_config["namespace_config"]
        assert nm.debug_mode is False
        assert nm.permission_assignment is not None

    def test_init_with_debug_mode(self, mock_bundle_db, mock_asset_db, mock_domain_db, sample_config):
        """Test initialization with debug mode enabled."""
        sample_config["migration"]["debug_mode"] = True
        nm = NamespaceMigration(mock_bundle_db, mock_asset_db, mock_domain_db, sample_config)

        assert nm.debug_mode is True


class TestGetNamespaceMappingId:
    """Test get_namespace_mapping_id method."""

    def test_get_existing_namespace_mapping(self, migration, mock_bundle_db):
        """Test getting an existing namespace mapping ID."""
        mock_connection = Mock()
        mock_bundle_db.execute_query.return_value = [{"id": "mapping-123"}]

        result = migration.get_namespace_mapping_id(mock_connection, "domain-123", "default")

        assert result == "mapping-123"
        mock_bundle_db.execute_query.assert_called_once()

    def test_get_namespace_mapping_not_found(self, migration, mock_bundle_db):
        """Test error when namespace mapping doesn't exist."""
        mock_connection = Mock()
        mock_bundle_db.execute_query.return_value = []

        with pytest.raises(ValueError) as exc_info:
            migration.get_namespace_mapping_id(mock_connection, "domain-123", "default")

        assert "Namespace mapping not found" in str(exc_info.value)

    def test_get_namespace_mapping_correct_query_params(self, migration, mock_bundle_db):
        """Test that correct parameters are passed to query."""
        mock_connection = Mock()
        mock_bundle_db.execute_query.return_value = [{"id": "mapping-123"}]

        migration.get_namespace_mapping_id(mock_connection, "domain-456", "my_namespace")

        call_args = mock_bundle_db.execute_query.call_args
        assert call_args[0][2] == ("domain-456", "my_namespace")


class TestGetDomainOwner:
    """Test get_domain_owner method."""

    def test_get_domain_owner_success(self, migration, mock_asset_db):
        """Test successfully getting domain owner."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = [{"created_by": "user-123"}]

        owner_id, owner_type = migration.get_domain_owner(mock_connection, "domain-123")

        assert owner_id == "user-123"
        assert owner_type == "USER"

    def test_get_domain_owner_domain_not_found(self, migration, mock_asset_db):
        """Test error when domain doesn't exist."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = []

        with pytest.raises(ValueError) as exc_info:
            migration.get_domain_owner(mock_connection, "domain-123")

        assert "Domain" in str(exc_info.value)
        assert "not found" in str(exc_info.value)

    def test_get_domain_owner_no_owners(self, migration, mock_asset_db):
        """Test when created_by is None."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = [{"created_by": None}]

        owner_id, owner_type = migration.get_domain_owner(mock_connection, "domain-123")

        assert owner_id is None
        assert owner_type == "USER"

    def test_get_domain_owner_empty_owners_list(self, migration, mock_asset_db):
        """Test error when created_by is empty string."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = [{"created_by": ""}]

        owner_id, owner_type = migration.get_domain_owner(mock_connection, "domain-123")

        assert owner_id == ""
        assert owner_type == "USER"

    def test_get_domain_owner_multiple_owners(self, migration, mock_asset_db):
        """Test that created_by user is returned."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = [{"created_by": "user-first"}]

        owner_id, owner_type = migration.get_domain_owner(mock_connection, "domain-123")

        assert owner_id == "user-first"
        assert owner_type == "USER"


class TestGetOrCreateNamespaceBundle:
    """Test get_or_create_namespace_bundle method."""

    def test_create_new_bundle(self, migration, mock_bundle_db):
        """Test creating a new namespace bundle."""
        mock_connection = Mock()
        mock_connection.cursor = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_bundle_db.execute_query.return_value = []  # Bundle doesn't exist

        result = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert result is not None
        assert isinstance(result, str)
        mock_cursor.execute.assert_called_once()

    def test_existing_bundle_fail_mode(self, migration, mock_bundle_db):
        """Test FAIL mode when bundle already exists."""
        mock_connection = Mock()
        mock_bundle_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "FAIL"

        with pytest.raises(ValueError) as exc_info:
            migration.get_or_create_namespace_bundle(
                mock_connection, "default", "domain-123", "user-123", "USER"
            )

        assert "already exists" in str(exc_info.value)

    def test_existing_bundle_skip_mode(self, migration, mock_bundle_db):
        """Test SKIP mode when bundle already exists."""
        mock_connection = Mock()
        mock_bundle_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "SKIP"

        result = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert result is None

    def test_existing_bundle_overwrite_mode(self, migration, mock_bundle_db):
        """Test OVERWRITE mode when bundle already exists."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_bundle_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "OVERWRITE"

        result = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert result is not None
        assert isinstance(result, str)
        # Verify deletion queries were executed
        assert mock_cursor.execute.call_count >= 3  # DELETE permissions, assets, bundle

    def test_bundle_name_format(self, migration, mock_bundle_db):
        """Test that bundle name follows correct format."""
        mock_connection = Mock()
        mock_connection.cursor = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_bundle_db.execute_query.return_value = []

        migration.get_or_create_namespace_bundle(
            mock_connection, "my_namespace", "domain-123", "user-123", "USER"
        )

        # Check that query was called with correct bundle name (iomete-namespace-{namespace})
        query_call_args = mock_bundle_db.execute_query.call_args[0]
        assert "iomete-namespace-my_namespace" in query_call_args[2]


class TestGetNamespacesForDomain:
    """Test get_namespaces_for_domain method."""

    def test_get_namespaces_success(self, migration, mock_asset_db):
        """Test successfully getting namespaces for a domain."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = [
            {"id": "ns-1", "namespace": "default", "domain_id": "domain-123"},
            {"id": "ns-2", "namespace": "dev", "domain_id": "domain-123"}
        ]

        result = migration.get_namespaces_for_domain(mock_connection, "domain-123")

        assert len(result) == 2
        assert result[0]["namespace"] == "default"
        assert result[1]["namespace"] == "dev"

    def test_get_namespaces_empty_domain(self, migration, mock_asset_db):
        """Test getting namespaces for domain with no namespaces."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = []

        result = migration.get_namespaces_for_domain(mock_connection, "domain-123")

        assert len(result) == 0

    def test_get_namespaces_query_formatting(self, migration, mock_asset_db):
        """Test that namespace query is formatted correctly."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = []

        migration.get_namespaces_for_domain(mock_connection, "domain-123")

        # Verify query was formatted with namespace_config
        call_args = mock_asset_db.execute_query.call_args[0]
        query = call_args[1]
        assert "lakehouse_namespace" in query  # From namespace_config.table


class TestMigrateDomain:
    """Test migrate_domain method."""

    def test_migrate_domain_success(self, migration, mock_bundle_db, mock_asset_db):
        """Test successful domain migration."""
        # Setup mocks
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # Mock responses
        mock_asset_db.execute_query.side_effect = [
            [{"created_by": "user-123"}],  # get_domain_owner
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            []  # get_users_for_namespace (from resource tables)
        ]

        mock_bundle_db.execute_query.side_effect = [
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
            []  # check if bundle exists
        ]

        result = migration.migrate_domain("domain-123")

        assert result is True

    def test_migrate_domain_no_namespaces(self, migration, mock_bundle_db, mock_asset_db):
        """Test migration when domain has no namespaces."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.execute_query.side_effect = [
            [{"created_by": "user-123"}],  # get_domain_owner
            []  # get_namespaces - empty
        ]

        result = migration.migrate_domain("domain-123")

        assert result is True

    def test_migrate_domain_dry_run_mode(self, migration, mock_bundle_db, mock_asset_db):
        """Test dry run mode rolls back changes."""
        migration.migration_config["dry_run"] = True

        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.execute_query.side_effect = [
            [{"created_by": "user-123"}],  # get_domain_owner
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces (not empty)
            []  # get_users_for_namespace
        ]

        mock_bundle_db.execute_query.side_effect = [
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
            []  # check if bundle exists
        ]

        result = migration.migrate_domain("domain-123")

        assert result is True
        mock_bundle_conn.rollback.assert_called_once()

    def test_migrate_domain_handles_errors(self, migration, mock_bundle_db, mock_asset_db):
        """Test that errors during migration are handled."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # Simulate error
        mock_asset_db.execute_query.side_effect = Exception("Database error")

        result = migration.migrate_domain("domain-123")

        assert result is False

    def test_migrate_domain_skip_existing_bundle(self, migration, mock_bundle_db, mock_asset_db):
        """Test that existing bundles are skipped in SKIP mode."""
        migration.migration_config["duplicate_bundle_action"] = "SKIP"

        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.execute_query.side_effect = [
            [{"created_by": "user-123"}],  # get_domain_owner
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}]  # get_namespaces
        ]

        mock_bundle_db.execute_query.side_effect = [
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
            [{"id": "bundle-123"}]  # Bundle already exists
        ]

        result = migration.migrate_domain("domain-123")

        assert result is True


class TestRunMigration:
    """Test run_migration method."""

    def test_run_migration_single_domain(self, migration, mock_bundle_db, mock_asset_db):
        """Test running migration for a single domain."""
        with patch.object(migration, 'migrate_domain', return_value=True) as mock_migrate:
            result = migration.run_migration()

            assert result is True
            mock_migrate.assert_called_once_with("domain-123")

    def test_run_migration_multiple_domains(self, migration, mock_bundle_db, mock_asset_db):
        """Test running migration for multiple domains."""
        migration.migration_config["domains"] = ["domain-1", "domain-2", "domain-3"]

        with patch.object(migration, 'migrate_domain', return_value=True) as mock_migrate:
            result = migration.run_migration()

            assert result is True
            assert mock_migrate.call_count == 3

    def test_run_migration_with_failures(self, migration, mock_bundle_db, mock_asset_db):
        """Test migration when some domains fail."""
        migration.migration_config["domains"] = ["domain-1", "domain-2", "domain-3"]

        # Second domain fails
        with patch.object(migration, 'migrate_domain', side_effect=[True, False, True]) as mock_migrate:
            result = migration.run_migration()

            assert result is False
            assert mock_migrate.call_count == 3

    def test_run_migration_domain_objects(self, migration, mock_bundle_db, mock_asset_db):
        """Test migration with domain objects instead of strings."""
        migration.migration_config["domains"] = [
            {"domain_id": "domain-1"},
            {"domain_id": "domain-2"}
        ]

        with patch.object(migration, 'migrate_domain', return_value=True) as mock_migrate:
            result = migration.run_migration()

            assert result is True
            assert mock_migrate.call_count == 2
            mock_migrate.assert_any_call("domain-1")
            mock_migrate.assert_any_call("domain-2")

    def test_run_migration_empty_domains(self, migration, mock_bundle_db, mock_asset_db):
        """Test migration with no domains configured."""
        migration.migration_config["domains"] = []

        with patch.object(migration, 'migrate_domain', return_value=True) as mock_migrate:
            result = migration.run_migration()

            assert result is True
            mock_migrate.assert_not_called()

    def test_run_migration_all_failures(self, migration, mock_bundle_db, mock_asset_db):
        """Test migration when all domains fail."""
        migration.migration_config["domains"] = ["domain-1", "domain-2"]

        with patch.object(migration, 'migrate_domain', return_value=False) as mock_migrate:
            result = migration.run_migration()

            assert result is False
            assert mock_migrate.call_count == 2


class TestNamespaceMigrationIntegration:
    """Integration tests for NamespaceMigration."""

    def test_complete_migration_flow(self, migration, mock_bundle_db, mock_asset_db):
        """Test complete end-to-end migration flow."""
        # Setup complex scenario with multiple namespaces and users
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # Setup responses for multiple namespaces
        mock_asset_db.execute_query.side_effect = [
            [{"created_by": "owner-123"}],  # get_domain_owner
            [  # get_namespaces
                {"id": "ns-1", "namespace": "default", "domain_id": "domain-123"},
                {"id": "ns-2", "namespace": "dev", "domain_id": "domain-123"}
            ],
            [{"username": "user1"}],  # users for default namespace
            [{"username": "user2"}],  # users for dev namespace
        ]

        mock_bundle_db.execute_query.side_effect = [
            [{"id": "mapping-1"}],  # namespace mapping for default
            [],  # bundle doesn't exist for default
            [{"id": "mapping-2"}],  # namespace mapping for dev
            []  # bundle doesn't exist for dev
        ]

        # Mock cursor for bundle creation
        mock_cursor = MagicMock()
        mock_bundle_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_bundle_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        result = migration.migrate_domain("domain-123")

        assert result is True
        # Verify bundles were created and namespaces added to bundle_asset for both namespaces
        # 2 bundle creations + 2 bundle_asset insertions = 4 executions
        assert mock_cursor.execute.call_count == 4
