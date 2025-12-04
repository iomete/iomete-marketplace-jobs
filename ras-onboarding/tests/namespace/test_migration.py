"""Test cases for namespace migration module."""

import pytest
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

        bundle_id, bundle_existed = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert bundle_id is not None
        assert isinstance(bundle_id, str)
        assert bundle_existed is False
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

        bundle_id, bundle_created = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert bundle_id is None
        assert bundle_created is False

    def test_existing_bundle_update_mode(self, migration, mock_bundle_db):
        """Test UPDATE mode when bundle already exists."""
        mock_connection = Mock()
        mock_bundle_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "UPDATE"

        bundle_id, bundle_existed = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert bundle_id == "bundle-123"
        assert bundle_existed is True

        mock_connection.cursor.assert_not_called()

    def test_update_mode_duplicate_asset_error(self, migration, mock_bundle_db):
        """Test that UPDATE mode raises error when namespace asset already exists in bundle."""
        mock_connection = Mock()

        mock_bundle_db.execute_query.return_value = [{"exists": 1}]

        with pytest.raises(ValueError) as exc_info:
            migration.add_namespace_asset_to_bundle(
                mock_connection, "bundle-123", "namespace-456", validate_bundle_uniqueness=True
            )

        assert "already exists" in str(exc_info.value)
        assert "Cannot add duplicate namespace asset" in str(exc_info.value)

    def test_update_mode_add_new_asset_success(self, migration, mock_bundle_db):
        """Test that UPDATE mode successfully adds new namespace asset when it doesn't exist."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_bundle_db.execute_query.return_value = []

        migration.add_namespace_asset_to_bundle(
            mock_connection, "bundle-123", "namespace-456", validate_bundle_uniqueness=True
        )

        mock_cursor.execute.assert_called_once()

    def test_existing_bundle_overwrite_mode(self, migration, mock_bundle_db):
        """Test OVERWRITE mode when bundle already exists."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_bundle_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "OVERWRITE"

        bundle_id, bundle_existed = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert bundle_id is not None
        assert isinstance(bundle_id, str)
        assert bundle_existed is False

        assert mock_cursor.execute.call_count >= 3

    def test_bundle_name_format(self, migration, mock_bundle_db):
        """Test that bundle name follows correct format."""
        mock_connection = Mock()
        mock_connection.cursor = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_bundle_db.execute_query.return_value = []

        bundle_id, bundle_existed = migration.get_or_create_namespace_bundle(
            mock_connection, "my_namespace", "domain-123", "user-123", "USER"
        )

        query_call_args = mock_bundle_db.execute_query.call_args[0]
        assert "namespace-domain-123-my_namespace" in query_call_args[2]
        assert bundle_existed is False


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


class TestMigrateDomain:
    """Test migrate_domain method."""

    def test_migrate_domain_success(self, migration, mock_bundle_db, mock_asset_db):
        """Test successful domain migration."""

        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        mock_asset_db.execute_query.side_effect = [
            [{"created_by": "user-123"}],
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],
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
            [{"created_by": "user-123"}],
            []
        ]

        result = migration.migrate_domain("domain-123")

        assert result is True

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
            [{"id": "mapping-123"}],
            [{"id": "bundle-123"}]
        ]

        result = migration.migrate_domain("domain-123")

        assert result is True


class TestRunMigration:
    """Test run_migration method."""

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
