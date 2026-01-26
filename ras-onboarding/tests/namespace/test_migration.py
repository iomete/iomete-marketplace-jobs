"""Test cases for namespace migration module."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from ras_onboarding.namespace.migration import NamespaceMigration


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
            "domains": [{"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}],
            "resource_tables": [
                {
                    "table": "lakehouse",
                    "namespace_column": "lakehouse_namespace",
                    "user_columns": ["created_by", "updated_by"]
                }
            ]
        },
        "asset_mappings": {
            "NAMESPACE": {
                "permissions": ["USE"]
            }
        },
        "namespace_config": {
            "table": "lakehouse_namespace",
            "id_column": "namespace_id",
            "namespace_column": "name",
            "domain_column": "domain"
        }
    }


@pytest.fixture
def migration(mock_iam_db, mock_core_db, sample_config):
    """Create NamespaceMigration instance with mocks."""
    return NamespaceMigration(mock_iam_db, mock_core_db, sample_config)


class TestGetNamespaceMappingId:
    """Test get_namespace_mapping_id method."""

    def test_get_existing_namespace_mapping(self, migration, mock_core_db):
        """Test getting an existing namespace mapping ID."""
        mock_connection = Mock()
        mock_core_db.execute_query.return_value = [{"id": "mapping-123"}]

        result = migration.get_namespace_mapping_id(mock_connection, "domain-123", "default")

        assert result == "mapping-123"
        mock_core_db.execute_query.assert_called_once()

    def test_get_namespace_mapping_not_found(self, migration, mock_core_db):
        """Test error when namespace mapping doesn't exist."""
        mock_connection = Mock()
        mock_core_db.execute_query.return_value = []

        with pytest.raises(ValueError) as exc_info:
            migration.get_namespace_mapping_id(mock_connection, "domain-123", "default")

        assert "Namespace mapping not found" in str(exc_info.value)


class TestGetOrCreateNamespaceBundle:
    """Test get_or_create_namespace_bundle method."""

    def test_create_new_bundle(self, migration, mock_iam_db):
        """Test creating a new namespace bundle."""
        mock_connection = Mock()
        mock_connection.cursor = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.return_value = []  # Bundle doesn't exist

        bundle_id, bundle_existed = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert bundle_id is not None
        assert isinstance(bundle_id, str)
        assert bundle_existed is False
        mock_cursor.execute.assert_called_once()

    def test_existing_bundle_fail_mode(self, migration, mock_iam_db):
        """Test FAIL mode when bundle already exists."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "FAIL"

        with pytest.raises(ValueError) as exc_info:
            migration.get_or_create_namespace_bundle(
                mock_connection, "default", "domain-123", "user-123", "USER"
            )

        assert "already exists" in str(exc_info.value)

    def test_existing_bundle_skip_mode(self, migration, mock_iam_db):
        """Test SKIP mode when bundle already exists."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "SKIP"

        bundle_id, bundle_created = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert bundle_id is None
        assert bundle_created is False

    def test_existing_bundle_update_mode(self, migration, mock_iam_db):
        """Test UPDATE mode when bundle already exists."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "UPDATE"

        bundle_id, bundle_existed = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
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

        mock_iam_db.execute_query.return_value = [{"id": "bundle-123"}]

        migration.migration_config["duplicate_bundle_action"] = "OVERWRITE"

        bundle_id, bundle_existed = migration.get_or_create_namespace_bundle(
            mock_connection, "default", "domain-123", "user-123", "USER"
        )

        assert bundle_id is not None
        assert isinstance(bundle_id, str)
        assert bundle_existed is False

        assert mock_cursor.execute.call_count >= 3

    def test_bundle_name_format(self, migration, mock_iam_db):
        """Test that bundle name follows correct format."""
        mock_connection = Mock()
        mock_connection.cursor = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.return_value = []

        bundle_id, bundle_existed = migration.get_or_create_namespace_bundle(
            mock_connection, "my_namespace", "domain-123", "user-123", "USER"
        )

        query_call_args = mock_iam_db.execute_query.call_args[0]
        assert "namespace-domain-123-my_namespace" in query_call_args[2]
        assert bundle_existed is False


class TestAddNamespaceAssetToBundle:
    """Test add_namespace_asset_to_bundle method."""

    def test_add_new_asset_success(self, migration, mock_iam_db):
        """Test successfully adding a new namespace asset to bundle."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        mock_iam_db.execute_query.return_value = []

        migration.add_namespace_asset_to_bundle(
            mock_connection, "bundle-123", "namespace-456",
            "domain-123", "default", validate_bundle_uniqueness=False
        )

        mock_cursor.execute.assert_called_once()

    def test_update_mode_duplicate_asset_error(self, migration, mock_iam_db):
        """Test that UPDATE mode raises error when namespace asset already exists in a different bundle."""
        mock_connection = Mock()

        # Asset exists in a different bundle
        mock_iam_db.execute_query.return_value = [{"name": "namespace-other-domain-other"}]

        with pytest.raises(ValueError) as exc_info:
            migration.add_namespace_asset_to_bundle(
                mock_connection, "bundle-123", "namespace-456",
                "domain-123", "default", validate_bundle_uniqueness=True
            )

        assert "already exists" in str(exc_info.value)
        assert "Cannot add duplicate namespace asset" in str(exc_info.value)

    def test_update_mode_same_bundle_no_error(self, migration, mock_iam_db):
        """Test that UPDATE mode does not raise error when asset exists in the same bundle."""
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_connection.cursor.return_value.__exit__ = Mock(return_value=False)

        # Asset exists in the expected bundle (same namespace-domain combination)
        mock_iam_db.execute_query.return_value = [{"name": "namespace-domain-123-default"}]

        migration.add_namespace_asset_to_bundle(
            mock_connection, "bundle-123", "namespace-456",
            "domain-123", "default", validate_bundle_uniqueness=True
        )

        mock_cursor.execute.assert_called_once()


class TestGetNamespacesForDomain:
    """Test get_namespaces_for_domain method."""

    def test_get_namespaces_success(self, migration, mock_core_db):
        """Test successfully getting namespaces for a domain."""
        mock_connection = Mock()
        mock_core_db.execute_query.return_value = [
            {"id": "ns-1", "namespace": "default", "domain_id": "domain-123"},
            {"id": "ns-2", "namespace": "dev", "domain_id": "domain-123"}
        ]

        result = migration.get_namespaces_for_domain(mock_connection, "domain-123")

        assert len(result) == 2
        assert result[0]["namespace"] == "default"
        assert result[1]["namespace"] == "dev"

    def test_get_namespaces_empty_domain(self, migration, mock_core_db):
        """Test getting namespaces for domain with no namespaces."""
        mock_connection = Mock()
        mock_core_db.execute_query.return_value = []

        result = migration.get_namespaces_for_domain(mock_connection, "domain-123")

        assert len(result) == 0


class TestMigrateDomain:
    """Test migrate_domain method."""

    def test_migrate_domain_success(self, migration, mock_iam_db, mock_core_db):
        """Test successful domain migration with users and groups."""

        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries: get_namespaces_for_domain, get_namespace_mapping_id
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries: check bundle exists, get users, get groups
        mock_iam_db.execute_query.side_effect = [
            [],  # check if bundle exists
            [{"username": "user1"}],  # get_users_for_namespace
            [{"groupname": "developers"}],  # get_groups_for_namespace
        ]

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True

    def test_migrate_domain_no_namespaces(self, migration, mock_iam_db, mock_core_db):
        """Test migration when domain has no namespaces."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.execute_query.side_effect = [
            []  # No namespaces
        ]

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True

    def test_migrate_domain_handles_errors(self, migration, mock_iam_db, mock_core_db):
        """Test that errors during migration are handled."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # Simulate error
        mock_core_db.execute_query.side_effect = Exception("Database error")

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is False

    def test_migrate_domain_skip_existing_bundle(self, migration, mock_iam_db, mock_core_db):
        """Test that existing bundles are skipped in SKIP mode."""
        migration.migration_config["duplicate_bundle_action"] = "SKIP"

        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries
        mock_iam_db.execute_query.side_effect = [
            [{"id": "bundle-123"}]  # bundle exists
        ]

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True

    def test_migrate_domain_with_users_and_groups(self, migration, mock_iam_db, mock_core_db):
        """Test migration with both users and groups in domain."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries
        mock_iam_db.execute_query.side_effect = [
            [],  # check if bundle exists
            [{"username": "user1"}, {"username": "user2"}],  # get_users_for_namespace
            [{"groupname": "developers"}, {"groupname": "analysts"}],  # get_groups_for_namespace
        ]

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True
        # Should insert permissions for 2 users + 2 groups = 4 total
        assert mock_iam_db.execute_insert.call_count == 4

    def test_migrate_domain_groups_only(self, migration, mock_iam_db, mock_core_db):
        """Test migration with only groups (no direct users) in domain."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries
        mock_iam_db.execute_query.side_effect = [
            [],  # check if bundle exists
            [],  # get_users_for_namespace - no users
            [{"groupname": "developers"}],  # get_groups_for_namespace
        ]

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True
        # Should insert permissions for 1 group only
        assert mock_iam_db.execute_insert.call_count == 1

    def test_migrate_domain_users_only(self, migration, mock_iam_db, mock_core_db):
        """Test migration with only users (no groups) in domain."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries
        mock_iam_db.execute_query.side_effect = [
            [],  # check if bundle exists
            [{"username": "user1"}],  # get_users_for_namespace
            [],  # get_groups_for_namespace - no groups
        ]

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True
        # Should insert permissions for 1 user only
        assert mock_iam_db.execute_insert.call_count == 1

    def test_migrate_domain_no_users_no_groups(self, migration, mock_iam_db, mock_core_db):
        """Test migration when domain has no users or groups."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries
        mock_iam_db.execute_query.side_effect = [
            [],  # check if bundle exists
            [],  # get_users_for_namespace - no users
            [],  # get_groups_for_namespace - no groups
        ]

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True
        # No permissions should be inserted
        assert mock_iam_db.execute_insert.call_count == 0


class TestActorTypePermissions:
    """Test that actor_type is correctly set for users and groups."""

    def test_user_permission_has_user_actor_type(self, migration, mock_iam_db, mock_core_db):
        """Verify that user permissions are created with actor_type='USER'."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries
        mock_iam_db.execute_query.side_effect = [
            [],  # check if bundle exists
            [{"username": "alice"}],  # get_users_for_namespace
            [],  # get_groups_for_namespace
        ]

        # Track insert calls
        insert_calls = []
        mock_iam_db.execute_insert.side_effect = lambda conn, query, params: insert_calls.append(params)

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        migration.migrate_domain(domain_config)

        # Verify user permission has correct actor_type
        assert len(insert_calls) == 1
        user_permission = insert_calls[0]
        assert user_permission[1] == 'USER'  # actor_type
        assert user_permission[2] == 'alice'  # actor_id

    def test_group_permission_has_group_actor_type(self, migration, mock_iam_db, mock_core_db):
        """Verify that group permissions are created with actor_type='GROUP'."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries
        mock_iam_db.execute_query.side_effect = [
            [],  # check if bundle exists
            [],  # get_users_for_namespace
            [{"groupname": "developers"}],  # get_groups_for_namespace
        ]

        # Track insert calls
        insert_calls = []
        mock_iam_db.execute_insert.side_effect = lambda conn, query, params: insert_calls.append(params)

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        migration.migrate_domain(domain_config)

        # Verify group permission has correct actor_type
        assert len(insert_calls) == 1
        group_permission = insert_calls[0]
        assert group_permission[1] == 'GROUP'  # actor_type
        assert group_permission[2] == 'developers'  # actor_id

    def test_mixed_permissions_have_correct_actor_types(self, migration, mock_iam_db, mock_core_db):
        """Verify that both user and group permissions have correct actor_types."""
        mock_bundle_conn = MagicMock()
        mock_asset_conn = MagicMock()

        mock_iam_db.get_transaction.return_value.__enter__ = Mock(return_value=mock_bundle_conn)
        mock_iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        mock_core_db.get_connection.return_value.__enter__ = Mock(return_value=mock_asset_conn)
        mock_core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # core_db queries
        mock_core_db.execute_query.side_effect = [
            [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}],  # get_namespaces
            [{"id": "mapping-123"}],  # get_namespace_mapping_id
        ]

        # iam_db queries
        mock_iam_db.execute_query.side_effect = [
            [],  # check if bundle exists
            [{"username": "alice"}, {"username": "bob"}],  # get_users_for_namespace
            [{"groupname": "developers"}, {"groupname": "analysts"}],  # get_groups_for_namespace
        ]

        # Track insert calls
        insert_calls = []
        mock_iam_db.execute_insert.side_effect = lambda conn, query, params: insert_calls.append(params)

        domain_config = {"domain_id": "domain-123", "owner_id": "user-123", "owner_type": "USER"}
        migration.migrate_domain(domain_config)

        # Should have 4 inserts: 2 users + 2 groups
        assert len(insert_calls) == 4

        # First two should be users
        user_permissions = [call for call in insert_calls if call[1] == 'USER']
        assert len(user_permissions) == 2
        assert user_permissions[0][2] in ['alice', 'bob']
        assert user_permissions[1][2] in ['alice', 'bob']

        # Last two should be groups
        group_permissions = [call for call in insert_calls if call[1] == 'GROUP']
        assert len(group_permissions) == 2
        assert group_permissions[0][2] in ['developers', 'analysts']
        assert group_permissions[1][2] in ['developers', 'analysts']


class TestRunMigration:
    """Test run_migration method."""

    def test_run_migration_multiple_domains(self, migration, mock_iam_db, mock_core_db):
        """Test running migration for multiple domains."""
        migration.migration_config["domains"] = [
            {"domain_id": "domain-1", "owner_id": "user-1"},
            {"domain_id": "domain-2", "owner_id": "user-2"},
            {"domain_id": "domain-3", "owner_id": "user-3"}
        ]

        with patch.object(migration, 'migrate_domain', return_value=True) as mock_migrate:
            result = migration.run_migration()

            assert result is True
            assert mock_migrate.call_count == 3

    def test_run_migration_with_failures(self, migration, mock_iam_db, mock_core_db):
        """Test migration when some domains fail."""
        migration.migration_config["domains"] = [
            {"domain_id": "domain-1", "owner_id": "user-1"},
            {"domain_id": "domain-2", "owner_id": "user-2"},
            {"domain_id": "domain-3", "owner_id": "user-3"}
        ]

        with patch.object(migration, 'migrate_domain', side_effect=[True, False, True]) as mock_migrate:
            result = migration.run_migration()

            assert result is False
            assert mock_migrate.call_count == 3
