"""Integration tests for namespace migration."""

import pytest
from unittest.mock import Mock, MagicMock, patch
from ras_onboarding.namespace.migration import NamespaceMigration
from ras_onboarding.namespace.permission_assignment import PermissionAssignment


@pytest.mark.integration
class TestNamespaceMigrationIntegration:
    """Integration tests simulating real-world scenarios."""

    @pytest.fixture
    def integration_config(self):
        return {
            "migration": {
                "debug_mode": True,
                "dry_run": False,
                "duplicate_bundle_action": "OVERWRITE",
                "domains": [
                    {"domain_id": "domain-prod-123", "owner_id": "admin-user", "owner_type": "USER"},
                    {"domain_id": "domain-dev-456", "owner_id": "dev-user", "owner_type": "USER"}
                ],
                "resource_tables": [
                    {
                        "table": "lakehouse",
                        "namespace_column": "lakehouse_namespace",
                        "user_columns": ["created_by", "updated_by", "owner"]
                    },
                    {
                        "table": "spark_job",
                        "namespace_column": "namespace",
                        "user_columns": ["created_by", "owner"]
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
    def setup_migration_scenario(self, integration_config):
        iam_db = Mock()
        core_db = Mock()

        # Scenario: Domain with 3 namespaces
        namespaces = [
            {"id": "ns-default", "namespace": "default", "domain_id": "domain-prod-123"},
            {"id": "ns-analytics", "namespace": "analytics", "domain_id": "domain-prod-123"},
            {"id": "ns-ml", "namespace": "ml_workloads", "domain_id": "domain-prod-123"}
        ]

        # Users in domain
        domain_users = [
            {"username": "alice"},
            {"username": "bob"},
            {"username": "charlie"},
            {"username": "diana"},
            {"username": "eve"}
        ]

        # Setup core_db (asset) responses
        core_db.execute_query.side_effect = [
            namespaces,  # get_namespaces_for_domain
        ]

        # Setup iam_db (bundle) responses
        # For each namespace: get_namespace_mapping_id, check if bundle exists, get_users_for_namespace
        namespace_mappings_and_bundles = [
            [{"id": "map-default"}],  # mapping for default
            [],  # no existing bundle for default
            domain_users,  # users for default
            [{"id": "map-analytics"}],  # mapping for analytics
            [],  # no existing bundle for analytics
            domain_users,  # users for analytics
            [{"id": "map-ml"}],  # mapping for ml
            [],  # no existing bundle for ml
            domain_users,  # users for ml
        ]
        iam_db.execute_query.side_effect = namespace_mappings_and_bundles

        return iam_db, core_db, integration_config

    def test_multi_namespace_migration(self, setup_migration_scenario):
        iam_db, core_db, config = setup_migration_scenario

        # Setup transaction context managers
        bundle_conn = MagicMock()
        asset_conn = MagicMock()

        iam_db.get_transaction.return_value.__enter__ = Mock(return_value=bundle_conn)
        iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        core_db.get_connection.return_value.__enter__ = Mock(return_value=asset_conn)
        core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        cursor = MagicMock()
        bundle_conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        bundle_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        migration = NamespaceMigration(iam_db, core_db, config)
        domain_config = {"domain_id": "domain-prod-123", "owner_id": "admin-user", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True
        # 3 namespaces × (create bundle + add asset) = 6 cursor executions
        assert cursor.execute.call_count == 6
        # 3 namespaces × 5 users = 15 permission inserts
        assert iam_db.execute_insert.call_count == 15

        bundle_conn.rollback.assert_not_called()

    def test_migration_dry_run_doesnt_commit(self):
        """Test that dry run mode doesn't commit changes."""
        iam_db = Mock()
        core_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "dry_run": True,  # Dry run enabled
                "duplicate_bundle_action": "FAIL",
                "domains": [{"domain_id": "domain-123", "owner_id": "owner-123", "owner_type": "USER"}],
                "resource_tables": [
                    {
                        "table": "lakehouse",
                        "namespace_column": "lakehouse_namespace",
                        "user_columns": ["owner"]
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

        namespaces = [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}]
        users = [{"username": "user1"}]

        core_db.execute_query.side_effect = [
            namespaces,
        ]

        iam_db.execute_query.side_effect = [
            [{"id": "map-1"}],  # get_namespace_mapping_id
            [],  # bundle doesn't exist
            users,  # get_users_for_namespace
        ]

        bundle_conn = MagicMock()
        asset_conn = MagicMock()

        iam_db.get_transaction.return_value.__enter__ = Mock(return_value=bundle_conn)
        iam_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        core_db.get_connection.return_value.__enter__ = Mock(return_value=asset_conn)
        core_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        cursor = MagicMock()
        bundle_conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        bundle_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        migration = NamespaceMigration(iam_db, core_db, config)
        domain_config = {"domain_id": "domain-123", "owner_id": "owner-123", "owner_type": "USER"}
        result = migration.migrate_domain(domain_config)

        assert result is True
        bundle_conn.rollback.assert_called_once()


@pytest.mark.integration
class TestPermissionAssignmentIntegration:
    """Integration tests for permission assignment."""

    def test_permission_assignment_handles_errors_gracefully(self):
        """Test that permission errors for individual users don't stop the process."""
        iam_db = Mock()
        core_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "resource_tables": []
            },
            "asset_mappings": {
                "NAMESPACE": {
                    "permissions": ["USE"]
                }
            }
        }

        # Simulate errors for some users
        iam_db.execute_insert.side_effect = [
            None,  # user1 succeeds
            Exception("Permission denied"),  # user2 fails
            None,  # user3 succeeds
            Exception("User not found"),  # user4 fails
            None   # user5 succeeds
        ]

        pa = PermissionAssignment(iam_db, core_db, config)
        connection = Mock()
        users = {"user1", "user2", "user3", "user4", "user5"}

        pa.set_namespace_permissions(connection, "bundle-123", "ns-123", users)
        assert iam_db.execute_insert.call_count == 5

    def test_get_users_for_namespace(self):
        """Test getting users for a namespace from IAM database."""
        iam_db = Mock()
        core_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "resource_tables": []
            },
            "asset_mappings": {
                "NAMESPACE": {
                    "permissions": ["USE"]
                }
            }
        }

        # Users in domain from IAM
        iam_db.execute_query.return_value = [
            {"username": "user1"},
            {"username": "user2"},
            {"username": "user3"}
        ]

        pa = PermissionAssignment(iam_db, core_db, config)
        connection = Mock()

        users = pa.get_users_for_namespace(connection, "default", "domain-123")
        assert len(users) == 3
        assert users == {"user1", "user2", "user3"}
