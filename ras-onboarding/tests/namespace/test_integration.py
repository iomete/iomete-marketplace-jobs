"""Integration tests for namespace migration."""

import pytest
import json
from unittest.mock import Mock, MagicMock, patch
from ras_onboarding.namespace.migration import NamespaceMigration
from ras_onboarding.namespace.permission_assignment import PermissionAssignment


@pytest.mark.integration
class TestNamespaceMigrationIntegration:
    """Integration tests simulating real-world scenarios."""

    @pytest.fixture
    def integration_config(self):
        """Configuration for integration tests."""
        return {
            "migration": {
                "debug_mode": True,
                "dry_run": False,
                "duplicate_bundle_action": "UPDATE",
                "namespace_permissions": ["READ", "WRITE", "EXECUTE"],
                "domains": ["domain-prod-123", "domain-dev-456"],
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
            "namespace_config": {
                "table": "lakehouse_namespace",
                "id_column": "namespace_id",
                "namespace_column": "name",
                "domain_column": "domain"
            }
        }

    @pytest.fixture
    def setup_migration_scenario(self, integration_config):
        """Setup a complete migration scenario with mocked database responses."""
        bundle_db = Mock()
        asset_db = Mock()

        # Scenario: Domain with 3 namespaces, multiple users, some with overlapping resources
        domain_owners = json.dumps(["admin-user"])

        namespaces = [
            {"id": "ns-default", "namespace": "default", "domain_id": "domain-prod-123"},
            {"id": "ns-analytics", "namespace": "analytics", "domain_id": "domain-prod-123"},
            {"id": "ns-ml", "namespace": "ml_workloads", "domain_id": "domain-prod-123"}
        ]

        # Users in different namespaces
        default_users = [
            {"username": "alice"},
            {"username": "bob"},
            {"username": "charlie"}
        ]

        analytics_users = [
            {"username": "bob"},  # Bob works in both default and analytics
            {"username": "diana"}
        ]

        ml_users = [
            {"username": "eve"},
            {"username": "alice"}  # Alice works in default and ml
        ]

        # Setup asset_db responses
        # Note: get_users_for_namespace uses UNION queries, so 1 call per table per namespace
        # We have 2 resource tables (lakehouse, spark_job) × 3 namespaces = 6 user query calls
        asset_db.execute_query.side_effect = [
            [{"owners": domain_owners}],  # get_domain_owner
            namespaces,  # get_namespaces_for_domain
            default_users,  # users from lakehouse table (UNION query) for default namespace
            default_users,  # users from spark_job table (UNION query) for default namespace
            analytics_users,  # users from lakehouse table (UNION query) for analytics
            analytics_users,  # users from spark_job table (UNION query) for analytics
            ml_users,  # users from lakehouse table (UNION query) for ml
            ml_users   # users from spark_job table (UNION query) for ml
        ]

        # Setup bundle_db responses
        namespace_mappings_and_bundles = [
            [{"id": "map-default"}],  # mapping for default
            [],  # no existing bundle for default
            [{"id": "map-analytics"}],  # mapping for analytics
            [],  # no existing bundle for analytics
            [{"id": "map-ml"}],  # mapping for ml
            []  # no existing bundle for ml
        ]
        bundle_db.execute_query.side_effect = namespace_mappings_and_bundles

        return bundle_db, asset_db, integration_config

    def test_multi_namespace_migration(self, setup_migration_scenario):
        """Test migrating domain with multiple namespaces and overlapping users."""
        bundle_db, asset_db, config = setup_migration_scenario

        # Setup transaction context managers
        bundle_conn = MagicMock()
        asset_conn = MagicMock()

        bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=bundle_conn)
        bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_conn)
        asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        # Setup cursor for bundle creation
        cursor = MagicMock()
        bundle_conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        bundle_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        # Create migration instance and run
        migration = NamespaceMigration(bundle_db, asset_db, config)
        result = migration.migrate_domain("domain-prod-123")

        # Assertions
        assert result is True

        # Verify 3 bundles were created and 3 namespace-bundle links added
        # 3 bundle creations + 3 bundle_asset insertions = 6 cursor executions
        assert cursor.execute.call_count == 6

        # Verify permissions were set
        # default: 3 users (alice, bob, charlie)
        # analytics: 2 users (bob, diana)
        # ml: 2 users (eve, alice)
        # Total: 7 permission assignments (some users in multiple namespaces)
        assert bundle_db.execute_insert.call_count == 7

        # Verify transaction was committed (not rolled back)
        bundle_conn.rollback.assert_not_called()

    def test_migration_with_existing_bundles_update_mode(self):
        """Test migration when bundles exist and update mode is enabled."""
        bundle_db = Mock()
        asset_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "dry_run": False,
                "duplicate_bundle_action": "UPDATE",
                "namespace_permissions": ["READ"],
                "domains": ["domain-123"],
                "resource_tables": [
                    {
                        "table": "lakehouse",
                        "namespace_column": "lakehouse_namespace",
                        "user_columns": ["owner"]
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

        # Setup responses
        owners = json.dumps(["owner-123"])
        namespaces = [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}]
        users = [{"username": "user1"}]

        asset_db.execute_query.side_effect = [
            [{"owners": owners}],
            namespaces,
            users
        ]

        # Bundle already exists
        bundle_db.execute_query.side_effect = [
            [{"id": "map-1"}],
            [{"id": "existing-bundle-123"}]  # Existing bundle
        ]

        # Setup connections
        bundle_conn = MagicMock()
        asset_conn = MagicMock()

        bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=bundle_conn)
        bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_conn)
        asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        migration = NamespaceMigration(bundle_db, asset_db, config)
        result = migration.migrate_domain("domain-123")

        assert result is True
        # Permissions should still be set for existing bundle
        assert bundle_db.execute_insert.call_count == 1

    def test_migration_dry_run_doesnt_commit(self):
        """Test that dry run mode doesn't commit changes."""
        bundle_db = Mock()
        asset_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "dry_run": True,  # Dry run enabled
                "duplicate_bundle_action": "FAIL",
                "namespace_permissions": ["READ"],
                "domains": ["domain-123"],
                "resource_tables": [
                    {
                        "table": "lakehouse",
                        "namespace_column": "lakehouse_namespace",
                        "user_columns": ["owner"]
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

        owners = json.dumps(["owner-123"])
        namespaces = [{"id": "ns-1", "namespace": "default", "domain_id": "domain-123"}]
        users = [{"username": "user1"}]

        asset_db.execute_query.side_effect = [
            [{"owners": owners}],
            namespaces,
            users
        ]

        bundle_db.execute_query.side_effect = [
            [{"id": "map-1"}],
            []
        ]

        bundle_conn = MagicMock()
        asset_conn = MagicMock()

        bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=bundle_conn)
        bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_conn)
        asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        cursor = MagicMock()
        bundle_conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        bundle_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        migration = NamespaceMigration(bundle_db, asset_db, config)
        result = migration.migrate_domain("domain-123")

        assert result is True
        # Verify rollback was called for dry run
        bundle_conn.rollback.assert_called_once()

    def test_migration_handles_partial_failures(self):
        """Test that migration continues when some operations fail."""
        bundle_db = Mock()
        asset_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "dry_run": False,
                "duplicate_bundle_action": "FAIL",
                "namespace_permissions": ["READ"],
                "domains": ["domain-123"],
                "resource_tables": [
                    {
                        "table": "lakehouse",
                        "namespace_column": "lakehouse_namespace",
                        "user_columns": ["owner"]
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

        owners = json.dumps(["owner-123"])
        namespaces = [
            {"id": "ns-1", "namespace": "default", "domain_id": "domain-123"},
            {"id": "ns-2", "namespace": "dev", "domain_id": "domain-123"}
        ]

        # One namespace has users, other fails
        asset_db.execute_query.side_effect = [
            [{"owners": owners}],
            namespaces,
            [{"username": "user1"}],  # Users for first namespace
            Exception("Query failed")  # Second namespace query fails
        ]

        # First namespace succeeds
        bundle_db.execute_query.side_effect = [
            [{"id": "map-1"}],
            [],
            [{"id": "map-2"}],
            []
        ]

        bundle_conn = MagicMock()
        asset_conn = MagicMock()

        bundle_db.get_transaction.return_value.__enter__ = Mock(return_value=bundle_conn)
        bundle_db.get_transaction.return_value.__exit__ = Mock(return_value=False)

        asset_db.get_connection.return_value.__enter__ = Mock(return_value=asset_conn)
        asset_db.get_connection.return_value.__exit__ = Mock(return_value=False)

        cursor = MagicMock()
        bundle_conn.cursor.return_value.__enter__ = Mock(return_value=cursor)
        bundle_conn.cursor.return_value.__exit__ = Mock(return_value=False)

        migration = NamespaceMigration(bundle_db, asset_db, config)
        result = migration.migrate_domain("domain-123")

        # Migration continues despite one failure
        assert result is True

    def test_run_migration_multiple_domains(self):
        """Test running migration across multiple domains."""
        bundle_db = Mock()
        asset_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "dry_run": False,
                "duplicate_bundle_action": "SKIP",
                "namespace_permissions": ["READ"],
                "domains": ["domain-1", "domain-2", "domain-3"],
                "resource_tables": [
                    {
                        "table": "lakehouse",
                        "namespace_column": "lakehouse_namespace",
                        "user_columns": ["owner"]
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

        migration = NamespaceMigration(bundle_db, asset_db, config)

        # Mock migrate_domain to track calls
        with patch.object(migration, 'migrate_domain', return_value=True) as mock_migrate:
            result = migration.run_migration()

            assert result is True
            assert mock_migrate.call_count == 3
            mock_migrate.assert_any_call("domain-1")
            mock_migrate.assert_any_call("domain-2")
            mock_migrate.assert_any_call("domain-3")


@pytest.mark.integration
class TestPermissionAssignmentIntegration:
    """Integration tests for permission assignment."""

    def test_user_deduplication_across_tables(self):
        """Test that users are deduplicated when found in multiple resource tables."""
        bundle_db = Mock()
        asset_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "resource_tables": [
                    {
                        "table": "lakehouse",
                        "namespace_column": "lakehouse_namespace",
                        "user_columns": ["created_by", "owner"]
                    },
                    {
                        "table": "spark_job",
                        "namespace_column": "namespace",
                        "user_columns": ["owner"]
                    }
                ],
                "namespace_permissions": ["READ"]
            }
        }

        # Mock returns one result per resource table (UNION query combines all user_columns per table)
        asset_db.execute_query.side_effect = [
            # lakehouse table: UNION of created_by and owner columns
            [{"username": "user1"}, {"username": "user2"}],
            # spark_job table: owner column
            [{"username": "user2"}, {"username": "user3"}]
        ]

        pa = PermissionAssignment(bundle_db, asset_db, config)
        connection = Mock()

        users = pa.get_users_for_namespace(connection, "default", "domain-123")

        print("users >>", users)

        # Should have 3 unique users
        assert len(users) == 3
        assert users == {"user1", "user2", "user3"}

    def test_permission_assignment_handles_errors_gracefully(self):
        """Test that permission errors for individual users don't stop the process."""
        bundle_db = Mock()
        asset_db = Mock()

        config = {
            "migration": {
                "debug_mode": False,
                "namespace_permissions": ["READ", "WRITE"],
                "resource_tables": []
            }
        }

        # Simulate errors for some users
        bundle_db.execute_insert.side_effect = [
            None,  # user1 succeeds
            Exception("Permission denied"),  # user2 fails
            None,  # user3 succeeds
            Exception("User not found"),  # user4 fails
            None   # user5 succeeds
        ]

        pa = PermissionAssignment(bundle_db, asset_db, config)
        connection = Mock()
        users = {"user1", "user2", "user3", "user4", "user5"}

        # Should not raise exception
        pa.set_namespace_permissions(connection, "bundle-123", "ns-123", users)

        # All users should be attempted
        assert bundle_db.execute_insert.call_count == 5
