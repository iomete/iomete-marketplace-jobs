"""Test cases for permission assignment module."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call
from ras_onboarding.namespace.permission_assignment import PermissionAssignment


@pytest.fixture
def mock_bundle_db():
    """Mock bundle database manager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    db.execute_insert = Mock()
    return db


@pytest.fixture
def mock_asset_db():
    """Mock asset database manager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    return db


@pytest.fixture
def sample_config():
    """Sample configuration for tests."""
    return {
        "migration": {
            "debug_mode": False,
            "resource_tables": [
                {
                    "table": "lakehouse",
                    "namespace_column": "lakehouse_namespace",
                    "user_columns": ["created_by", "updated_by"]
                },
                {
                    "table": "spark_job",
                    "namespace_column": "namespace",
                    "user_columns": ["owner"]
                }
            ],
            "namespace_permissions": ["READ", "WRITE"]
        }
    }


@pytest.fixture
def permission_assignment(mock_bundle_db, mock_asset_db, sample_config):
    """Create PermissionAssignment instance with mocks."""
    return PermissionAssignment(mock_bundle_db, mock_asset_db, sample_config)


class TestPermissionAssignmentInit:
    """Test PermissionAssignment initialization."""

    def test_init_with_valid_config(self, mock_bundle_db, mock_asset_db, sample_config):
        """Test initialization with valid configuration."""
        pa = PermissionAssignment(mock_bundle_db, mock_asset_db, sample_config)

        assert pa.bundle_db == mock_bundle_db
        assert pa.asset_db == mock_asset_db
        assert pa.config == sample_config
        assert pa.migration_config == sample_config["migration"]
        assert pa.debug_mode is False

    def test_init_with_debug_mode(self, mock_bundle_db, mock_asset_db, sample_config):
        """Test initialization with debug mode enabled."""
        sample_config["migration"]["debug_mode"] = True
        pa = PermissionAssignment(mock_bundle_db, mock_asset_db, sample_config)

        assert pa.debug_mode is True


class TestGetUsersForNamespace:
    """Test get_users_for_namespace method."""

    def test_get_users_from_single_table(self, permission_assignment, mock_asset_db):
        """Test getting users from a single resource table."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = [
            {"username": "user1"},
            {"username": "user2"}
        ]

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert users == {"user1", "user2"}
        assert mock_asset_db.execute_query.call_count == 2

    def test_get_users_from_multiple_tables(self, permission_assignment, mock_asset_db):
        """Test getting users from multiple resource tables."""
        mock_connection = Mock()

        mock_asset_db.execute_query.side_effect = [
            [{"username": "user1"}, {"username": "user2"}],
            [{"username": "user2"}, {"username": "user3"}]
        ]

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert users == {"user1", "user2", "user3"}
        assert len(users) == 3

    def test_get_users_excludes_null_usernames(self, permission_assignment, mock_asset_db):
        """Test that null usernames are filtered out."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = [
            {"username": "user1"},
            {"username": None},
            {"username": "user2"},
            {"username": ""}
        ]

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert users == {"user1", "user2"}
        assert None not in users
        assert "" not in users

    def test_get_users_with_no_results(self, permission_assignment, mock_asset_db):
        """Test getting users when no resources exist."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = []

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert users == set()
        assert len(users) == 0

    def test_get_users_query_construction(self, permission_assignment, mock_asset_db):
        """Test that queries are constructed correctly."""
        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = []

        permission_assignment.get_users_for_namespace(
            mock_connection, "my_namespace", "domain-456"
        )

        first_call_query = mock_asset_db.execute_query.call_args_list[0][0][1]
        first_call_params = mock_asset_db.execute_query.call_args_list[0][0][2]

        assert "SELECT DISTINCT" in first_call_query
        assert "FROM lakehouse" in first_call_query
        assert "WHERE lakehouse_namespace = %s" in first_call_query
        assert "AND domain = %s" in first_call_query
        assert "AND is_deleted = false" in first_call_query
        assert first_call_params == ("my_namespace", "domain-456", "my_namespace", "domain-456")

    def test_get_users_handles_table_error(self, permission_assignment, mock_asset_db):
        """Test that errors in one table don't stop processing other tables."""
        mock_connection = Mock()

        mock_asset_db.execute_query.side_effect = [
            Exception("Database error"),
            [{"username": "user1"}]
        ]

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )


        assert users == {"user1"}

    def test_get_users_with_debug_mode(self, mock_bundle_db, mock_asset_db, sample_config):
        """Test debug logging when debug mode is enabled."""
        sample_config["migration"]["debug_mode"] = True
        pa = PermissionAssignment(mock_bundle_db, mock_asset_db, sample_config)

        mock_connection = Mock()
        mock_asset_db.execute_query.return_value = [{"username": "user1"}]

        with patch("ras_onboarding.namespace.permission_assignment.logger") as mock_logger:
            pa.get_users_for_namespace(mock_connection, "default", "domain-123")


            debug_calls = [call for call in mock_logger.debug.call_args_list]
            assert len(debug_calls) > 0


class TestSetNamespacePermissions:
    """Test set_namespace_permissions method."""

    def test_set_permissions_for_multiple_users(self, permission_assignment, mock_bundle_db):
        """Test setting permissions for multiple users."""
        mock_connection = Mock()
        users = {"user1", "user2", "user3"}

        permission_assignment.set_namespace_permissions(
            mock_connection, "bundle-123", "namespace-456", users
        )

        assert mock_bundle_db.execute_insert.call_count == 3

    def test_set_permissions_with_empty_user_set(self, permission_assignment, mock_bundle_db):
        """Test setting permissions with no users."""
        mock_connection = Mock()
        users = set()

        permission_assignment.set_namespace_permissions(
            mock_connection, "bundle-123", "namespace-456", users
        )


        mock_bundle_db.execute_insert.assert_not_called()

    def test_set_permissions_correct_parameters(self, permission_assignment, mock_bundle_db):
        """Test that permissions are set with correct parameters."""
        mock_connection = Mock()
        users = {"user1"}
        bundle_id = "bundle-123"
        namespace_id = "namespace-456"
        permissions = ["READ", "WRITE"]

        permission_assignment.set_namespace_permissions(
            mock_connection, bundle_id, namespace_id, users
        )


        call_args = mock_bundle_db.execute_insert.call_args
        assert call_args[0][0] == mock_connection
        assert call_args[0][2] == (bundle_id, "user1", permissions)

    def test_set_permissions_handles_user_error(self, permission_assignment, mock_bundle_db):
        """Test that error for one user doesn't stop processing others."""
        mock_connection = Mock()
        users = {"user1", "user2", "user3"}


        mock_bundle_db.execute_insert.side_effect = [
            None,
            Exception("Permission error"),
            None
        ]

        permission_assignment.set_namespace_permissions(
            mock_connection, "bundle-123", "namespace-456", users
        )


        assert mock_bundle_db.execute_insert.call_count == 3

    def test_set_permissions_with_debug_mode(self, mock_bundle_db, mock_asset_db, sample_config):
        """Test debug logging when setting permissions."""
        sample_config["migration"]["debug_mode"] = True
        pa = PermissionAssignment(mock_bundle_db, mock_asset_db, sample_config)

        mock_connection = Mock()
        users = {"user1"}

        with patch("ras_onboarding.namespace.permission_assignment.logger") as mock_logger:
            pa.set_namespace_permissions(mock_connection, "bundle-123", "namespace-456", users)


            debug_calls = [call for call in mock_logger.debug.call_args_list]
            assert len(debug_calls) > 0

    def test_set_permissions_logs_success_and_errors(self, permission_assignment, mock_bundle_db):
        """Test that success and error counts are logged."""
        mock_connection = Mock()
        users = {"user1", "user2", "user3"}


        mock_bundle_db.execute_insert.side_effect = [
            None,
            Exception("Error"),
            None
        ]

        with patch("ras_onboarding.namespace.permission_assignment.logger") as mock_logger:
            permission_assignment.set_namespace_permissions(
                mock_connection, "bundle-123", "namespace-456", users
            )


            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("2 users" in call for call in info_calls)
            assert any("errors: 1" in call for call in info_calls)


class TestPermissionAssignmentIntegration:
    """Integration tests for PermissionAssignment."""

    def test_full_workflow(self, permission_assignment, mock_bundle_db, mock_asset_db):
        """Test complete workflow of getting users and setting permissions."""
        mock_asset_conn = Mock()
        mock_bundle_conn = Mock()


        mock_asset_db.execute_query.return_value = [
            {"username": "user1"},
            {"username": "user2"}
        ]


        users = permission_assignment.get_users_for_namespace(
            mock_asset_conn, "default", "domain-123"
        )

        assert len(users) == 2


        permission_assignment.set_namespace_permissions(
            mock_bundle_conn, "bundle-123", "namespace-456", users
        )


        assert mock_bundle_db.execute_insert.call_count == 2
