"""Test cases for permission assignment module."""

import pytest
from unittest.mock import Mock, MagicMock, patch, call

from ras_onboarding.namespace.permission_assignment import PermissionAssignment


@pytest.fixture
def mock_iam_db():
    """Mock IAM database manager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    db.execute_insert = Mock()
    return db


@pytest.fixture
def mock_core_db():
    """Mock core database manager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    return db


@pytest.fixture
def sample_config():
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
            ]
        },
        "asset_mappings": {
            "NAMESPACE": {
                "permissions": ["USE"]
            }
        }
    }


@pytest.fixture
def permission_assignment(mock_iam_db, mock_core_db, sample_config):
    """Create PermissionAssignment instance with mocks."""
    return PermissionAssignment(mock_iam_db, mock_core_db, sample_config)


class TestGetUsersForNamespace:
    """Test get_users_for_namespace method."""

    def test_get_users_from_iam_db(self, permission_assignment, mock_iam_db):
        """Test getting users from IAM database."""
        mock_connection = Mock()

        mock_iam_db.execute_query.return_value = [
            {"username": "user1"},
            {"username": "user2"},
            {"username": "user3"}
        ]

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert users == {"user1", "user2", "user3"}
        assert len(users) == 3

    def test_get_users_excludes_null_usernames(self, permission_assignment, mock_iam_db):
        """Test that null and empty usernames are excluded."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [
            {"username": "user1"},
            {"username": None},
            {"username": "user2"},
            {"username": ""}
        ]

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )

        # Only non-null, non-empty usernames are included
        assert "user1" in users
        assert "user2" in users
        assert None not in users
        assert "" not in users

    def test_get_users_handles_error(self, permission_assignment, mock_iam_db):
        """Test that errors are handled gracefully."""
        mock_connection = Mock()

        mock_iam_db.execute_query.side_effect = Exception("Database error")

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert users == set()

    def test_get_users_with_debug_mode(self, mock_iam_db, mock_core_db, sample_config):
        """Test that debug mode logs additional information."""
        sample_config["migration"]["debug_mode"] = True
        pa = PermissionAssignment(mock_iam_db, mock_core_db, sample_config)

        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"username": "user1"}]

        with patch("ras_onboarding.namespace.permission_assignment.logger") as mock_logger:
            pa.get_users_for_namespace(mock_connection, "default", "domain-123")

            debug_calls = [call for call in mock_logger.debug.call_args_list]
            assert len(debug_calls) > 0


class TestGetGroupsForNamespace:
    """Test get_groups_for_namespace method."""

    def test_get_groups_from_iam_db(self, permission_assignment, mock_iam_db):
        """Test getting groups from IAM database."""
        mock_connection = Mock()

        mock_iam_db.execute_query.return_value = [
            {"groupname": "developers"},
            {"groupname": "analysts"},
            {"groupname": "admins"}
        ]

        groups = permission_assignment.get_groups_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert groups == {"developers", "analysts", "admins"}
        assert len(groups) == 3

    def test_get_groups_excludes_null_groupnames(self, permission_assignment, mock_iam_db):
        """Test that null and empty group names are excluded."""
        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [
            {"groupname": "developers"},
            {"groupname": None},
            {"groupname": "analysts"},
            {"groupname": ""}
        ]

        groups = permission_assignment.get_groups_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert "developers" in groups
        assert "analysts" in groups
        assert None not in groups
        assert "" not in groups

    def test_get_groups_handles_error(self, permission_assignment, mock_iam_db):
        """Test that errors are handled gracefully."""
        mock_connection = Mock()

        mock_iam_db.execute_query.side_effect = Exception("Database error")

        groups = permission_assignment.get_groups_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert groups == set()

    def test_get_groups_with_debug_mode(self, mock_iam_db, mock_core_db, sample_config):
        """Test that debug mode logs additional information."""
        sample_config["migration"]["debug_mode"] = True
        pa = PermissionAssignment(mock_iam_db, mock_core_db, sample_config)

        mock_connection = Mock()
        mock_iam_db.execute_query.return_value = [{"groupname": "developers"}]

        with patch("ras_onboarding.namespace.permission_assignment.logger") as mock_logger:
            pa.get_groups_for_namespace(mock_connection, "default", "domain-123")

            debug_calls = [call for call in mock_logger.debug.call_args_list]
            assert len(debug_calls) > 0


class TestSetNamespacePermissionsForUsers:
    """Test set_namespace_permissions_for_users method."""

    def test_set_permissions_for_multiple_users(self, permission_assignment, mock_iam_db):
        """Test setting permissions for multiple users."""
        mock_connection = Mock()
        users = {"user1", "user2", "user3"}

        permission_assignment.set_namespace_permissions_for_users(
            mock_connection, "bundle-123", "namespace-456", users
        )

        assert mock_iam_db.execute_insert.call_count == 3

    def test_set_permissions_with_empty_user_set(self, permission_assignment, mock_iam_db):
        """Test that no permissions are set for empty user set."""
        mock_connection = Mock()
        users = set()

        permission_assignment.set_namespace_permissions_for_users(
            mock_connection, "bundle-123", "namespace-456", users
        )

        mock_iam_db.execute_insert.assert_not_called()

    def test_set_permissions_correct_parameters(self, permission_assignment, mock_iam_db):
        """Test that permissions are set with correct parameters."""
        mock_connection = Mock()
        users = {"user1"}
        bundle_id = "bundle-123"
        namespace_id = "namespace-456"
        permissions = ["USE"]

        permission_assignment.set_namespace_permissions_for_users(
            mock_connection, bundle_id, namespace_id, users
        )

        call_args = mock_iam_db.execute_insert.call_args
        assert call_args[0][0] == mock_connection
        # Updated to match new parameter order: (bundle_id, actor_type, actor_id, permissions)
        assert call_args[0][2] == (bundle_id, 'USER', "user1", permissions)

    def test_set_permissions_handles_user_error(self, permission_assignment, mock_iam_db):
        """Test that errors for individual users don't stop the process."""
        mock_connection = Mock()
        users = {"user1", "user2", "user3"}

        mock_iam_db.execute_insert.side_effect = [
            None,
            Exception("Permission error"),
            None
        ]

        permission_assignment.set_namespace_permissions_for_users(
            mock_connection, "bundle-123", "namespace-456", users
        )

        assert mock_iam_db.execute_insert.call_count == 3

    def test_set_permissions_logs_success_and_errors(self, permission_assignment, mock_iam_db):
        """Test that success and error counts are logged."""
        mock_connection = Mock()
        users = {"user1", "user2", "user3"}

        mock_iam_db.execute_insert.side_effect = [
            None,
            Exception("Error"),
            None
        ]

        with patch("ras_onboarding.namespace.permission_assignment.logger") as mock_logger:
            permission_assignment.set_namespace_permissions_for_users(
                mock_connection, "bundle-123", "namespace-456", users
            )

            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("2 users" in call for call in info_calls)
            assert any("errors: 1" in call for call in info_calls)


class TestSetNamespacePermissionsForGroups:
    """Test set_namespace_permissions_for_groups method."""

    def test_set_permissions_for_multiple_groups(self, permission_assignment, mock_iam_db):
        """Test setting permissions for multiple groups."""
        mock_connection = Mock()
        groups = {"developers", "analysts", "admins"}

        permission_assignment.set_namespace_permissions_for_groups(
            mock_connection, "bundle-123", "namespace-456", groups
        )

        assert mock_iam_db.execute_insert.call_count == 3

    def test_set_permissions_with_empty_group_set(self, permission_assignment, mock_iam_db):
        """Test that no permissions are set for empty group set."""
        mock_connection = Mock()
        groups = set()

        permission_assignment.set_namespace_permissions_for_groups(
            mock_connection, "bundle-123", "namespace-456", groups
        )

        mock_iam_db.execute_insert.assert_not_called()

    def test_set_permissions_correct_parameters_for_groups(self, permission_assignment, mock_iam_db):
        """Test that group permissions are set with correct parameters."""
        mock_connection = Mock()
        groups = {"developers"}
        bundle_id = "bundle-123"
        namespace_id = "namespace-456"
        permissions = ["USE"]

        permission_assignment.set_namespace_permissions_for_groups(
            mock_connection, bundle_id, namespace_id, groups
        )

        call_args = mock_iam_db.execute_insert.call_args
        assert call_args[0][0] == mock_connection
        # Parameter order: (bundle_id, actor_type, actor_id, permissions)
        assert call_args[0][2] == (bundle_id, 'GROUP', "developers", permissions)

    def test_set_permissions_handles_group_error(self, permission_assignment, mock_iam_db):
        """Test that errors for individual groups don't stop the process."""
        mock_connection = Mock()
        groups = {"developers", "analysts", "admins"}

        mock_iam_db.execute_insert.side_effect = [
            None,
            Exception("Permission error"),
            None
        ]

        permission_assignment.set_namespace_permissions_for_groups(
            mock_connection, "bundle-123", "namespace-456", groups
        )

        assert mock_iam_db.execute_insert.call_count == 3

    def test_set_permissions_logs_success_and_errors_for_groups(self, permission_assignment, mock_iam_db):
        """Test that success and error counts are logged for groups."""
        mock_connection = Mock()
        groups = {"developers", "analysts", "admins"}

        mock_iam_db.execute_insert.side_effect = [
            None,
            Exception("Error"),
            None
        ]

        with patch("ras_onboarding.namespace.permission_assignment.logger") as mock_logger:
            permission_assignment.set_namespace_permissions_for_groups(
                mock_connection, "bundle-123", "namespace-456", groups
            )

            info_calls = [str(call) for call in mock_logger.info.call_args_list]
            assert any("2 groups" in call for call in info_calls)
            assert any("errors: 1" in call for call in info_calls)


class TestPermissionAssignmentIntegration:
    """Integration tests for PermissionAssignment."""

    def test_full_workflow_users_only(self, permission_assignment, mock_iam_db, mock_core_db):
        """Test complete workflow of getting users and setting permissions."""
        mock_connection = Mock()

        mock_iam_db.execute_query.return_value = [
            {"username": "user1"},
            {"username": "user2"}
        ]

        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert len(users) == 2

        permission_assignment.set_namespace_permissions_for_users(
            mock_connection, "bundle-123", "namespace-456", users
        )

        assert mock_iam_db.execute_insert.call_count == 2

    def test_full_workflow_groups_only(self, permission_assignment, mock_iam_db, mock_core_db):
        """Test complete workflow of getting groups and setting permissions."""
        mock_connection = Mock()

        mock_iam_db.execute_query.return_value = [
            {"groupname": "developers"},
            {"groupname": "analysts"}
        ]

        groups = permission_assignment.get_groups_for_namespace(
            mock_connection, "default", "domain-123"
        )

        assert len(groups) == 2

        permission_assignment.set_namespace_permissions_for_groups(
            mock_connection, "bundle-123", "namespace-456", groups
        )

        assert mock_iam_db.execute_insert.call_count == 2

    def test_full_workflow_users_and_groups(self, permission_assignment, mock_iam_db, mock_core_db):
        """Test complete workflow with both users and groups."""
        mock_connection = Mock()

        # First query returns users, second returns groups
        mock_iam_db.execute_query.side_effect = [
            [{"username": "user1"}, {"username": "user2"}],
            [{"groupname": "developers"}, {"groupname": "analysts"}]
        ]

        # Get and set user permissions
        users = permission_assignment.get_users_for_namespace(
            mock_connection, "default", "domain-123"
        )
        assert len(users) == 2

        permission_assignment.set_namespace_permissions_for_users(
            mock_connection, "bundle-123", "namespace-456", users
        )

        # Get and set group permissions
        groups = permission_assignment.get_groups_for_namespace(
            mock_connection, "default", "domain-123"
        )
        assert len(groups) == 2

        permission_assignment.set_namespace_permissions_for_groups(
            mock_connection, "bundle-123", "namespace-456", groups
        )

        # Should have called execute_insert 4 times (2 users + 2 groups)
        assert mock_iam_db.execute_insert.call_count == 4

    def test_workflow_with_mixed_actor_types(self, permission_assignment, mock_iam_db):
        """Test that USER and GROUP actor types are correctly set."""
        mock_connection = Mock()

        # Track all calls to execute_insert
        insert_calls = []
        mock_iam_db.execute_insert.side_effect = lambda conn, query, params: insert_calls.append(params)

        # Set user permissions
        permission_assignment.set_namespace_permissions_for_users(
            mock_connection, "bundle-123", "namespace-456", {"user1"}
        )

        # Set group permissions
        permission_assignment.set_namespace_permissions_for_groups(
            mock_connection, "bundle-123", "namespace-456", {"developers"}
        )

        assert len(insert_calls) == 2

        # Verify USER actor type
        user_call = insert_calls[0]
        assert user_call[1] == 'USER'  # actor_type
        assert user_call[2] == 'user1'  # actor_id

        # Verify GROUP actor type
        group_call = insert_calls[1]
        assert group_call[1] == 'GROUP'  # actor_type
        assert group_call[2] == 'developers'  # actor_id
