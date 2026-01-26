"""Permission assignment logic for namespace migrations."""

from typing import Set, Dict, Any

from ras_onboarding.common.database import DatabaseManager
from ras_onboarding.common.logger import get_logger
from ras_onboarding.namespace.queries import (
    SET_NAMESPACE_PERMISSION,
    GET_ALL_DOMAIN_USERS,
    GET_ALL_DOMAIN_GROUPS
)

logger = get_logger(__name__)


class PermissionAssignment:
    """Handles permission assignment for namespace resources."""

    def __init__(self, iam_db: DatabaseManager, core_db: DatabaseManager, config: Dict[str, Any]):
        self.iam_db = iam_db
        self.core_db = core_db
        self.config = config
        self.migration_config = config["migration"]
        self.asset_mappings = config.get("asset_mappings", {})
        self.debug_mode = self.migration_config.get("debug_mode", False)

    def get_users_for_namespace(self, connection, namespace: str, domain_id: str) -> Set[str]:
        """Get all users in the domain who should have namespace permissions."""
        all_users = set()

        try:
            if self.debug_mode:
                logger.debug(f"Fetching all domain users for domain {domain_id}")

            results = self.iam_db.execute_query(connection, GET_ALL_DOMAIN_USERS, (domain_id,))
            all_users = {r['username'] for r in results if r['username']}

            logger.info(f"Found {len(all_users)} users in domain {domain_id} for namespace {namespace}")
        except Exception as e:
            logger.error(f"Error fetching domain users for namespace {namespace}: {e}")

        return all_users

    def get_groups_for_namespace(self, connection, namespace: str, domain_id: str) -> Set[str]:
        all_groups = set()

        try:
            if self.debug_mode:
                logger.debug(f"Fetching all domain groups for domain {domain_id}")

            results = self.iam_db.execute_query(connection, GET_ALL_DOMAIN_GROUPS, (domain_id,))
            all_groups = {r['groupname'] for r in results if r['groupname']}

            logger.info(f"Found {len(all_groups)} groups in domain {domain_id} for namespace {namespace}")
        except Exception as e:
            logger.error(f"Error fetching domain groups for namespace {namespace}: {e}")

        return all_groups

    def set_namespace_permissions_for_users(self, connection, bundle_id: str, namespace_id: str, users: Set[str]):
        if not users:
            logger.info(f"No users to grant permissions for namespace {namespace_id}")
            return

        namespace_config = self.asset_mappings.get("NAMESPACE", {})
        permissions = namespace_config.get("permissions", ["USE"])
        success_count = 0
        error_count = 0

        for username in users:
            try:
                if self.debug_mode:
                    logger.debug(f"Granting {permissions} to user {username} on namespace {namespace_id}")

                self.iam_db.execute_insert(connection, SET_NAMESPACE_PERMISSION,
                                              (bundle_id, 'USER', username, permissions))
                success_count += 1
            except Exception as e:
                logger.error(f"Error granting permissions to user {username} on namespace {namespace_id}: {e}")
                error_count += 1

        logger.info(f"Granted permissions to {success_count} users on namespace {namespace_id} (errors: {error_count})")

    def set_namespace_permissions_for_groups(self, connection, bundle_id: str, namespace_id: str, groups: Set[str]):
        """Grant namespace permissions to groups."""
        if not groups:
            logger.info(f"No groups to grant permissions for namespace {namespace_id}")
            return

        namespace_config = self.asset_mappings.get("NAMESPACE", {})
        permissions = namespace_config.get("permissions", ["USE"])
        success_count = 0
        error_count = 0

        for group_name in groups:
            try:
                if self.debug_mode:
                    logger.debug(f"Granting {permissions} to group {group_name} on namespace {namespace_id}")

                self.iam_db.execute_insert(connection, SET_NAMESPACE_PERMISSION,
                                              (bundle_id, 'GROUP', group_name, permissions))
                success_count += 1
            except Exception as e:
                logger.error(f"Error granting permissions to group {group_name} on namespace {namespace_id}: {e}")
                error_count += 1

        logger.info(f"Granted permissions to {success_count} groups on namespace {namespace_id} (errors: {error_count})")
