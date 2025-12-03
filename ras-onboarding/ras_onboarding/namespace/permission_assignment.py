"""Permission assignment logic for namespace migrations."""

from typing import Set, Dict, Any

from ras_onboarding.common.database import DatabaseManager
from ras_onboarding.common.logger import get_logger
from ras_onboarding.namespace.queries import SET_NAMESPACE_PERMISSION

logger = get_logger(__name__)


class PermissionAssignment:
    """Handles permission assignment for namespace resources."""

    def __init__(self, bundle_db: DatabaseManager, asset_db: DatabaseManager, config: Dict[str, Any]):
        self.bundle_db = bundle_db
        self.asset_db = asset_db
        self.config = config
        self.migration_config = config["migration"]
        self.debug_mode = self.migration_config.get("debug_mode", False)

    def get_users_for_namespace(self, connection, namespace: str, domain_id: str) -> Set[str]:
        all_users = set()

        for resource_table in self.migration_config["resource_tables"]:
            table_name = resource_table["table"]
            namespace_col = resource_table["namespace_column"]
            user_columns = resource_table["user_columns"]

            try:
                # Build UNION query for all user columns in this table
                union_queries = []
                for user_col in user_columns:
                    query = f"""
                    SELECT DISTINCT {user_col} as username
                    FROM {table_name}
                    WHERE {namespace_col} = %s
                      AND domain = %s
                      AND is_deleted = false
                      AND {user_col} IS NOT NULL
                """
                    union_queries.append(query)

                # Combine all user columns with UNION
                combined_query = " UNION ".join(union_queries)

                if self.debug_mode:
                    logger.debug(f"User query for {table_name}: {combined_query}")
                    logger.debug(f"Parameters: ({namespace}, {domain_id})")

                results = self.asset_db.execute_query(connection, combined_query,
                                                      (namespace, domain_id) * len(user_columns))
                table_users = {r['username'] for r in results if r['username']}
                all_users.update(table_users)

                if table_users:
                    logger.debug(f"Found {len(table_users)} users in {table_name} for namespace {namespace}")
            except Exception as e:
                logger.error(f"Error querying users from {table_name} for namespace {namespace}: {e}")
                # Continue processing other tables

        logger.info(f"Total {len(all_users)} unique users found for namespace {namespace} in domain {domain_id}")
        return all_users

    def set_namespace_permissions(self, connection, bundle_id: str, namespace_id: str, users: Set[str]):
        if not users:
            logger.info(f"No users to grant permissions for namespace {namespace_id}")
            return

        permissions = self.migration_config["namespace_permissions"]
        success_count = 0
        error_count = 0

        for username in users:
            try:
                if self.debug_mode:
                    logger.debug(f"Granting {permissions} to user {username} on namespace {namespace_id}")

                self.bundle_db.execute_insert(connection, SET_NAMESPACE_PERMISSION,
                                              (bundle_id, username, permissions))
                success_count += 1
            except Exception as e:
                logger.error(f"Error granting permissions to user {username} on namespace {namespace_id}: {e}")
                error_count += 1

        logger.info(f"Granted permissions to {success_count} users on namespace {namespace_id} (errors: {error_count})")
