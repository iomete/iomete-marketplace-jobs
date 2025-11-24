"""Namespace migration logic for resource-based permissions."""

from typing import Dict, Any, List, Set
from .database import DatabaseManager
from .logger import get_logger

logger = get_logger(__name__)


class NamespaceMigration:
    """Handles namespace permission migration based on resource usage."""

    def __init__(self, bundle_db: DatabaseManager, asset_db: DatabaseManager, config: Dict[str, Any]):
        """
        Initialize namespace migration.

        Args:
            bundle_db: Database manager for bundle/IAM database
            asset_db: Database manager for asset database (resources)
            config: Configuration dictionary
        """
        self.bundle_db = bundle_db
        self.asset_db = asset_db
        self.config = config
        self.migration_config = config["migration"]
        self.namespace_config = config["namespace_config"]
        self.debug_mode = self.migration_config.get("debug_mode", False)

    def get_or_create_resource_bundle(self, connection, domain_id: str) -> str:
        """
        Get existing resource bundle or create one for the domain.

        Args:
            connection: Database connection to bundle_db
            domain_id: Domain identifier

        Returns:
            Bundle ID
        """
        # Check if resource bundle already exists
        check_query = """
            SELECT id FROM bundle
            WHERE domain = %s AND name = 'resource'
        """

        results = self.bundle_db.execute_query(connection, check_query, (domain_id,))

        if results:
            bundle_id = results[0]['id']
            logger.info(f"Found existing resource bundle {bundle_id} for domain {domain_id}")

            duplicate_action = self.migration_config.get("duplicate_bundle_action", "FAIL")
            if duplicate_action == "FAIL":
                raise ValueError(f"Resource bundle already exists for domain {domain_id}. Use UPDATE or SKIP mode.")
            elif duplicate_action == "SKIP":
                logger.info(f"Skipping migration for domain {domain_id} - bundle already exists")
                return None
            # If UPDATE, continue with the existing bundle_id
            return bundle_id

        # Create new resource bundle
        insert_query = """
            INSERT INTO bundle (name, description, owner_id, owner_type, domain, created_at, created_by, updated_at, updated_by, is_archived)
            VALUES ('resource', %s, 'system', 'USER', %s, current_timestamp, 'system', current_timestamp, 'system', false)
            RETURNING id
        """
        description = f"Resource bundle for domain {domain_id} - automatically created by namespace onboarding"
        bundle_id = self.bundle_db.execute_insert(connection, insert_query, (description, domain_id))

        logger.info(f"Created resource bundle {bundle_id} for domain {domain_id}")
        return bundle_id

    def get_namespaces_for_domain(self, connection, domain_id: str) -> List[Dict[str, Any]]:
        """
        Get all namespaces for a domain.

        Args:
            connection: Database connection to asset_db
            domain_id: Domain identifier

        Returns:
            List of namespace records
        """
        query = f"""
            SELECT {self.namespace_config['id_column']} as id,
                   {self.namespace_config['namespace_column']} as namespace,
                   {self.namespace_config['domain_column']} as domain_id
            FROM {self.namespace_config['table']}
            WHERE {self.namespace_config['domain_column']} = %s
        """

        if self.debug_mode:
            logger.debug(f"Namespace query: {query}")
            logger.debug(f"Parameters: ({domain_id},)")

        results = self.asset_db.execute_query(connection, query, (domain_id,))
        logger.info(f"Found {len(results)} namespaces for domain {domain_id}")

        return results

    def get_users_for_namespace(self, connection, namespace: str, domain_id: str) -> Set[str]:
        """
        Get all users who have resources in a specific namespace.

        Args:
            connection: Database connection to asset_db
            namespace: Namespace identifier
            domain_id: Domain identifier

        Returns:
            Set of usernames
        """
        all_users = set()

        for resource_table in self.migration_config["resource_tables"]:
            table_name = resource_table["table"]
            namespace_col = resource_table["namespace_column"]
            user_columns = resource_table["user_columns"]

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

            try:
                results = self.asset_db.execute_query(connection, combined_query, (namespace, domain_id) * len(user_columns))
                table_users = {r['username'] for r in results if r['username']}
                all_users.update(table_users)

                if table_users:
                    logger.debug(f"Found {len(table_users)} users in {table_name} for namespace {namespace}")
            except Exception as e:
                logger.error(f"Error querying users from {table_name} for namespace {namespace}: {e}")
                # Continue with other tables even if one fails
                continue

        logger.info(f"Total {len(all_users)} unique users found for namespace {namespace} in domain {domain_id}")
        return all_users

    def set_namespace_permissions(self, connection, bundle_id: str, namespace_id: str, users: Set[str]):
        """
        Grant namespace permissions to users in the bundle.

        Args:
            connection: Database connection to bundle_db
            bundle_id: Bundle identifier
            namespace_id: Namespace identifier (from domain_namespace_mapping.id)
            users: Set of usernames to grant permissions to
        """
        if not users:
            logger.info(f"No users to grant permissions for namespace {namespace_id}")
            return

        permissions = self.migration_config["namespace_permissions"]

        # Insert permissions for each user
        insert_query = """
            INSERT INTO bundle_permission
            (bundle_id, asset_type, asset_id, actor_type, actor_id, permissions, created_at, created_by, updated_at, updated_by)
            VALUES (%s, 'NAMESPACE', %s, 'USER', %s, %s, current_timestamp, 'system', current_timestamp, 'system')
            ON CONFLICT (bundle_id, asset_type, asset_id, actor_type, actor_id)
            DO UPDATE SET
                permissions = EXCLUDED.permissions,
                updated_at = current_timestamp,
                updated_by = 'system'
        """

        success_count = 0
        error_count = 0

        for username in users:
            try:
                if self.debug_mode:
                    logger.debug(f"Granting {permissions} to user {username} on namespace {namespace_id}")

                self.bundle_db.execute_insert(connection, insert_query, (bundle_id, namespace_id, username, permissions))
                success_count += 1
            except Exception as e:
                logger.error(f"Error granting permissions to user {username} on namespace {namespace_id}: {e}")
                error_count += 1

        logger.info(f"Granted permissions to {success_count} users on namespace {namespace_id} (errors: {error_count})")

    def migrate_domain(self, domain_id: str) -> bool:
        """
        Migrate namespace permissions for a domain.

        Args:
            domain_id: Domain identifier

        Returns:
            True if migration succeeded, False otherwise
        """
        logger.info(f"Starting namespace migration for domain: {domain_id}")

        if self.migration_config.get("dry_run", False):
            logger.warning("DRY RUN MODE - No changes will be committed")

        try:
            # Use bundle_db transaction for bundle operations
            with self.bundle_db.get_transaction() as bundle_conn:
                # Get or create resource bundle
                bundle_id = self.get_or_create_resource_bundle(bundle_conn, domain_id)

                if bundle_id is None:
                    logger.info(f"Skipping domain {domain_id}")
                    return True

                # Use asset_db connection for asset queries (read-only, no transaction needed)
                with self.asset_db.get_connection() as asset_conn:
                    # Get all namespaces for this domain
                    namespaces = self.get_namespaces_for_domain(asset_conn, domain_id)

                    if not namespaces:
                        logger.warning(f"No namespaces found for domain {domain_id}")
                        return True

                    # Process each namespace
                    for namespace_record in namespaces:
                        namespace_id = namespace_record['id']
                        namespace_name = namespace_record['namespace']

                        logger.info(f"Processing namespace: {namespace_name} (ID: {namespace_id})")

                        # Get users who have resources in this namespace
                        users = self.get_users_for_namespace(asset_conn, namespace_name, domain_id)

                        # Grant permissions to those users
                        self.set_namespace_permissions(bundle_conn, bundle_id, namespace_id, users)

                # Dry run - rollback instead of commit
                if self.migration_config.get("dry_run", False):
                    bundle_conn.rollback()
                    logger.info(f"DRY RUN: Changes rolled back for domain {domain_id}")
                else:
                    # Transaction will auto-commit when context exits successfully
                    logger.info(f"Successfully migrated namespace permissions for domain {domain_id}")

            return True

        except Exception as e:
            logger.error(f"Error migrating domain {domain_id}: {e}", exc_info=True)
            return False

    def run_migration(self) -> bool:
        """
        Run migration for all configured domains.

        Returns:
            True if all migrations succeeded, False otherwise
        """
        domains = self.migration_config["domains"]
        logger.info(f"Starting namespace migration for {len(domains)} domain(s)")

        success_count = 0
        error_count = 0

        for domain_id in domains:
            if self.migrate_domain(domain_id):
                success_count += 1
            else:
                error_count += 1

        logger.info(f"Migration completed: {success_count} succeeded, {error_count} failed")
        return error_count == 0
