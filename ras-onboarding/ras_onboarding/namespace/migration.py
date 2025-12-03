"""Namespace migration logic for resource-based permissions."""
import uuid
from typing import Dict, Any, List, Optional
from ..common.database import DatabaseManager
from ..common.logger import get_logger
from .queries import (GET_NAMESPACE_MAPPING_ID, GET_BUNDLE_FROM_DOMAIN_AND_NAME,
                      GET_NAMESPACES_FOR_DOMAIN, CREATE_NAMESPACE_BUNDLE,
                      GET_DOMAIN_MEMBERS, ADD_NAMESPACE_TO_BUNDLE, CHECK_NAMESPACE_IN_BUNDLE,
                      DELETE_BUNDLE_PERMISSIONS, DELETE_BUNDLE_ASSETS,
                      DELETE_NAMESPACE_BUNDLE)
from .permission_assignment import PermissionAssignment

logger = get_logger(__name__)


class NamespaceMigration:
    def __init__(self, bundle_db: DatabaseManager, asset_db: DatabaseManager, domain_db: DatabaseManager,
                 config: Dict[str, Any]):
        self.bundle_db = bundle_db
        self.asset_db = asset_db
        self.domain_db = domain_db
        self.config = config
        self.migration_config = config["migration"]
        self.namespace_config = config["namespace_config"]
        self.debug_mode = self.migration_config.get("debug_mode", False)
        self.permission_assignment = PermissionAssignment(bundle_db, asset_db, config)

    def get_namespace_mapping_id(self, connection, domain_id: str, namespace: str) -> str:
        query = GET_NAMESPACE_MAPPING_ID
        results = self.bundle_db.execute_query(connection, query, (domain_id, namespace))

        if results:
            mapping_id = results[0]['id']
            logger.debug(f"Found namespace mapping {mapping_id} for {namespace} in domain {domain_id}")
            return mapping_id

        error_msg = f"Namespace mapping not found for {namespace} in domain {domain_id}. Ensure domain_namespace_mapping table is populated."
        logger.error(error_msg)
        raise ValueError(error_msg)

    def get_domain_owner(self, connection, domain_id: str) -> tuple[str, str]:
        result = self.asset_db.execute_query(connection, GET_DOMAIN_MEMBERS, (domain_id,))
        if result and len(result) > 0:
            return result[0]['created_by'], 'USER'

        raise ValueError(f"Domain {domain_id} not found")

    def get_or_create_namespace_bundle(self, connection, namespace: str, domain_id: str,
                                       owner_id: str, owner_type: str) -> tuple[Optional[str], bool]:
        bundle_name = f"namespace-{domain_id}-{namespace}"
        check_query = GET_BUNDLE_FROM_DOMAIN_AND_NAME

        results = self.bundle_db.execute_query(connection, check_query, (domain_id, bundle_name))

        if results:
            bundle_id = results[0]['id']
            logger.info(f"Found existing namespace bundle {bundle_id} for namespace {namespace} in domain {domain_id}")

            duplicate_action = self.migration_config.get("duplicate_bundle_action", "FAIL")
            if duplicate_action == "FAIL":
                raise ValueError(
                    f"Namespace bundle already exists for {namespace} in domain {domain_id}. Use OVERWRITE, UPDATE, or SKIP mode.")
            elif duplicate_action == "SKIP":
                logger.info(f"Skipping bundle creation for namespace {namespace} - bundle already exists")
                return (None, False)
            elif duplicate_action == "UPDATE":
                logger.info(f"UPDATE mode: Merging with existing bundle for namespace {namespace}")
                return (bundle_id, True)  # Bundle existed, will need duplicate check
            elif duplicate_action == "OVERWRITE":
                logger.info(f"Deleting existing bundle for namespace {namespace}")
                with connection.cursor() as cursor:
                    cursor.execute(DELETE_BUNDLE_PERMISSIONS, (bundle_id,))
                    cursor.execute(DELETE_BUNDLE_ASSETS, (bundle_id,))
                    cursor.execute(DELETE_NAMESPACE_BUNDLE, (bundle_id,))

        # Create namespace bundle in database
        bundle_id = str(uuid.uuid4())
        description = "Default Resource bundle for namespace"

        with connection.cursor() as cursor:
            cursor.execute(CREATE_NAMESPACE_BUNDLE,
                           (bundle_id, bundle_name, description, owner_id, owner_type, domain_id))

        logger.info(f"Created namespace bundle {bundle_id} for namespace {namespace} in domain {domain_id}")
        return (bundle_id, False)  # New bundle created, no duplicate check needed

    def add_namespace_asset_to_bundle(self, connection, bundle_id: str, namespace_mapping_id: str,
                                       check_duplicate: bool = False):
        """
        Add namespace asset to bundle.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            namespace_mapping_id: Namespace mapping identifier
            check_duplicate: If True, raises error if asset already exists in bundle
        """
        try:
            # Check if asset already exists when in UPDATE mode
            if check_duplicate:
                existing = self.bundle_db.execute_query(
                    connection, CHECK_NAMESPACE_IN_BUNDLE, (bundle_id, namespace_mapping_id)
                )
                if existing:
                    raise ValueError(
                        f"Namespace asset {namespace_mapping_id} already exists in bundle {bundle_id}. "
                        "Cannot add duplicate namespace asset in UPDATE mode."
                    )

            with connection.cursor() as cursor:
                cursor.execute(ADD_NAMESPACE_TO_BUNDLE, (bundle_id, namespace_mapping_id))
            logger.info(f"Added namespace {namespace_mapping_id} to bundle {bundle_id} in bundle_asset table")
        except ValueError:
            # Re-raise ValueError (our duplicate check error)
            raise
        except Exception as e:
            logger.error(f"Error adding namespace {namespace_mapping_id} to bundle {bundle_id}: {e}")
            raise

    def get_namespaces_for_domain(self, connection, domain_id: str) -> List[Dict[str, Any]]:
        query = GET_NAMESPACES_FOR_DOMAIN.format(
            id_column=self.namespace_config['id_column'],
            namespace_column=self.namespace_config['namespace_column'],
            domain_column=self.namespace_config['domain_column'],
            table=self.namespace_config['table']
        )

        if self.debug_mode:
            logger.debug(f"Namespace query: {query}")
            logger.debug(f"Parameters: ({domain_id},)")

        results = self.asset_db.execute_query(connection, query, (domain_id,))
        logger.info(f"Found {len(results)} namespaces for domain {domain_id}")
        return results

    def migrate_domain(self, domain_id: str) -> bool:
        logger.info(f"Starting namespace migration for domain: {domain_id}")

        if self.migration_config.get("dry_run", False):
            logger.warning("DRY RUN MODE - No changes will be committed")
        try:
            # Use bundle_db transaction for bundle operations
            with self.bundle_db.get_transaction() as bundle_conn:
                with self.asset_db.get_connection() as asset_conn:
                    domain_owner_id, domain_owner_type = self.get_domain_owner(bundle_conn, domain_id)
                    logger.info(f"Using domain owner {domain_owner_id} ({domain_owner_type}) for bundle creation")

                    namespaces = self.get_namespaces_for_domain(asset_conn, domain_id)

                    if not namespaces:
                        logger.warning(f"No namespaces found for domain {domain_id}")
                        return True

                    for namespace_record in namespaces:
                        namespace_name = namespace_record['namespace']
                        logger.info(f"Processing namespace: {namespace_name} in domain {domain_id}")

                        namespace_mapping_id = self.get_namespace_mapping_id(
                            asset_conn, domain_id, namespace_name
                        )

                        with self.bundle_db.get_transaction() as bundle_conn2:
                            bundle_id, bundle_existed = self.get_or_create_namespace_bundle(
                                bundle_conn2, namespace_name, domain_id,
                                owner_id=domain_owner_id, owner_type=domain_owner_type
                            )

                            if bundle_id is None:
                                logger.info(f"Skipping namespace {namespace_name} - bundle already exists")
                                continue

                            self.add_namespace_asset_to_bundle(
                                bundle_conn2, bundle_id, namespace_mapping_id,
                                check_duplicate=bundle_existed
                            )

                            with self.asset_db.get_connection() as asset_conn2:
                                users = self.permission_assignment.get_users_for_namespace(
                                    asset_conn2, namespace_name, domain_id
                                )

                                self.permission_assignment.set_namespace_permissions(
                                    bundle_conn2, bundle_id, namespace_mapping_id, users
                                )

                if self.migration_config.get("dry_run", False):
                    bundle_conn.rollback()
                    logger.info(f"DRY RUN: Changes rolled back for domain {domain_id}")
                else:
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
        domains_config = self.migration_config["domains"]

        # Handle both list of strings and list of objects
        domains = []
        if isinstance(domains_config, list) and len(domains_config) > 0:
            if isinstance(domains_config[0], str):
                domains = domains_config
            else:
                domains = [d["domain_id"] for d in domains_config]

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