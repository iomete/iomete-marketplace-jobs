"""Namespace migration logic for resource-based permissions."""
import uuid
from typing import Dict, Any, List, Optional
from ..common.database import DatabaseManager
from ..common.logger import get_logger
from .queries import (GET_NAMESPACE_MAPPING_ID, GET_BUNDLE_FROM_DOMAIN_AND_NAME,
                      GET_NAMESPACES_FOR_DOMAIN, CREATE_NAMESPACE_BUNDLE,
                      ADD_NAMESPACE_TO_BUNDLE, CHECK_NAMESPACE_IN_BUNDLE,
                      CHECK_NAMESPACE_IN_ANY_BUNDLE, DELETE_BUNDLE_PERMISSIONS,
                      DELETE_BUNDLE_ASSETS, DELETE_NAMESPACE_BUNDLE)
from .permission_assignment import PermissionAssignment

logger = get_logger(__name__)


class NamespaceMigration:
    def __init__(self, iam_db: DatabaseManager, core_db: DatabaseManager, config: Dict[str, Any]):
        self.iam_db = iam_db
        self.core_db = core_db
        self.config = config
        self.migration_config = config["migration"]
        self.namespace_config = config["namespace_config"]
        self.debug_mode = self.migration_config.get("debug_mode", False)
        self.permission_assignment = PermissionAssignment(iam_db, core_db, config)

    def get_namespace_mapping_id(self, connection, domain_id: str, namespace: str) -> str:
        query = GET_NAMESPACE_MAPPING_ID
        results = self.iam_db.execute_query(connection, query, (domain_id, namespace))

        if results:
            mapping_id = results[0]['id']
            logger.debug(f"Found namespace mapping {mapping_id} for {namespace} in domain {domain_id}")
            return mapping_id

        error_msg = f"Namespace mapping not found for {namespace} in domain {domain_id}. Ensure domain_namespace_mapping table is populated."
        logger.error(error_msg)
        raise ValueError(error_msg)

    def get_or_create_namespace_bundle(self, connection, namespace: str, domain_id: str,
                                       owner_id: str, owner_type: str) -> tuple[Optional[str], bool]:
        bundle_name = f"namespace-{domain_id}-{namespace}"
        check_query = GET_BUNDLE_FROM_DOMAIN_AND_NAME

        results = self.iam_db.execute_query(connection, check_query, (domain_id, bundle_name))

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
                                      domain_id: str, namespace: str, validate_bundle_uniqueness: bool = False):
        """
        Add namespace asset to bundle.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            namespace_mapping_id: Namespace mapping identifier
            domain_id: Domain identifier
            namespace: Namespace name
            validate_bundle_uniqueness: If True, raises error if asset already exists in a different bundle
        """
        try:
            # Check if asset already exists in a different bundle when in UPDATE mode
            if validate_bundle_uniqueness:
                existing = self.iam_db.execute_query(
                    connection, CHECK_NAMESPACE_IN_ANY_BUNDLE, (namespace_mapping_id,)
                )
                if existing:
                    existing_bundle_name = existing[0]['name']
                    expected_bundle_name = f"namespace-{domain_id}-{namespace}"

                    if existing_bundle_name != expected_bundle_name:
                        raise ValueError(
                            f"Namespace asset {namespace_mapping_id} already exists in bundle '{existing_bundle_name}'. "
                            f"Expected bundle name: '{expected_bundle_name}'. "
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

        results = self.core_db.execute_query(connection, query, (domain_id,))
        logger.info(f"Found {len(results)} namespaces for domain {domain_id}")
        return results

    def migrate_domain(self, domain_config: Dict[str, Any]) -> bool:
        domain_id = domain_config["domain_id"]
        owner_id = domain_config["owner_id"]
        owner_type = domain_config.get("owner_type", "USER")

        logger.info(f"Starting namespace migration for domain: {domain_id}")

        if self.migration_config.get("dry_run", False):
            logger.warning("DRY RUN MODE - No changes will be committed")
        try:
            # Use iam_db transaction for bundle operations
            with self.iam_db.get_transaction() as iam_conn:
                with self.core_db.get_connection() as core_conn:
                    logger.info(f"Using owner {owner_id} ({owner_type}) for bundle creation")

                    namespaces = self.get_namespaces_for_domain(core_conn, domain_id)

                    if not namespaces:
                        logger.warning(f"No namespaces found for domain {domain_id}")
                        return True

                    for namespace_record in namespaces:
                        namespace_name = namespace_record['namespace']
                        logger.info(f"Processing namespace: {namespace_name} in domain {domain_id}")

                        namespace_mapping_id = self.get_namespace_mapping_id(
                            core_conn, domain_id, namespace_name
                        )

                        bundle_id, bundle_existed = self.get_or_create_namespace_bundle(
                            iam_conn, namespace_name, domain_id,
                            owner_id=owner_id, owner_type=owner_type
                        )

                        if bundle_id is None:
                            logger.info(f"Skipping namespace {namespace_name} - bundle already exists")
                            continue

                        self.add_namespace_asset_to_bundle(
                            iam_conn, bundle_id, namespace_mapping_id,
                            domain_id, namespace_name,
                            validate_bundle_uniqueness=bundle_existed
                        )

                        users = self.permission_assignment.get_users_for_namespace(
                            iam_conn, namespace_name, domain_id
                        )

                        self.permission_assignment.set_namespace_permissions(
                            iam_conn, bundle_id, namespace_mapping_id, users
                        )

                if self.migration_config.get("dry_run", False):
                    iam_conn.rollback()
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

        logger.info(f"Starting namespace migration for {len(domains_config)} domain(s)")
        success_count = 0
        error_count = 0

        for domain_config in domains_config:
            if self.migrate_domain(domain_config):
                success_count += 1
            else:
                error_count += 1

        logger.info(f"Migration completed: {success_count} succeeded, {error_count} failed")
        return error_count == 0