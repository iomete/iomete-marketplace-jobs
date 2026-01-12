"""Domain migration logic for DOMAIN asset type."""

import uuid
from typing import Dict, Any, Optional
from ..common.database import DatabaseManager
from ..common.logger import get_logger
from .queries import (
    GET_DOMAIN_BUNDLE_BY_DOMAIN_ID,
    CREATE_DOMAIN_BUNDLE,
    ADD_DOMAIN_ASSET_TO_BUNDLE,
    INSERT_DOMAIN_PERMISSIONS,
    UPDATE_RESOURCE_BUNDLES_PARENT,
    CHECK_RESOURCE_BUNDLES_EXIST,
    DELETE_DOMAIN_BUNDLE,
    DELETE_BUNDLE_PERMISSIONS,
    DELETE_BUNDLE_ASSETS
)

logger = get_logger(__name__)


class DomainMigration:
    """Handles migration of DOMAIN asset type to bundle-based RAS."""

    def __init__(self, iam_db: DatabaseManager, core_db: DatabaseManager, config: Dict[str, Any]):
        """
        Initialize domain migration.

        Args:
            iam_db: IAM database manager for bundle operations
            core_db: Core database manager (kept for consistency, may not be needed)
            config: Migration configuration
        """
        self.iam_db = iam_db
        self.core_db = core_db
        self.config = config
        self.migration_config = config["migration"]
        self.debug_mode = self.migration_config.get("debug_mode", False)

    def get_or_create_domain_bundle(
        self, connection, domain_id: str, owner_id: str, owner_type: str
    ) -> tuple[Optional[str], bool]:
        """
        Get or create DOMAIN bundle.

        Args:
            connection: Database connection
            domain_id: Domain identifier
            owner_id: Owner identifier
            owner_type: Owner type (USER or GROUP)

        Returns:
            Tuple of (bundle_id, bundle_existed)
            - bundle_id: Bundle UUID or None if skipped
            - bundle_existed: True if bundle already existed
        """
        bundle_name = domain_id + "-domain-bundle"
        check_query = GET_DOMAIN_BUNDLE_BY_DOMAIN_ID

        results = self.iam_db.execute_query(connection, check_query, (bundle_name,))

        if results:
            bundle_id = results[0]['id']
            logger.info(f"Found existing DOMAIN bundle {bundle_id} for domain {domain_id}")

            duplicate_action = self.migration_config.get("duplicate_bundle_action", "FAIL")
            if duplicate_action == "FAIL":
                raise ValueError(
                    f"DOMAIN bundle already exists for {domain_id}. Use UPDATE, SKIP, or OVERWRITE mode."
                )
            elif duplicate_action == "SKIP":
                logger.info(f"Skipping DOMAIN bundle creation for {domain_id} - already exists")
                return (None, False)
            elif duplicate_action == "UPDATE":
                logger.info(f"UPDATE mode: Using existing DOMAIN bundle for {domain_id}")
                return (bundle_id, True)
            elif duplicate_action == "OVERWRITE":
                logger.info(f"OVERWRITE mode: Deleting existing DOMAIN bundle for {domain_id}")
                with connection.cursor() as cursor:
                    cursor.execute(DELETE_BUNDLE_PERMISSIONS, (bundle_id,))
                    cursor.execute(DELETE_BUNDLE_ASSETS, (bundle_id,))
                    cursor.execute(DELETE_DOMAIN_BUNDLE, (bundle_id,))

        # Create new DOMAIN bundle
        bundle_id = str(uuid.uuid4())
        description = f"System-generated bundle for domain {domain_id}"

        with connection.cursor() as cursor:
            cursor.execute(
                CREATE_DOMAIN_BUNDLE,
                (bundle_id, bundle_name, description, owner_id, owner_type, owner_id, owner_id)
            )

        logger.info(f"Created DOMAIN bundle {bundle_id} for domain {domain_id}")
        return (bundle_id, False)

    def add_domain_asset(self, connection, bundle_id: str, domain_id: str):
        """
        Add DOMAIN asset to bundle.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            domain_id: Domain identifier (used as asset_id)
        """
        try:
            with connection.cursor() as cursor:
                cursor.execute(ADD_DOMAIN_ASSET_TO_BUNDLE, (bundle_id, domain_id))
            logger.info(f"Added DOMAIN asset {domain_id} to bundle {bundle_id}")
        except Exception as e:
            logger.error(f"Error adding DOMAIN asset {domain_id} to bundle: {e}")
            raise

    def set_domain_permissions(self, connection, bundle_id: str, domain_id: str):
        """
        Set domain permissions using complex role-based SQL.

        This method executes a complex SQL query that:
        - Maps service/actions to permissions
        - Flattens roles from JSONB
        - Includes both explicit and default roles for users/groups
        - Aggregates permissions per actor

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            domain_id: Domain identifier
        """
        try:
            with connection.cursor() as cursor:
                # SQL requires: domain_id (5x), bundle_id (1x)
                cursor.execute(
                    INSERT_DOMAIN_PERMISSIONS,
                    (domain_id, domain_id, domain_id, domain_id, domain_id, bundle_id)
                )
                affected_rows = cursor.rowcount
                logger.info(
                    f"Set DOMAIN permissions for {affected_rows} actors in domain {domain_id}"
                )
        except Exception as e:
            logger.error(f"Error setting DOMAIN permissions for {domain_id}: {e}")
            raise

    def update_resource_bundles_parent(self, connection, bundle_id: str, domain_id: str):
        """
        Update parent_bundle_id for all RESOURCE bundles in the domain.

        Args:
            connection: Database connection
            bundle_id: Parent DOMAIN bundle ID
            domain_id: Domain identifier
        """
        try:
            # Optional: Check if RESOURCE bundles exist
            results = self.iam_db.execute_query(
                connection, CHECK_RESOURCE_BUNDLES_EXIST, (domain_id,)
            )
            resource_count = results[0]['count'] if results else 0

            if resource_count == 0:
                logger.warning(
                    f"No RESOURCE bundles found in domain {domain_id} to update parent"
                )
                return

            # Update parent_bundle_id
            with connection.cursor() as cursor:
                cursor.execute(UPDATE_RESOURCE_BUNDLES_PARENT, (bundle_id, domain_id))
                updated_count = cursor.rowcount
                logger.info(
                    f"Updated parent_bundle_id for {updated_count} RESOURCE bundles in domain {domain_id}"
                )
        except Exception as e:
            logger.error(f"Error updating RESOURCE bundles parent for {domain_id}: {e}")
            raise

    def validate_domain_config(self, domain_config: Dict[str, Any]) -> bool:
        """
        Validate domain configuration has required fields.

        Args:
            domain_config: Domain configuration dictionary

        Returns:
            True if valid, False otherwise
        """
        required_fields = ['domain_id', 'owner_id', 'owner_type']
        for field in required_fields:
            if field not in domain_config:
                logger.error(f"Missing required field '{field}' in domain configuration")
                return False

        if domain_config['owner_type'] not in ['USER', 'GROUP']:
            logger.error(f"Invalid owner_type: {domain_config['owner_type']}")
            return False

        return True

    def migrate_domain(self, domain_config: Dict[str, Any]) -> bool:
        """
        Migrate a single domain to bundle-based RAS.

        Args:
            domain_config: Domain configuration with domain_id, owner_id, owner_type

        Returns:
            True if migration successful, False otherwise
        """
        if not self.validate_domain_config(domain_config):
            return False

        domain_id = domain_config["domain_id"]
        owner_id = domain_config["owner_id"]
        owner_type = domain_config.get("owner_type", "USER")

        logger.info(f"Starting DOMAIN migration for domain: {domain_id}")

        if self.migration_config.get("dry_run", False):
            logger.warning("DRY RUN MODE - No changes will be committed")

        try:
            with self.iam_db.get_transaction() as iam_conn:
                # 1. Create or get DOMAIN bundle
                bundle_id, bundle_existed = self.get_or_create_domain_bundle(
                    iam_conn, domain_id, owner_id, owner_type
                )

                if bundle_id is None:
                    logger.info(f"Skipping DOMAIN migration for {domain_id}")
                    return True

                # 2. Add DOMAIN asset to bundle
                self.add_domain_asset(iam_conn, bundle_id, domain_id)

                # 3. Set domain permissions (complex role-based)
                self.set_domain_permissions(iam_conn, bundle_id, domain_id)

                # 4. Update parent_bundle_id for RESOURCE bundles
                self.update_resource_bundles_parent(iam_conn, bundle_id, domain_id)

                if self.migration_config.get("dry_run", False):
                    iam_conn.rollback()
                    logger.info(f"DRY RUN: Changes rolled back for domain {domain_id}")
                else:
                    logger.info(
                        f"Successfully migrated DOMAIN for domain {domain_id}"
                    )

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

        logger.info(f"Starting DOMAIN migration for {len(domains_config)} domain(s)")
        success_count = 0
        error_count = 0

        for domain_config in domains_config:
            if self.migrate_domain(domain_config):
                success_count += 1
            else:
                error_count += 1

        logger.info(f"DOMAIN migration completed: {success_count} succeeded, {error_count} failed")
        return error_count == 0
