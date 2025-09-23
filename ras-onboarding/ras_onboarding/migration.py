"""Core migration logic for asset onboarding."""

from typing import Dict, Any, List
from .database import DatabaseManager
from .logger import get_logger

logger = get_logger(__name__)


class AssetOnboardingMigration:
    """Handles the asset onboarding migration process for any asset type."""

    def __init__(self, bundle_migration_db: DatabaseManager, asset_dbs: Dict[str, DatabaseManager], config: Dict[str, Any]):
        self.bundle_migration_db = bundle_migration_db
        self.asset_dbs = asset_dbs
        self.migration_config = config.get("migration", {})
        self.asset_mappings = config.get("asset_mappings", {})
        self.debug_mode = self.migration_config.get("debug_mode", False)

    def create_default_bundle(self, connection, domain_id: str, owner_id: str, owner_type: str) -> str:
        """
        Create default bundle for a domain.

        Args:
            connection: Database connection
            domain_id: Domain identifier
            owner_id: Owner identifier
            owner_type: Owner type (USER or GROUP)

        Returns:
            Generated bundle ID
        """
        bundle_name = f"{domain_id}_default"
        description = "Default Bundle"

        insert_bundle_query = """
            INSERT INTO bundle (id, name, description, owner_id, owner_type, domain, created_at, created_by, updated_at, updated_by, is_archived)
            VALUES (gen_random_uuid(), %s, %s, %s, %s, %s, current_timestamp, 'system', current_timestamp, 'system', false)
            RETURNING id
        """

        bundle_id = self.bundle_migration_db.execute_insert(connection, insert_bundle_query, (bundle_name, description, owner_id, owner_type, domain_id))

        logger.info(f"Created default bundle {bundle_id} for domain {domain_id}")
        return bundle_id

    def get_domain_assets(self, asset_db: DatabaseManager, domain_id: str, asset_type: str) -> List[str]:
        """
        Fetch assets for a given domain from the correct asset DB.
        """
        mapping = self.asset_mappings.get(asset_type)
        if not mapping:
            raise ValueError(f"Unknown asset type: {asset_type}")

        table = mapping["table"]
        id_column = mapping["id_column"]
        domain_column = mapping["domain_column"]

        query = f"""
            SELECT {id_column}
            FROM {table}
            WHERE is_deleted = false AND {domain_column} = %s
        """

        with asset_db.get_connection() as conn:
            results = asset_db.execute_query(conn, query, (domain_id,))
            asset_ids = [row[id_column] for row in results] if results else []

        logger.info(f"Found {len(asset_ids)} {asset_type.lower()} assets in domain {domain_id}")
        return asset_ids

    def move_assets_to_bundle(self, connection, bundle_id: str, asset_ids: List[str], asset_type: str):
        """
        Move assets to the default bundle.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            asset_ids: List of asset IDs to move
            asset_type: Asset type (COMPUTE, PIPELINE, DATASET, etc.)
        """
        if not asset_ids:
            logger.info(f"No {asset_type.lower()} assets to move")
            return

        # Build the values clause for the insert
        values_clause = ', '.join(
            f"('{bundle_id}', '{asset_type}', '{asset_id}', current_timestamp, 'system', current_timestamp, 'system')"
            for asset_id in asset_ids
        )

        insert_bundle_assets_query = f"""
            INSERT INTO bundle_asset (bundle_id, asset_type, asset_id, created_at, created_by, updated_at, updated_by)
            VALUES {values_clause}
        """

        with connection.cursor() as cursor:
            cursor.execute(insert_bundle_assets_query)
        logger.info(f"Moved {len(asset_ids)} {asset_type.lower()} assets to bundle {bundle_id}")


    def set_user_permissions(self, connection, bundle_id: str, domain_id: str, asset_type: str):
        """
        Set user permissions for the bundle based on existing role mappings.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            domain_id: Domain identifier
            asset_type: Asset type (COMPUTE, PIPELINE, DATASET, etc.)
        """
        mapping = self.asset_mappings.get(asset_type)
        if not mapping:
            raise ValueError(f"Unknown asset type: {asset_type}")

        permission_filter = mapping["permissions_filter"]

        # Make sure LIKE works as intended
        if not permission_filter.startswith("%"):
            permission_filter = f"%{permission_filter}%"

        user_permissions_query = """
            WITH user_permissions AS (
                SELECT DISTINCT
                    u.username,
                    ARRAY_AGG(DISTINCT perm) as new_permissions
                FROM iam_user u
                JOIN user_role_mapping_v2 urm ON urm.username = u.username
                JOIN iam_role r ON r.name = urm.role_name AND r.domain = urm.domain
                CROSS JOIN LATERAL (
                    SELECT unnest(ARRAY[
                        CASE WHEN r.permissions::text LIKE '%%"list"%%' THEN 'VIEW' END,
                        CASE WHEN r.permissions::text LIKE '%%"view"%%' THEN 'VIEW' END,
                        CASE WHEN r.permissions::text LIKE '%%"manage"%%' THEN 'UPDATE' END,
                        CASE WHEN r.permissions::text LIKE '%%"manage"%%' THEN 'DELETE' END,
                        CASE WHEN r.permissions::text LIKE '%%"manage"%%' THEN 'EXECUTE' END,
                        CASE WHEN r.permissions::text LIKE '%%"manage"%%' THEN 'CONSUME' END
                    ]) as perm
                ) perms
                WHERE r.domain = %s
                AND u.is_deleted = false
                AND r.is_deleted = false
                AND r.permissions::text LIKE %s
                AND perm IS NOT NULL
                GROUP BY u.username
            )
            INSERT INTO bundle_permission (bundle_id, asset_type, actor_type, actor_id, permissions, created_at, created_by, updated_at, updated_by)
            SELECT
                %s,
                %s,
                'USER',
                username,
                new_permissions,
                current_timestamp,
                'system',
                current_timestamp,
                'system'
            FROM user_permissions
            ORDER BY username
        """


        affected_rows = 0
        logger.debug(f"update: {user_permissions_query}")
        logger.debug(f"Parameters: ({domain_id}, {permission_filter}, {bundle_id}, {asset_type})")

        with connection.cursor() as cursor:
                cursor.execute(user_permissions_query, (domain_id, permission_filter,bundle_id, asset_type))
                affected_rows = cursor.rowcount

        logger.info(f"Set permissions for {affected_rows} users in domain {domain_id}")

    def set_group_permissions(self, connection, bundle_id: str, domain_id: str, asset_type: str):
        """
        Set group permissions for the bundle based on existing role mappings.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            domain_id: Domain identifier
            asset_type: Asset type (typically 'COMPUTE')
        """
        mapping = self.asset_mappings.get(asset_type)
        if not mapping:
            raise ValueError(f"Unknown asset type: {asset_type}")

        permission_filter = mapping["permissions_filter"]

        # Make sure LIKE works as intended
        if not permission_filter.startswith("%"):
            permission_filter = f"%{permission_filter}%"

        group_permissions_query = """
            WITH group_permissions AS (
                SELECT DISTINCT
                    g.name as group_name,
                    ARRAY_AGG(DISTINCT perm) as new_permissions
                FROM iam_group g
                JOIN group_role_mapping_v2 grm ON grm.group_name = g.name
                JOIN iam_role r ON r.name = grm.role_name AND r.domain = grm.domain
                CROSS JOIN LATERAL (
                    SELECT unnest(ARRAY[
                        CASE WHEN r.permissions::text LIKE '%%"list"%%' THEN 'VIEW' END,
                        CASE WHEN r.permissions::text LIKE '%%"view"%%' THEN 'VIEW' END,
                        CASE WHEN r.permissions::text LIKE '%%"manage"%%' THEN 'UPDATE' END,
                        CASE WHEN r.permissions::text LIKE '%%"manage"%%' THEN 'DELETE' END,
                        CASE WHEN r.permissions::text LIKE '%%"manage"%%' THEN 'EXECUTE' END,
                        CASE WHEN r.permissions::text LIKE '%%"manage"%%' THEN 'CONSUME' END
                    ]) as perm
                ) perms
                WHERE r.domain = %s
                AND g.is_deleted = false
                AND r.is_deleted = false
                AND r.permissions::text LIKE %s
                AND perm IS NOT NULL
                GROUP BY g.name
            )
            INSERT INTO bundle_permission (bundle_id, asset_type, actor_type, actor_id, permissions, created_at, created_by, updated_at, updated_by)
            SELECT
                %s,
                %s,
                'GROUP',
                group_name,
                new_permissions,
                current_timestamp,
                'system',
                current_timestamp,
                'system'
            FROM group_permissions
            ORDER BY group_name
        """


        affected_rows = 0
        with connection.cursor() as cursor:
            cursor.execute(group_permissions_query, (domain_id, permission_filter, bundle_id, asset_type))
            affected_rows = cursor.rowcount

        logger.info(f"Set permissions for {affected_rows} groups in domain {domain_id}")

    def check_existing_bundle(self, connection, domain_id: str) -> Dict[str, Any]:
        """
        Check if default bundle already exists for a domain.

        Args:
            connection: Database connection
            domain_id: Domain identifier

        Returns:
            Dictionary with bundle info if exists, None otherwise
        """
        existing_bundle_query = """
            SELECT id, owner_id, owner_type FROM bundle
            WHERE name = %s AND domain = %s AND is_archived = false
        """
        bundle_name = f"{domain_id}_default"
        results = self.bundle_migration_db.execute_query(connection, existing_bundle_query, (bundle_name, domain_id))

        return results[0] if results else None

    def validate_domain_migration(self, connection, domain_id: str, asset_type: str) -> Dict[str, Any]:
        """
        Validate that domain migration can proceed.

        Args:
            connection: Database connection
            domain_id: Domain identifier
            asset_type: Asset type to validate

        Returns:
            Dictionary with validation result and existing bundle info
        """
        # Check if domain exists and has assets
        asset_db = self.asset_dbs.get(asset_type)
        if not asset_db:
            raise ValueError(f"No database configured for asset type: {asset_type}")

        assets = self.get_domain_assets(asset_db, domain_id, asset_type)
        if not assets:
            logger.warning(f"No {asset_type.lower()} assets found in domain {domain_id}")
            return {"can_proceed": True, "existing_bundle": None, "has_assets": False}

        # Check if default bundle already exists
        existing_bundle = self.check_existing_bundle(connection, domain_id)
        duplicate_action = self.migration_config.get('duplicate_bundle_action', 'FAIL').upper()

        if existing_bundle:
            if duplicate_action == 'FAIL':
                logger.error(f"Default bundle already exists for domain {domain_id} (action: FAIL)")
                return {"can_proceed": False, "existing_bundle": existing_bundle, "has_assets": True}
            elif duplicate_action == 'SKIP':
                logger.info(f"Default bundle already exists for domain {domain_id} (action: SKIP)")
                return {"can_proceed": False, "existing_bundle": existing_bundle, "has_assets": True, "skip": True}
            elif duplicate_action == 'UPDATE':
                logger.info(f"Default bundle already exists for domain {domain_id} (action: UPDATE)")
                return {"can_proceed": True, "existing_bundle": existing_bundle, "has_assets": True, "update": True}

        return {"can_proceed": True, "existing_bundle": None, "has_assets": True}

    def update_existing_bundle(self, connection, bundle_id: str, owner_id: str, owner_type: str, domain_id: str):
        """
        Update existing bundle ownership and metadata.

        Args:
            connection: Database connection
            bundle_id: Existing bundle ID
            owner_id: New owner identifier
            owner_type: New owner type (USER or GROUP)
            domain_id: Domain identifier
        """
        update_bundle_query = """
            UPDATE bundle
            SET owner_id = %s, owner_type = %s, updated_at = current_timestamp, updated_by = 'system'
            WHERE id = %s
        """

        logger.debug(f"update: {update_bundle_query}")
        logger.debug(f"Parameters: ({owner_id}, {owner_type}, {bundle_id})")

        with connection.cursor() as cursor:
            cursor.execute(update_bundle_query, (owner_id, owner_type, bundle_id))

        logger.info(f"Updated existing bundle {bundle_id} ownership to {owner_type}:{owner_id} for domain {domain_id}")

    def clear_bundle_assets(self, connection, bundle_id: str, asset_type: str):
        """
        Clear existing assets from bundle for re-processing.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            asset_type: Asset type to clear
        """
        clear_assets_query = """
            DELETE FROM bundle_asset
            WHERE bundle_id = %s AND asset_type = %s
        """

        logger.debug(f"delete: {clear_assets_query}")
        logger.debug(f"Parameters: ({bundle_id}, {asset_type})")

        affected_rows = 0
        with connection.cursor() as cursor:
            cursor.execute(clear_assets_query, (bundle_id, asset_type))
            affected_rows = cursor.rowcount

        logger.info(f"Cleared {affected_rows} existing {asset_type.lower()} assets from bundle {bundle_id}")

    def clear_bundle_permissions(self, connection, bundle_id: str, asset_type: str):
        """
        Clear existing permissions from bundle for re-processing.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            asset_type: Asset type to clear permissions for
        """
        clear_permissions_query = """
            DELETE FROM bundle_permission
            WHERE bundle_id = %s AND asset_type = %s
        """

        logger.info(f"delete: {clear_permissions_query}")
        logger.info(f"Parameters: ({bundle_id}, {asset_type})")

        affected_rows = 0
        with connection.cursor() as cursor:
            cursor.execute(clear_permissions_query, (bundle_id, asset_type))
            affected_rows = cursor.rowcount

        logger.info(f"Cleared {affected_rows} existing permissions for {asset_type.lower()} from bundle {bundle_id}")

    def migrate_domain(self, domain_config: Dict[str, Any]) -> bool:
        domain_id = domain_config["domain_id"]
        owner_id = domain_config["owner_id"]
        owner_type = domain_config["owner_type"]
        asset_type = domain_config.get("asset_type", "COMPUTE")

        try:
            with self.bundle_migration_db.get_transaction() as mig_conn:
                # Validation
                validation = self.validate_domain_migration(mig_conn, domain_id, asset_type)
                if not validation["can_proceed"]:
                    return validation.get("skip", False)

                existing_bundle = validation.get("existing_bundle")
                is_update = validation.get("update", False)

                if is_update and existing_bundle:
                    bundle_id = existing_bundle["id"]
                    self.update_existing_bundle(mig_conn, bundle_id, owner_id, owner_type, domain_id)
                    self.clear_bundle_assets(mig_conn, bundle_id, asset_type)
                    self.clear_bundle_permissions(mig_conn, bundle_id, asset_type)
                else:
                    bundle_id = self.create_default_bundle(mig_conn, domain_id, owner_id, owner_type)

                # Use asset DB for fetching assets
                asset_db = self.asset_dbs.get(asset_type)
                if not asset_db:
                    raise Exception(f"No asset DB configured for {asset_type}")

                asset_ids = self.get_domain_assets(asset_db, domain_id, asset_type)

                # Insert assets + permissions into migration DB
                self.move_assets_to_bundle(mig_conn, bundle_id, asset_ids, asset_type)
                self.set_user_permissions(mig_conn, bundle_id, domain_id, asset_type)
                self.set_group_permissions(mig_conn, bundle_id, domain_id, asset_type)

                logger.info(f"Domain {domain_id} migrated with {len(asset_ids)} {asset_type} assets")
                return True
        except Exception as e:
            logger.error(f"Migration failed for domain {domain_id}: {e}")
            return False

    def run_migration(self) -> bool:
        """
        Run migration for all configured domains.

        Returns:
            True if all migrations successful, False otherwise
        """
        domains = self.migration_config.get('domains', [])
        if not domains:
            logger.warning("No domains configured for migration")
            return True

        logger.info(f"Starting migration for {len(domains)} domains")

        success_count = 0
        for domain_config in domains:
            if self.migrate_domain(domain_config):
                success_count += 1
            else:
                logger.error(f"Failed to migrate domain {domain_config.get('domain_id', 'unknown')}")

        total_domains = len(domains)
        logger.info(f"Migration completed: {success_count}/{total_domains} domains successful")

        return success_count == total_domains