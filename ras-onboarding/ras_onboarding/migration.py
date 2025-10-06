"""Core migration logic for asset onboarding."""

from typing import Dict, Any, List
from .database import DatabaseManager
from .logger import get_logger

logger = get_logger(__name__)


class AssetOnboardingMigration:
    """Handles the asset onboarding migration process for any asset type."""

    def __init__(self, bundle_migration_db: DatabaseManager, asset_db: DatabaseManager, config: Dict[str, Any]):
        self.bundle_migration_db = bundle_migration_db
        self.asset_db = asset_db
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

    def build_asset_query(self, asset_type: str) -> str:
        """
        Build dynamic asset query from configuration.

        Args:
            asset_type: Asset type to build query for

        Returns:
            SQL query string for fetching assets
        """
        mapping = self.asset_mappings.get(asset_type)
        if not mapping:
            raise ValueError(f"Unknown asset type: {asset_type}")

        table = mapping["table"]
        id_column = mapping["id_column"]
        domain_column = mapping["domain_column"]

        base_query = f"""
            SELECT {id_column}
            FROM {table}
            WHERE {domain_column} = %s
        """

        # Add optional filter condition
        if filter_condition := mapping.get('filter_condition'):
            base_query += f" AND {filter_condition}"

        return base_query

    def get_domain_assets(self, asset_db: DatabaseManager, domain_id: str, asset_type: str) -> List[str]:
        """
        Fetch assets for a given domain from the correct asset DB.
        """
        mapping = self.asset_mappings.get(asset_type)
        if not mapping:
            raise ValueError(f"Unknown asset type: {asset_type}")

        id_column = mapping["id_column"]
        query = self.build_asset_query(asset_type)

        with asset_db.get_connection() as conn:
            results = asset_db.execute_query(conn, query, (domain_id,))
            asset_ids = [row[id_column] for row in results] if results else []

        logger.info(f"Found {len(asset_ids)} {asset_type.lower()} assets in domain {domain_id}")
        return asset_ids

    def get_existing_bundle_assets(self, connection, bundle_id: str, asset_type: str, asset_ids: List[str]) -> List[str]:
        """
        Get assets that already exist in the bundle.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            asset_type: Asset type
            asset_ids: List of asset IDs to check

        Returns:
            List of asset IDs that already exist in the bundle
        """
        if not asset_ids:
            return []

        # Build query to check which assets already exist
        asset_ids_str = ', '.join(f"'{asset_id}'" for asset_id in asset_ids)
        check_existing_query = f"""
            SELECT asset_id FROM bundle_asset
            WHERE bundle_id = %s AND asset_type = %s AND asset_id IN ({asset_ids_str})
        """

        results = self.bundle_migration_db.execute_query(connection, check_existing_query, (bundle_id, asset_type))
        existing_asset_ids = [row['asset_id'] for row in results] if results else []

        logger.info(f"Found {len(existing_asset_ids)} existing {asset_type.lower()} assets in bundle {bundle_id}")
        return existing_asset_ids

    def get_existing_bundle_permissions(self, connection, bundle_id: str, asset_type: str) -> List[Dict[str, Any]]:
        """
        Get existing permissions for an asset type in the bundle.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            asset_type: Asset type

        Returns:
            List of existing permission records
        """
        check_permissions_query = """
            SELECT actor_type, actor_id, permissions FROM bundle_permission
            WHERE bundle_id = %s AND asset_type = %s
        """

        results = self.bundle_migration_db.execute_query(connection, check_permissions_query, (bundle_id, asset_type))
        logger.info(f"Found {len(results)} existing permission records for {asset_type.lower()} in bundle {bundle_id}")
        return results if results else []

    def move_assets_to_bundle(self, connection, bundle_id: str, asset_ids: List[str], asset_type: str, asset_action_on_duplicate: str = 'UPDATE'):
        """
        Move assets to the default bundle.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            asset_ids: List of asset IDs to move
            asset_type: Asset type (COMPUTE, PIPELINE, DATASET, etc.)
            asset_action_on_duplicate: Action for duplicate assets (SKIP, UPDATE, ERROR, RESET)
        """
        if not asset_ids:
            logger.info(f"No {asset_type.lower()} assets to move")
            return

        asset_action = asset_action_on_duplicate.upper()

        # Check for existing assets
        existing_asset_ids = self.get_existing_bundle_assets(connection, bundle_id, asset_type, asset_ids)

        # Handle ERROR action
        if asset_action == 'ERROR' and existing_asset_ids:
            raise ValueError(
                f"Asset action is ERROR and {len(existing_asset_ids)} {asset_type.lower()} assets already exist in bundle {bundle_id}: {existing_asset_ids[:5]}"
            )

        # Handle SKIP action - only insert new assets
        if asset_action == 'SKIP':
            asset_ids_to_insert = [aid for aid in asset_ids if aid not in existing_asset_ids]
            if not asset_ids_to_insert:
                logger.info(f"All {len(asset_ids)} {asset_type.lower()} assets already exist in bundle (action: SKIP)")
                return
            logger.info(f"Skipping {len(existing_asset_ids)} existing assets, inserting {len(asset_ids_to_insert)} new {asset_type.lower()} assets")
            asset_ids = asset_ids_to_insert

        # For UPDATE and RESET: insert with ON CONFLICT DO NOTHING (RESET already cleared assets)
        # Build the values clause for the insert
        values_clause = ', '.join(
            f"('{bundle_id}', '{asset_type}', '{asset_id}', current_timestamp, 'system', current_timestamp, 'system')"
            for asset_id in asset_ids
        )

        insert_bundle_assets_query = f"""
            INSERT INTO bundle_asset (bundle_id, asset_type, asset_id, created_at, created_by, updated_at, updated_by)
            VALUES {values_clause}
            ON CONFLICT (bundle_id, asset_type, asset_id) DO NOTHING
        """

        with connection.cursor() as cursor:
            cursor.execute(insert_bundle_assets_query)
        logger.info(f"Moved {len(asset_ids)} {asset_type.lower()} assets to bundle {bundle_id} (action: {asset_action})")

    def build_permission_subquery(self, asset_type: str) -> str:
        """
        Build dynamic permission extraction subquery based on asset type configuration.

        Args:
            asset_type: Asset type to build permissions for

        Returns:
            SQL subquery for extracting permissions
        """
        mapping = self.asset_mappings.get(asset_type)
        if not mapping:
            raise ValueError(f"Unknown asset type: {asset_type}")

        service = mapping['service']
        permission_mappings = mapping.get('permission_mappings', {})

        # Build CASE statements dynamically
        case_statements = []
        for action, permissions in permission_mappings.items():
            for perm in permissions:
                case_statements.append(
                    f"CASE WHEN jsonb_path_exists({service}_perms, '$.actions[*] ? (@.action == \"{action}\")') THEN '{perm}' END"
                )

        case_statements_joined = ',\n                         '.join(case_statements)
        return f"""
                     WITH {service}_service AS (
                         SELECT jsonb_path_query(r.permissions::jsonb, '$[*] ? (@.service == "{service}")') as {service}_perms
                     )
                     SELECT unnest(ARRAY[
                         {case_statements_joined}
                         ]) as perm
                     FROM {service}_service
                     WHERE {service}_perms IS NOT NULL
        """

    def validate_asset_configuration(self, asset_type: str) -> Dict[str, Any]:
        """
        Validate asset type configuration.

        Args:
            asset_type: Asset type to validate

        Returns:
            Validation result dictionary
        """
        mapping = self.asset_mappings.get(asset_type)
        if not mapping:
            return {
                "is_valid": False,
                "error": f"Asset type '{asset_type}' not found in configuration"
            }

        required_fields = ['table', 'id_column', 'domain_column', 'service', 'asset_action_on_duplicate']
        for field in required_fields:
            if not mapping.get(field):
                return {
                    "is_valid": False,
                    "error": f"Missing required field '{field}' for asset type '{asset_type}'"
                }

        # Validate permission mappings exist
        if not mapping.get('permission_mappings'):
            return {
                "is_valid": False,
                "error": f"Missing permission_mappings for asset type '{asset_type}'"
            }

        # Validate asset_action_on_duplicate has valid value
        valid_actions = ['SKIP', 'UPDATE', 'ERROR', 'RESET']
        asset_action = mapping.get('asset_action_on_duplicate', '').upper()
        if asset_action not in valid_actions:
            return {
                "is_valid": False,
                "error": f"Invalid asset_action_on_duplicate '{asset_action}' for asset type '{asset_type}'. Must be one of: {', '.join(valid_actions)}"
            }

        return {"is_valid": True}

    def get_asset_types_from_config(self, domain_config: Dict[str, Any]) -> List[str]:
        """
        Extract asset types from domain configuration.

        Args:
            domain_config: Domain configuration dictionary

        Returns:
            List of asset types to migrate
        """
        # Support both 'asset_types' (preferred) and 'asset_type' (backward compatibility)
        if 'asset_types' in domain_config:
            asset_types = domain_config['asset_types']
            if isinstance(asset_types, list):
                return asset_types
            else:
                return [asset_types]  # Convert single item to list
        elif 'asset_type' in domain_config:
            # Backward compatibility: convert single asset_type to list
            return [domain_config['asset_type']]
        else:
            return ["COMPUTE"]  # Default fallback

    def set_user_permissions(self, connection, bundle_id: str, domain_id: str, asset_type: str, asset_action_on_duplicate: str = 'UPDATE'):
        """
        Set user permissions for the bundle based on existing role mappings.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            domain_id: Domain identifier
            asset_type: Asset type (COMPUTE, PIPELINE, DATASET, etc.)
            asset_action_on_duplicate: Action for duplicate permissions (SKIP, UPDATE, ERROR, RESET)
        """
        # Validate asset configuration
        validation = self.validate_asset_configuration(asset_type)
        if not validation["is_valid"]:
            raise ValueError(f"Asset configuration validation failed: {validation['error']}")

        asset_action = asset_action_on_duplicate.upper()

        # Check for existing permissions
        existing_permissions = self.get_existing_bundle_permissions(connection, bundle_id, asset_type)

        # Handle ERROR action
        if asset_action == 'ERROR' and existing_permissions:
            raise ValueError(
                f"Asset action is ERROR and {len(existing_permissions)} permission records already exist for {asset_type.lower()} in bundle {bundle_id}"
            )

        # Handle SKIP action
        if asset_action == 'SKIP' and existing_permissions:
            logger.info(f"Skipping permission setting for {asset_type.lower()} as {len(existing_permissions)} records already exist (action: SKIP)")
            return

        # Build dynamic permission subquery
        permission_subquery = self.build_permission_subquery(asset_type)

        # For UPDATE: merge permissions using array concatenation and deduplication
        # For RESET: RESET already cleared permissions, so just insert
        on_conflict_clause = ""
        if asset_action == 'UPDATE':
            on_conflict_clause = """
            ON CONFLICT (bundle_id, asset_type, actor_type, actor_id)
            DO UPDATE SET
                permissions = ARRAY(SELECT DISTINCT unnest(bundle_permission.permissions || EXCLUDED.permissions)),
                updated_at = current_timestamp,
                updated_by = 'system'
            """
        elif asset_action == 'RESET':
            on_conflict_clause = "ON CONFLICT (bundle_id, asset_type, actor_type, actor_id) DO NOTHING"

        user_permissions_query = f"""
            WITH all_domain_users AS (
                SELECT dm.identity_id as username
                FROM domain_member dm
                         JOIN iam_user u ON u.username = dm.identity_id
                WHERE dm.domain_id = %s
                  AND dm.identity_type = 'USER'
                  AND u.is_deleted = false
            ),
             user_all_permissions AS (
                 SELECT
                     adu.username,
                     ARRAY_AGG(DISTINCT perm) FILTER (WHERE perm IS NOT NULL) as all_permissions
                 FROM all_domain_users adu
                          LEFT JOIN user_role_mapping_v2 urm ON urm.username = adu.username AND urm.domain = %s
                          LEFT JOIN iam_role r ON (r.name = urm.role_name AND r.domain = urm.domain AND r.is_deleted = false)
                     OR (r.name = 'default' AND r.domain = %s AND r.is_deleted = false)
                          CROSS JOIN LATERAL ({permission_subquery.strip()}
                     ) perms
                 GROUP BY adu.username
             )
            INSERT INTO bundle_permission (bundle_id, asset_type, actor_type, actor_id, permissions, created_at, created_by, updated_at, updated_by)
            SELECT
                %s,
                %s,
                'USER',
                username,
                all_permissions,
                current_timestamp,
                'system',
                current_timestamp,
                'system'
            FROM user_all_permissions
            WHERE all_permissions IS NOT NULL
            ORDER BY username
            {on_conflict_clause}
        """

        affected_rows = 0
        logger.debug(f"update: {user_permissions_query}")
        logger.debug(f"Parameters: ({domain_id}, {domain_id}, {domain_id}, {bundle_id}, {asset_type})")

        with connection.cursor() as cursor:
                cursor.execute(user_permissions_query, (domain_id, domain_id, domain_id, bundle_id, asset_type))
                affected_rows = cursor.rowcount

        logger.info(f"Set permissions for {affected_rows} users in domain {domain_id} (action: {asset_action})")

    def set_group_permissions(self, connection, bundle_id: str, domain_id: str, asset_type: str, asset_action_on_duplicate: str = 'UPDATE'):
        """
        Set group permissions for the bundle based on existing role mappings.

        Args:
            connection: Database connection
            bundle_id: Bundle identifier
            domain_id: Domain identifier
            asset_type: Asset type (COMPUTE, PIPELINE, DATASET, etc.)
            asset_action_on_duplicate: Action for duplicate permissions (SKIP, UPDATE, ERROR, RESET)
        """
        # Validate asset configuration
        validation = self.validate_asset_configuration(asset_type)
        if not validation["is_valid"]:
            raise ValueError(f"Asset configuration validation failed: {validation['error']}")

        asset_action = asset_action_on_duplicate.upper()

        # Check for existing permissions
        existing_permissions = self.get_existing_bundle_permissions(connection, bundle_id, asset_type)

        # Handle ERROR action
        if asset_action == 'ERROR' and existing_permissions:
            raise ValueError(
                f"Asset action is ERROR and {len(existing_permissions)} permission records already exist for {asset_type.lower()} in bundle {bundle_id}"
            )

        # Handle SKIP action
        if asset_action == 'SKIP' and existing_permissions:
            logger.info(f"Skipping permission setting for {asset_type.lower()} groups as {len(existing_permissions)} records already exist (action: SKIP)")
            return

        # Build dynamic permission subquery
        permission_subquery = self.build_permission_subquery(asset_type)

        # For UPDATE: merge permissions using array concatenation and deduplication
        # For RESET: RESET already cleared permissions, so just insert
        on_conflict_clause = ""
        if asset_action == 'UPDATE':
            on_conflict_clause = """
            ON CONFLICT (bundle_id, asset_type, actor_type, actor_id)
            DO UPDATE SET
                permissions = ARRAY(SELECT DISTINCT unnest(bundle_permission.permissions || EXCLUDED.permissions)),
                updated_at = current_timestamp,
                updated_by = 'system'
            """
        elif asset_action == 'RESET':
            on_conflict_clause = "ON CONFLICT (bundle_id, asset_type, actor_type, actor_id) DO NOTHING"

        group_permissions_query = f"""
            WITH all_domain_groups AS (
                SELECT dm.identity_id as group_name
                FROM domain_member dm
                         JOIN iam_group g ON g.name = dm.identity_id
                WHERE dm.domain_id = %s
                  AND dm.identity_type = 'GROUP'
                  AND g.is_deleted = false
            ),
             group_all_permissions AS (
                 SELECT
                     adg.group_name,
                     ARRAY_AGG(DISTINCT perm) FILTER (WHERE perm IS NOT NULL) as all_permissions
                 FROM all_domain_groups adg
                          LEFT JOIN group_role_mapping_v2 grm ON grm.group_name = adg.group_name AND grm.domain = %s
                          LEFT JOIN iam_role r ON (r.name = grm.role_name AND r.domain = grm.domain AND r.is_deleted = false)
                     OR (r.name = 'default' AND r.domain = %s AND r.is_deleted = false)
                          CROSS JOIN LATERAL ({permission_subquery.strip()}
                     ) perms
                 GROUP BY adg.group_name
             )
            INSERT INTO bundle_permission (bundle_id, asset_type, actor_type, actor_id, permissions, created_at, created_by, updated_at, updated_by)
            SELECT
                %s,
                %s,
                'GROUP',
                group_name,
                all_permissions,
                current_timestamp,
                'system',
                current_timestamp,
                'system'
            FROM group_all_permissions
            WHERE all_permissions IS NOT NULL
            ORDER BY group_name
            {on_conflict_clause}
        """

        affected_rows = 0
        with connection.cursor() as cursor:
            cursor.execute(group_permissions_query, (domain_id, domain_id, domain_id, bundle_id, asset_type))
            affected_rows = cursor.rowcount

        logger.info(f"Set permissions for {affected_rows} groups in domain {domain_id} (action: {asset_action})")

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

    def validate_owner(self, connection, owner_id: str, owner_type: str) -> Dict[str, Any]:
        """
        Validate that the owner exists and owner type is valid.

        Args:
            connection: Database connection
            owner_id: Owner identifier
            owner_type: Owner type (USER or GROUP)

        Returns:
            Dictionary with validation result and error message if invalid
        """
        # Validate owner_type is valid
        valid_owner_types = ['USER', 'GROUP']
        if owner_type not in valid_owner_types:
            return {
                "is_valid": False,
                "error": f"Invalid owner_type '{owner_type}'. Must be one of: {', '.join(valid_owner_types)}"
            }

        # Validate owner exists in database based on owner_type
        if owner_type == 'USER':
            owner_query = """
                SELECT username FROM iam_user
                WHERE username = %s AND is_deleted = false
            """
        else:  # owner_type == 'GROUP'
            owner_query = """
                SELECT name FROM iam_group
                WHERE name = %s AND is_deleted = false
            """

        try:
            results = self.bundle_migration_db.execute_query(connection, owner_query, (owner_id,))
            if not results:
                entity_type = "user" if owner_type == 'USER' else "group"
                return {
                    "is_valid": False,
                    "error": f"Owner {entity_type} '{owner_id}' not found or is deleted"
                }

            logger.info(f"Owner validation successful: {owner_type}:{owner_id}")
            return {"is_valid": True}

        except Exception as e:
            logger.error(f"Error validating owner {owner_type}:{owner_id}: {e}")
            return {
                "is_valid": False,
                "error": f"Database error while validating owner: {str(e)}"
            }

    def validate_domain_migration(self, connection, domain_id: str, asset_type: str, owner_id: str = None, owner_type: str = None) -> Dict[str, Any]:
        """
        Validate that domain migration can proceed.

        Args:
            connection: Database connection
            domain_id: Domain identifier
            asset_type: Asset type to validate
            owner_id: Owner identifier (optional for validation)
            owner_type: Owner type (optional for validation)

        Returns:
            Dictionary with validation result and existing bundle info
        """
        # Validate owner if provided
        if owner_id and owner_type:
            owner_validation = self.validate_owner(connection, owner_id, owner_type)
            if not owner_validation["is_valid"]:
                logger.error(f"Owner validation failed: {owner_validation['error']}")
                return {"can_proceed": False, "owner_validation_error": owner_validation['error']}

        # Check if domain exists and has assets
        asset_db = self.asset_db

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

    def migrate_single_asset_type(self, domain_id: str, owner_id: str, owner_type: str, asset_type: str) -> bool:
        """
        Migrate a single asset type for a domain.

        Args:
            domain_id: Domain identifier
            owner_id: Owner identifier
            owner_type: Owner type (USER or GROUP)
            asset_type: Asset type to migrate

        Returns:
            True if migration successful, False otherwise
        """
        try:
            # Validate asset configuration before starting migration
            asset_validation = self.validate_asset_configuration(asset_type)
            if not asset_validation["is_valid"]:
                logger.error(f"Asset configuration validation failed for {asset_type}: {asset_validation['error']}")
                return False

            # Get asset-specific duplicate action from configuration
            asset_mapping = self.asset_mappings.get(asset_type)
            asset_action_on_duplicate = asset_mapping.get('asset_action_on_duplicate', 'UPDATE').upper()
            logger.info(f"Asset action on duplicate for {asset_type}: {asset_action_on_duplicate}")

            with self.bundle_migration_db.get_transaction() as mig_conn:
                # Validation
                validation = self.validate_domain_migration(mig_conn, domain_id, asset_type, owner_id, owner_type)
                if not validation["can_proceed"]:
                    return validation.get("skip", False)

                existing_bundle = validation.get("existing_bundle")
                is_update = validation.get("update", False)

                if is_update and existing_bundle:
                    bundle_id = existing_bundle["id"]
                    self.update_existing_bundle(mig_conn, bundle_id, owner_id, owner_type, domain_id)

                    # Handle RESET action - clear assets and permissions for this asset type only
                    if asset_action_on_duplicate == 'RESET':
                        logger.info(f"RESET action: clearing {asset_type} assets and permissions from bundle {bundle_id}")
                        self.clear_bundle_assets(mig_conn, bundle_id, asset_type)
                        self.clear_bundle_permissions(mig_conn, bundle_id, asset_type)
                else:
                    bundle_id = self.create_default_bundle(mig_conn, domain_id, owner_id, owner_type)

                # Use asset DB for fetching assets
                asset_db = self.asset_db

                asset_ids = self.get_domain_assets(asset_db, domain_id, asset_type)

                # Insert assets + permissions into migration DB with asset-specific action
                self.move_assets_to_bundle(mig_conn, bundle_id, asset_ids, asset_type, asset_action_on_duplicate)
                self.set_user_permissions(mig_conn, bundle_id, domain_id, asset_type, asset_action_on_duplicate)
                self.set_group_permissions(mig_conn, bundle_id, domain_id, asset_type, asset_action_on_duplicate)

                logger.info(f"Domain {domain_id} migrated with {len(asset_ids)} {asset_type} assets (action: {asset_action_on_duplicate})")
                return True
        except Exception as e:
            logger.error(f"Migration failed for domain {domain_id}, asset type {asset_type}: {e}")
            return False

    def migrate_domain(self, domain_config: Dict[str, Any]) -> bool:
        """
        Migrate a domain with one or more asset types.

        Args:
            domain_config: Domain configuration dictionary

        Returns:
            True if all asset types migrated successfully, False otherwise
        """
        domain_id = domain_config["domain_id"]
        owner_id = domain_config["owner_id"]
        owner_type = domain_config["owner_type"]

        # Get list of asset types to migrate (supports both single and multiple)
        asset_types = self.get_asset_types_from_config(domain_config)

        logger.info(f"Starting migration for domain {domain_id} with asset types: {asset_types}")

        success_count = 0
        for asset_type in asset_types:
            logger.info(f"Migrating {asset_type} assets for domain {domain_id}")
            if self.migrate_single_asset_type(domain_id, owner_id, owner_type, asset_type):
                success_count += 1
            else:
                logger.error(f"Failed to migrate {asset_type} assets for domain {domain_id}")

        total_types = len(asset_types)
        success = success_count == total_types

        if success:
            logger.info(f"Domain {domain_id} migration completed successfully: {success_count}/{total_types} asset types")
        else:
            logger.error(f"Domain {domain_id} migration partially failed: {success_count}/{total_types} asset types successful")

        return success

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