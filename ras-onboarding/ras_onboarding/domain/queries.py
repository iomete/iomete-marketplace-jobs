"""SQL query constants for domain migration."""

# Check if DOMAIN bundle already exists
GET_DOMAIN_BUNDLE_BY_DOMAIN_ID = """
    SELECT id, owner_id, owner_type, bundle_type
    FROM bundle
    WHERE name = %s AND bundle_type = 'DOMAIN' AND is_archived = false
"""

# Create DOMAIN bundle with bundle_type and NULL domain
CREATE_DOMAIN_BUNDLE = """
    INSERT INTO bundle (
        id, name, description, owner_id, owner_type, domain,
        bundle_type, parent_bundle_id,
        created_at, created_by, updated_at, updated_by, is_archived
    )
    VALUES (
        %s, %s, %s, %s, %s, %s,
        'DOMAIN', NULL,
        current_timestamp, %s, current_timestamp, %s, false
    )
    RETURNING id
"""

# Add DOMAIN asset to bundle
ADD_DOMAIN_ASSET_TO_BUNDLE = """
    INSERT INTO bundle_asset (bundle_id, asset_type, asset_id, created_at, created_by, updated_at, updated_by)
    VALUES (%s, 'DOMAIN', %s, current_timestamp, 'system', current_timestamp, 'system')
    ON CONFLICT (bundle_id, asset_type, asset_id) DO NOTHING
"""

# Complex permission insertion with CTEs for role-based permission mapping
INSERT_DOMAIN_PERMISSIONS = """
    WITH
    -- 1. Define Permission Mappings
    permission_rules(service, action, mapped_permission) AS (
        VALUES
            ('lakehouse', 'create', 'CREATE_COMPUTE'),
            ('spark_job', 'create', 'CREATE_SPARK_JOB'),
            ('marketplace', 'manage', 'MANAGE_MARKETPLACE'),
            ('data_product', 'view', 'VIEW_DATA_PRODUCT'),
            ('data_product', 'manage', 'MANAGE_DATA_PRODUCT'),
            ('data_catalog', 'view', 'VIEW_DATA_CATALOG'),
            ('data_catalog', 'manage', 'MANAGE_DATA_CATALOG'),
            ('spark_settings', 'view', 'VIEW_SPARK_SETTINGS'),
            ('spark_settings', 'manage', 'MANAGE_SPARK_SETTINGS'),
            ('secrets', 'list', 'LIST_SECRETS'),
            ('secrets', 'view', 'VIEW_SECRETS'),
            ('secrets', 'manage', 'MANAGE_SECRETS'),
            ('shared_worksheet', 'list', 'LIST_SHARED_WORKSHEET'),
            ('shared_worksheet', 'manage', 'MANAGE_SHARED_WORKSHEET'),
            ('git_repository', 'manage', 'MANAGE_GIT_REPO'),
            ('sql_editor', 'export', 'EXPORT_SQL_EDITOR'),
            ('access_token', 'manage', 'MANAGE_ACCESS_TOKEN')
    ),
    -- 2. Flatten IAM Roles
    role_permissions_flat AS (
        SELECT
            r.name AS role_name,
            r.domain,
            s->>'service' AS service,
            a->>'action' AS action
        FROM iam_role r,
             jsonb_array_elements(r.permissions::jsonb) s,
             jsonb_array_elements(s->'actions') a
        WHERE r.domain = %s
    ),
    -- 3. Map Permissions
    role_permissions_map AS (
        SELECT r.role_name, r.domain, pr.mapped_permission
        FROM role_permissions_flat r
        JOIN permission_rules pr ON r.service = pr.service AND r.action = pr.action
    ),
    -- 4. Determine all roles for every member (Explicit + Default)
    effective_member_roles AS (
        -- USERS: Explicit Roles
        SELECT dm.identity_id AS actor_id, 'USER' AS actor_type, urm.role_name
        FROM domain_member dm
        JOIN user_role_mapping_v2 urm ON dm.identity_id = urm.username AND dm.domain_id = urm.domain
        WHERE dm.domain_id = %s AND dm.identity_type = 'USER'
        UNION ALL
        -- USERS: Default Role
        SELECT dm.identity_id AS actor_id, 'USER' AS actor_type, 'default' AS role_name
        FROM domain_member dm
        WHERE dm.domain_id = %s AND dm.identity_type = 'USER'
        UNION ALL
        -- GROUPS: Explicit Roles
        SELECT dm.identity_id AS actor_id, 'GROUP' AS actor_type, grm.role_name
        FROM domain_member dm
        JOIN group_role_mapping_v2 grm ON dm.identity_id = grm.group_name AND dm.domain_id = grm.domain
        WHERE dm.domain_id = %s AND dm.identity_type = 'GROUP'
        UNION ALL
        -- GROUPS: Default Role
        SELECT dm.identity_id AS actor_id, 'GROUP' AS actor_type, 'default' AS role_name
        FROM domain_member dm
        WHERE dm.domain_id = %s AND dm.identity_type = 'GROUP'
    )
    -- 5. Aggregate and Insert
    INSERT INTO bundle_permission (bundle_id, asset_type, actor_type, actor_id, permissions, created_at, created_by, updated_at, updated_by)
    SELECT
        %s AS bundle_id,
        'DOMAIN' AS asset_type,
        emr.actor_type,
        emr.actor_id,
        ARRAY_AGG(DISTINCT rpm.mapped_permission) AS permissions,
        current_timestamp, 'system', current_timestamp, 'system'
    FROM effective_member_roles emr
    JOIN role_permissions_map rpm ON emr.role_name = rpm.role_name
    GROUP BY emr.actor_type, emr.actor_id
    ON CONFLICT (bundle_id, asset_type, actor_type, actor_id) DO UPDATE SET
        permissions = EXCLUDED.permissions,
        updated_at = current_timestamp
"""

# Update parent_bundle_id for RESOURCE bundles in the domain
UPDATE_RESOURCE_BUNDLES_PARENT = """
    UPDATE bundle
    SET parent_bundle_id = %s, updated_at = current_timestamp, updated_by = 'system'
    WHERE domain = %s
      AND bundle_type = 'RESOURCE'
"""

# Check if RESOURCE bundles exist in domain
CHECK_RESOURCE_BUNDLES_EXIST = """
    SELECT COUNT(*) as count
    FROM bundle
    WHERE domain = %s AND bundle_type = 'RESOURCE'
"""

# Delete operations for OVERWRITE mode
DELETE_DOMAIN_BUNDLE = """
    DELETE FROM bundle
    WHERE id = %s AND bundle_type = 'DOMAIN'
"""

DELETE_BUNDLE_PERMISSIONS = """
    DELETE FROM bundle_permission
    WHERE bundle_id = %s
"""

DELETE_BUNDLE_ASSETS = """
    DELETE FROM bundle_asset
    WHERE bundle_id = %s
"""