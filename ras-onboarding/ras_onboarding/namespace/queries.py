GET_NAMESPACE_MAPPING_ID = """SELECT id
                              FROM domain_namespace_mapping
                              WHERE domain_id = %s
                                AND namespace = %s LIMIT 1"""

GET_BUNDLE_FROM_DOMAIN_AND_NAME = """
                                  SELECT id
                                  FROM bundle
                                  WHERE domain = %s
                                    AND name = %s
                                  """

SET_NAMESPACE_PERMISSION = """
                           INSERT INTO bundle_permission
                           (bundle_id, asset_type, actor_type, actor_id, permissions, created_at, created_by,
                            updated_at, updated_by)
                           VALUES (%s, 'NAMESPACE', 'USER', %s, %s, current_timestamp, 'system', current_timestamp,
                                   'system') ON CONFLICT (bundle_id, asset_type, actor_type, actor_id)
            DO
                           UPDATE SET
                               permissions = EXCLUDED.permissions,
                               updated_at = current_timestamp,
                               updated_by = 'system'
                           """


GET_NAMESPACES_FOR_DOMAIN = """
            SELECT {id_column} as id,
                   {namespace_column} as namespace,
                   {domain_column} as domain_id
            FROM {table}
            WHERE {domain_column} = %s
        """

CREATE_NAMESPACE_BUNDLE = """
            INSERT INTO bundle (id, name, description, owner_id, owner_type, domain,
                               created_at, created_by, updated_at, updated_by, is_archived)
            VALUES (%s, %s, %s, %s, %s, %s, current_timestamp, 'system', current_timestamp, 'system', false)
        """

GET_ALL_DOMAIN_USERS = """
            SELECT DISTINCT dm.identity_id as username
            FROM domain_member dm
            JOIN iam_user u ON u.username = dm.identity_id
            WHERE dm.domain_id = %s
              AND dm.identity_type = 'USER'
              AND u.is_deleted = false
        """

CHECK_NAMESPACE_IN_ANY_BUNDLE = """
            SELECT b.id, b.name FROM bundle_asset ba
            JOIN bundle b ON ba.bundle_id = b.id
            WHERE ba.asset_type = 'NAMESPACE'
              AND ba.asset_id = %s
            LIMIT 1
        """

ADD_NAMESPACE_TO_BUNDLE = """
            INSERT INTO bundle_asset (bundle_id, asset_type, asset_id, created_at, created_by, updated_at, updated_by)
            VALUES (%s, 'NAMESPACE', %s, current_timestamp, 'system', current_timestamp, 'system')
            ON CONFLICT (bundle_id, asset_type, asset_id) DO NOTHING
        """

DELETE_NAMESPACE_BUNDLE = """
DELETE FROM bundle
    WHERE id = %s"""

DELETE_BUNDLE_PERMISSIONS = """
                            DELETE
                            FROM bundle_permission
                            WHERE bundle_id = %s
                            """

DELETE_BUNDLE_ASSETS = """
DELETE FROM bundle_asset
    WHERE bundle_id = %s
"""
