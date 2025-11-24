GET_NAMESPACE_MAPPING_ID = """SELECT id
                              FROM domain_namespace_mapping
                              WHERE domain_id = %s
                                AND namespace = %s"""

GET_BUNDLE_FROM_DOMAIN_AND_NAME = """
                                  SELECT id
                                  FROM bundle
                                  WHERE domain = %s
                                    AND name = %s \
                                  """

SET_NAMESPACE_PERMISSION = """
            INSERT INTO bundle_permission
            (bundle_id, asset_type, asset_id, actor_type, actor_id, permissions, created_at, created_by, updated_at, updated_by)
            VALUES (%s, 'NAMESPACE', %s, 'USER', %s, %s, current_timestamp, 'system', current_timestamp, 'system')
            ON CONFLICT (bundle_id, asset_type, asset_id, actor_type, actor_id)
            DO UPDATE SET
                permissions = EXCLUDED.permissions,
                updated_at = current_timestamp,
                updated_by = 'system'
        """


GET_NAMESPACES_FOR_DOMAIN = """
            SELECT {namespace_config['id_column']} as id,
                   {namespace_config['namespace_column']} as namespace,
                   {namespace_config['domain_column']} as domain_id
            FROM {namespace_config['table']}
            WHERE {namespace_config['domain_column']} = %s
        """

CREATE_NAMESPACE_BUNDLE = """
            INSERT INTO bundle (id, name, description, owner_id, owner_type, domain,
                               created_at, created_by, updated_at, updated_by, is_archived)
            VALUES (%s, %s, %s, %s, %s, %s, current_timestamp, 'system', current_timestamp, 'system', false)
        """

GET_DOMAIN_OWNERS = """
            SELECT owners
            FROM domain
            WHERE id = %s AND is_deleted = false
        """
