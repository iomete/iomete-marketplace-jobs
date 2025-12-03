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
                               updated_by = 'system' \
                           """

DELETE_BUNDLE_PERMISSIONS = """
                            DELETE
                            FROM bundle_permission
                            WHERE bundle_id = %s \
                            """
