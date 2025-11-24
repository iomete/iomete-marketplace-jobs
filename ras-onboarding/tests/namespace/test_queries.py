"""Test cases for namespace queries."""

import pytest
from ras_onboarding.namespace import queries


class TestQueries:
    """Test SQL query definitions."""

    def test_get_namespace_mapping_id_query_structure(self):
        """Test GET_NAMESPACE_MAPPING_ID query has required elements."""
        query = queries.GET_NAMESPACE_MAPPING_ID
        assert "SELECT id" in query
        assert "FROM domain_namespace_mapping" in query
        assert "WHERE domain_id = %s" in query
        assert "AND namespace = %s" in query

    def test_get_bundle_from_domain_and_name_query_structure(self):
        """Test GET_BUNDLE_FROM_DOMAIN_AND_NAME query has required elements."""
        query = queries.GET_BUNDLE_FROM_DOMAIN_AND_NAME
        assert "SELECT id" in query
        assert "FROM bundle" in query
        assert "WHERE domain = %s" in query
        assert "AND name = %s" in query

    def test_set_namespace_permission_query_structure(self):
        """Test SET_NAMESPACE_PERMISSION query has upsert logic."""
        query = queries.SET_NAMESPACE_PERMISSION
        assert "INSERT INTO bundle_permission" in query
        assert "ON CONFLICT" in query
        assert "DO UPDATE SET" in query
        assert "bundle_id" in query
        assert "asset_type" in query
        assert "asset_id" in query
        assert "actor_type" in query
        assert "actor_id" in query
        assert "permissions" in query
        assert "'NAMESPACE'" in query
        assert "'USER'" in query

    def test_set_namespace_permission_has_correct_placeholders(self):
        """Test SET_NAMESPACE_PERMISSION has correct parameter placeholders."""
        query = queries.SET_NAMESPACE_PERMISSION
        # Count %s placeholders - should have 4 (bundle_id, namespace_id, username, permissions)
        placeholder_count = query.count("%s")
        assert placeholder_count == 4

    def test_get_namespaces_for_domain_query_template(self):
        """Test GET_NAMESPACES_FOR_DOMAIN is a template string."""
        query = queries.GET_NAMESPACES_FOR_DOMAIN
        assert "{namespace_config['id_column']}" in query
        assert "{namespace_config['namespace_column']}" in query
        assert "{namespace_config['domain_column']}" in query
        assert "{namespace_config['table']}" in query
        assert "WHERE" in query

    def test_get_namespaces_for_domain_formats_correctly(self):
        """Test GET_NAMESPACES_FOR_DOMAIN formats with config."""
        namespace_config = {
            'id_column': 'namespace_id',
            'namespace_column': 'name',
            'domain_column': 'domain',
            'table': 'lakehouse_namespace'
        }
        query = queries.GET_NAMESPACES_FOR_DOMAIN.format(namespace_config=namespace_config)

        assert "namespace_id as id" in query
        assert "name as namespace" in query
        assert "domain as domain_id" in query
        assert "FROM lakehouse_namespace" in query

    def test_create_namespace_bundle_query_structure(self):
        """Test CREATE_NAMESPACE_BUNDLE has all required fields."""
        query = queries.CREATE_NAMESPACE_BUNDLE
        assert "INSERT INTO bundle" in query
        assert "id" in query
        assert "name" in query
        assert "description" in query
        assert "owner_id" in query
        assert "owner_type" in query
        assert "domain" in query
        assert "is_archived" in query
        assert "false" in query

    def test_create_namespace_bundle_has_correct_placeholders(self):
        """Test CREATE_NAMESPACE_BUNDLE has correct parameter placeholders."""
        query = queries.CREATE_NAMESPACE_BUNDLE
        # Should have 6 placeholders: id, name, description, owner_id, owner_type, domain
        placeholder_count = query.count("%s")
        assert placeholder_count == 6

    def test_get_domain_owners_query_structure(self):
        """Test GET_DOMAIN_OWNERS query has required elements."""
        query = queries.GET_DOMAIN_OWNERS
        assert "SELECT owners" in query
        assert "FROM domain" in query
        assert "WHERE id = %s" in query
        assert "is_deleted = false" in query

    def test_get_domain_owners_has_correct_placeholders(self):
        """Test GET_DOMAIN_OWNERS has correct parameter placeholders."""
        query = queries.GET_DOMAIN_OWNERS
        placeholder_count = query.count("%s")
        assert placeholder_count == 1
