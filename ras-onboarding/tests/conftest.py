"""Shared fixtures and configuration for tests."""

import pytest
import json
from unittest.mock import Mock, MagicMock
from contextlib import contextmanager


@pytest.fixture
def sample_domain_id():
    """Sample domain ID for tests."""
    return "domain-test-123"


@pytest.fixture
def sample_namespace():
    """Sample namespace for tests."""
    return "default"


@pytest.fixture
def sample_user_id():
    """Sample user ID for tests."""
    return "user-test-123"


@pytest.fixture
def sample_bundle_id():
    """Sample bundle ID for tests."""
    return "bundle-test-123"


@pytest.fixture
def sample_namespace_mapping_id():
    """Sample namespace mapping ID for tests."""
    return "mapping-test-123"


@pytest.fixture
def mock_database_connection():
    """Create a mock database connection."""
    connection = MagicMock()
    connection.cursor.return_value.__enter__ = Mock(return_value=MagicMock())
    connection.cursor.return_value.__exit__ = Mock(return_value=False)
    connection.commit = Mock()
    connection.rollback = Mock()
    connection.close = Mock()
    return connection


@pytest.fixture
def mock_database_manager():
    """Create a mock DatabaseManager."""
    db = Mock()
    db.execute_query = Mock(return_value=[])
    db.execute_insert = Mock()
    db.test_connection = Mock(return_value=True)

    @contextmanager
    def get_connection():
        connection = mock_database_connection()
        yield connection

    @contextmanager
    def get_transaction():
        connection = mock_database_connection()
        try:
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    db.get_connection = get_connection
    db.get_transaction = get_transaction

    return db


@pytest.fixture
def full_migration_config():
    """Complete migration configuration for tests."""
    return {
        "migration": {
            "debug_mode": False,
            "dry_run": False,
            "duplicate_bundle_action": "FAIL",
            "namespace_permissions": ["READ", "WRITE"],
            "domains": ["domain-123"],
            "resource_tables": [
                {
                    "table": "lakehouse",
                    "namespace_column": "lakehouse_namespace",
                    "user_columns": ["created_by", "updated_by", "owner"]
                },
                {
                    "table": "spark_job",
                    "namespace_column": "namespace",
                    "user_columns": ["created_by", "owner"]
                },
                {
                    "table": "spark_connect",
                    "namespace_column": "namespace",
                    "user_columns": ["created_by", "updated_by"]
                }
            ]
        },
        "namespace_config": {
            "table": "lakehouse_namespace",
            "id_column": "namespace_id",
            "namespace_column": "name",
            "domain_column": "domain"
        }
    }


@pytest.fixture
def sample_domain_owners():
    """Sample domain owners as JSON string."""
    return json.dumps(["user-owner-1", "user-owner-2"])


@pytest.fixture
def sample_namespace_records():
    """Sample namespace records from database."""
    return [
        {"id": "ns-1", "namespace": "default", "domain_id": "domain-123"},
        {"id": "ns-2", "namespace": "dev", "domain_id": "domain-123"},
        {"id": "ns-3", "namespace": "prod", "domain_id": "domain-123"}
    ]


@pytest.fixture
def sample_user_records():
    """Sample user records from resource tables."""
    return [
        {"username": "user1"},
        {"username": "user2"},
        {"username": "user3"},
        {"username": "user1"},  # Duplicate
    ]


@pytest.fixture
def sample_bundle_permission_params():
    """Sample parameters for bundle permission insertion."""
    return {
        "bundle_id": "bundle-123",
        "asset_type": "NAMESPACE",
        "asset_id": "namespace-456",
        "actor_type": "USER",
        "actor_id": "user-789",
        "permissions": ["READ", "WRITE"]
    }

