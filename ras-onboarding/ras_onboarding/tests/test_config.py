"""Tests for configuration module."""

import os
import tempfile
import pytest

from ras_onboarding.common.config import get_config

def test_get_config_with_file():
    """Test configuration loading from file."""
    config_content = '''
    {
        database: {
            host: "test_host"
            port: 5432
            name: "test_db"
        }
        migration: {
            domains: [
                {
                    domain_id: "test_domain"
                    owner_id: "test_owner"
                    owner_type: "USER"
                    asset_type: "COMPUTE"
                }
            ]
        }
    }
    '''

    with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
        f.write(config_content)
        f.flush()

        try:
            config = get_config(f.name)
            assert config['database']['host'] == "test_host"
            assert config['database']['port'] == 5432
            assert len(config['migration']['domains']) == 1
            assert config['migration']['domains'][0]['domain_id'] == "test_domain"
        finally:
            os.unlink(f.name)


def test_get_config_with_env_override():
    """Test configuration override from environment variables."""
    config_content = '''
    {
        database: {
            host: "original_host"
            port: 5432
        }
    }
    '''

    os.environ['DB_HOST'] = 'env_host'
    os.environ['DB_PORT'] = '3306'

    try:
        with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
            f.write(config_content)
            f.flush()

            try:
                config = get_config(f.name)
                assert config['database']['host'] == "env_host"
                assert config['database']['port'] == 3306
            finally:
                os.unlink(f.name)
    finally:
        # Clean up environment
        if 'DB_HOST' in os.environ:
            del os.environ['DB_HOST']
        if 'DB_PORT' in os.environ:
            del os.environ['DB_PORT']