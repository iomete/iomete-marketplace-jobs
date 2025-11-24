"""Configuration management for namespace onboarding migration job."""

import os
from typing import Dict, Any
from pyhocon import ConfigFactory


def get_config(config_path: str = None) -> Dict[str, Any]:
    """
    Load configuration from file and override with environment variables.

    Args:
        config_path: Path to configuration file

    Returns:
        Configuration dictionary
    """
    if config_path is None:
        config_path = "/etc/configs/application.conf"

    if not os.path.exists(config_path):
        config_path = "application.conf"

    config = ConfigFactory.parse_file(config_path)

    # Override bundle_db from env
    bundle_env_map = {
        "DB_HOST": "host",
        "DB_PORT": "port",
        "DB_NAME": "name",
        "DB_USER": "user",
        "DB_PASSWORD": "password",
        "DB_SSL_MODE": "ssl_mode",
    }
    for env, key in bundle_env_map.items():
        if env in os.environ:
            config["databases"]["bundle_db"][key] = (
                int(os.environ[env]) if key == "port" else os.environ[env]
            )

    # Override asset_db from env
    asset_env_map = {
        "ASSET_DB_HOST": "host",
        "ASSET_DB_PORT": "port",
        "ASSET_DB_NAME": "name",
        "ASSET_DB_USER": "user",
        "ASSET_DB_PASSWORD": "password",
        "ASSET_DB_SSL_MODE": "ssl_mode",
    }
    for env, key in asset_env_map.items():
        if env in os.environ:
            config["databases"]["asset_db"][key] = (
                int(os.environ[env]) if key == "port" else os.environ[env]
            )

    return config
