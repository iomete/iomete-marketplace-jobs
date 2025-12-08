"""Configuration management for compute onboarding migration job."""

import os
import logging
from typing import Dict, Any
from pyhocon import ConfigFactory

logger = logging.getLogger(__name__)

def _validate_database_config(config: dict) -> None:
    """Validate that the config has the required database keys."""
    databases = config.get("databases", {})
    required_keys = ["iam_db", "core_db"]

    missing = [k for k in required_keys if k not in databases]
    if missing:
        raise ValueError(
            f"Missing required database config: {missing}. "
            f"Expected keys: {required_keys}"
        )


def get_config(config_path: str = None) -> Dict[str, Any]:
    if config_path is None:
        config_path = "/etc/configs/application.conf"
        if not os.path.exists(config_path):
            config_path = "application.conf"

    logger.info(f"Loading configuration from: {config_path}")
    config = ConfigFactory.parse_file(config_path)

    domains = config.get("migration", {}).get("domains", [])
    for domain in domains:
        domain_id = domain.get("domain_id", "unknown")
        asset_types = domain.get("asset_types", [])
        logger.info(f"Domain '{domain_id}' configured with asset_types: {asset_types}")

    if "databases" not in config:
        raise ValueError("Missing 'databases' section in configuration")

    # Override database config from env
    env_map = {
        "DB_HOST": "host",
        "DB_PORT": "port",
        "DB_NAME": "name",
        "DB_USER": "user",
        "DB_PASSWORD": "password",
        "DB_SSL_MODE": "ssl_mode",
    }
    for env, key in env_map.items():
        if env in os.environ:
            config["databases"]["iam_db"][key] = (
                int(os.environ[env]) if key == "port" else os.environ[env]
            )

    _validate_database_config(config)
    return config
