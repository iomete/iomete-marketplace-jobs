"""Configuration management for compute onboarding migration job."""

import os
import logging
from typing import Dict, Any
from pyhocon import ConfigFactory

logger = logging.getLogger(__name__)


def get_config(config_path: str = None) -> Dict[str, Any]:
    # Check MIGRATION_TYPE env var for config file selection
    env_migration_type = os.environ.get("MIGRATION_TYPE", "").lower()

    if config_path is None:
        if env_migration_type == "namespace":
            config_path = "/etc/configs/application-namespace.conf"
            if not os.path.exists(config_path):
                config_path = "application-namespace.conf"
        else:
            config_path = "/etc/configs/application.conf"
            if not os.path.exists(config_path):
                config_path = "application.conf"

    logger.info(f"Loading configuration from: {config_path}")
    config = ConfigFactory.parse_file(config_path)

    config_migration_type = config.get("migration", {}).get("migration_type", "asset")

    if env_migration_type and env_migration_type != config_migration_type:
        raise ValueError(
            f"Migration type mismatch: MIGRATION_TYPE env var is '{env_migration_type}' "
            f"but config file specifies migration_type='{config_migration_type}'. "
            f"Please ensure the environment variable and config file are consistent."
        )

    logger.info(f"Migration type: {config_migration_type}")

    if "databases" not in config:
        config["databases"] = {"bundle_db": config.get("database")}

    # Override database config from env (for asset migration using bundle_db)
    if config_migration_type == "asset" and "bundle_db" in config.get("databases", {}):
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
                config["databases"]["bundle_db"][key] = (
                    int(os.environ[env]) if key == "port" else os.environ[env]
                )

    return config
