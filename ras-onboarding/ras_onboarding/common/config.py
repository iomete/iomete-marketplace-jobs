"""Configuration management for compute onboarding migration job."""

import os
from typing import Dict, Any
from pyhocon import ConfigFactory


def get_config(config_path: str = None) -> Dict[str, Any]:
    if config_path is None:
        config_path = "/etc/configs/application.conf"

    if not os.path.exists(config_path):
        config_path = "application.conf"

    config = ConfigFactory.parse_file(config_path)

    # Backward compat: single "database" -> treat as bundle_db
    if "databases" not in config:
        config["databases"] = {"bundle_db": config.get("database")}

    # Override bundle_db from env
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
