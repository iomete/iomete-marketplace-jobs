"""Configuration management for orphaned asset detection job."""

import os
import logging
from dataclasses import dataclass
from pyhocon import ConfigFactory

logger = logging.getLogger(__name__)


@dataclass
class DatabaseConfig:
    host: str
    port: int
    name: str
    user: str
    password: str
    ssl_mode: str = "require"


@dataclass
class ApplicationConfig:
    global_db: DatabaseConfig
    debug_mode: bool = False


def _parse_db_config(config, db_key: str) -> DatabaseConfig:
    db = config["databases"][db_key]
    return DatabaseConfig(
        host=db["host"],
        port=int(db["port"]),
        name=db["name"],
        user=db["user"],
        password=db["password"],
        ssl_mode=db.get("ssl_mode", "require"),
    )


def get_config(config_path: str = None) -> ApplicationConfig:
    if config_path is None:
        config_path = "/etc/configs/application.conf"
        if not os.path.exists(config_path):
            config_path = "application.conf"

    logger.info(f"Loading configuration from: {config_path}")
    config = ConfigFactory.parse_file(config_path)

    if "databases" not in config:
        raise ValueError("Missing 'databases' section in configuration")

    global_db = _parse_db_config(config, "global_db")
    debug_mode = config.get("job", {}).get("debug_mode", False)

    return ApplicationConfig(global_db=global_db, debug_mode=bool(debug_mode))
