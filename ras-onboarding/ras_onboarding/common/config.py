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


def _validate_overwrite_action_config(config: dict) -> None:
    """Validate that OVERWRITE action is only used with NAMESPACE asset type.

    When duplicate_bundle_action is OVERWRITE, all domains must have only
    NAMESPACE in their asset_types list.
    """
    migration_config = config.get("migration", {})
    duplicate_action = migration_config.get("duplicate_bundle_action", "FAIL")

    if duplicate_action != "OVERWRITE":
        return

    domains = migration_config.get("domains", [])
    invalid_domains = []

    for domain in domains:
        domain_id = domain.get("domain_id", "unknown")
        asset_types = domain.get("asset_types", [])

        # Check if asset_types contains only NAMESPACE
        if asset_types != ["NAMESPACE"]:
            invalid_domains.append({
                "domain_id": domain_id,
                "asset_types": asset_types
            })

    if invalid_domains:
        domain_details = ", ".join(
            f"'{d['domain_id']}' has asset_types={d['asset_types']}"
            for d in invalid_domains
        )
        raise ValueError(
            f"When duplicate_bundle_action is 'OVERWRITE', all domains must have "
            f"only ['NAMESPACE'] in asset_types. Invalid domains: {domain_details}"
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
    _validate_overwrite_action_config(config)
    return config
