import copy
from typing import Dict, Any, List

from ras_onboarding.common import DatabaseManager, get_logger

logger = get_logger(__name__)


def get_asset_types_from_domain(domain_config: Dict[str, Any]) -> List[str]:
    """Extract asset types from domain configuration."""
    if 'asset_types' in domain_config:
        asset_types = domain_config['asset_types']
        return asset_types
    else:
        raise Exception("No asset types found in domain configuration")


def _segregate_asset_types(asset_types: List[str]) -> tuple[bool, bool, List[str]]:
    """
    Segregate asset types into namespace, domain, and other types.

    Returns:
        Tuple of (has_namespace, has_domain, other_asset_types)
    """
    has_namespace = "NAMESPACE" in asset_types
    has_domain = "DOMAIN" in asset_types
    other_types = [t for t in asset_types if t not in ["NAMESPACE", "DOMAIN"]]
    return has_namespace, has_domain, other_types


def get_namespace_domain_and_asset_configs(config: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """
    Build separate configs for namespace, domain, and asset migrations.

    Args:
        config: Original configuration

    Returns:
        Tuple of (namespace_config, domain_config, asset_config)
    """
    migration_config = config.get("migration", {})
    domains = migration_config.get("domains", [])

    namespace_domains = []
    domain_domains = []
    asset_domains = []

    for domain_config in domains:
        asset_types = get_asset_types_from_domain(domain_config)
        has_namespace, has_domain, other_asset_types = _segregate_asset_types(asset_types)

        # Add to namespace domains if NAMESPACE is in asset_types
        if has_namespace:
            namespace_domains.append(domain_config)

        # Add to domain domains if DOMAIN is in asset_types
        if has_domain:
            domain_domains.append(domain_config)

        # Add to asset domains with filtered asset_types (excluding NAMESPACE and DOMAIN)
        if other_asset_types:
            asset_domain_config = {**domain_config, "asset_types": other_asset_types}
            asset_domains.append(asset_domain_config)

    # Build configs
    namespace_config = copy.deepcopy(config)
    namespace_config["migration"]["domains"] = namespace_domains

    domain_config = copy.deepcopy(config)
    domain_config["migration"]["domains"] = domain_domains

    asset_config = copy.deepcopy(config)
    asset_config["migration"]["domains"] = asset_domains

    return namespace_config, domain_config, asset_config


# Keep backward compatibility with old function name
def get_namespace_and_asset_configs(config: Dict[str, Any]) -> tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Deprecated: Use get_namespace_domain_and_asset_configs() instead.

    Build separate configs for namespace and asset migrations.

    Args:
        config: Original configuration

    Returns:
        Tuple of (namespace_config, asset_config)
    """
    namespace_config, _, asset_config = get_namespace_domain_and_asset_configs(config)
    return namespace_config, asset_config


def check_db_conn(conn: DatabaseManager):
    name = conn.db_config.get("name")
    if not conn.test_connection():
        raise Exception(f"Failed to connect to {name}")

    logger.info(f"{name} DB connection successful")


def get_db_conn(config: Dict[str, Any], debug_mode) -> DatabaseManager:
    conn = DatabaseManager(config, debug_mode)
    check_db_conn(conn)
    return conn
