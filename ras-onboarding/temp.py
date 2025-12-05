#!/usr/bin/env python3
"""Test script for namespace migration on local dev setup."""

import os
import sys

# Set database credentials from environment or use defaults
os.environ.setdefault('DB_USER', 'postgres')
os.environ.setdefault('DB_PASSWORD', 'postgresql_root_pass')
os.environ.setdefault('ASSET_DB_USER', 'postgres')
os.environ.setdefault('ASSET_DB_PASSWORD', 'postgresql_root_pass')
os.environ.setdefault('MIGRATION_TYPE', 'namespace')

from ras_onboarding.common.config import get_config
from ras_onboarding.common.logger import init_logger, get_logger
from ras_onboarding.common.database import DatabaseManager
from ras_onboarding.namespace.migration import NamespaceMigration

def test_migration():
    """Run namespace migration test."""

    # Load configuration
    config_path = "/Users/ujjawalkhare/Desktop/iomete-marketplace-jobs/ras-onboarding/application-namespace.conf"
    print(f"Loading configuration from: {config_path}")
    config = get_config(config_path)

    # Initialize logger
    debug_mode = config.get("migration", {}).get("debug_mode", False)
    dry_run = config.get("migration", {}).get("dry_run", False)
    migration_type = config.get("migration", {}).get("migration_type", "asset")
    init_logger(debug_mode)

    logger = get_logger(__name__)
    logger.info("=" * 80)
    logger.info("Starting Namespace Migration Test")
    logger.info(f"Migration Type: {migration_type}")
    logger.info("=" * 80)

    if dry_run:
        logger.warning("DRY RUN MODE ENABLED - No changes will be committed to database")

    try:
        # Initialize IAM DB (bundle/permission database)
        logger.info("Connecting to IAM DB...")
        iam_db_conf = config["databases"]["iam_db"]
        logger.info(f"  Host: {iam_db_conf['host']}:{iam_db_conf['port']}")
        logger.info(f"  Database: {iam_db_conf['name']}")
        logger.info(f"  User: {iam_db_conf['user']}")

        iam_db = DatabaseManager(iam_db_conf, debug_mode)
        if not iam_db.test_connection():
            raise Exception("Failed to connect to IAM database")
        logger.info("IAM DB connection successful")

        # Initialize Core DB (resource database)
        logger.info("\nConnecting to Core DB...")
        core_db_conf = config["databases"]["core_db"]
        logger.info(f"  Host: {core_db_conf['host']}:{core_db_conf['port']}")
        logger.info(f"  Database: {core_db_conf['name']}")
        logger.info(f"  User: {core_db_conf['user']}")

        core_db = DatabaseManager(core_db_conf, debug_mode)
        if not core_db.test_connection():
            raise Exception("Failed to connect to Core database")
        logger.info("Core DB connection successful")

        # Display migration configuration
        logger.info("\n" + "=" * 80)
        logger.info("Migration Configuration:")
        logger.info("=" * 80)
        logger.info(f"Domains: {config['migration']['domains']}")
        logger.info(f"Duplicate Bundle Action: {config['migration'].get('duplicate_bundle_action', 'FAIL')}")
        logger.info(f"Namespace Permissions: {config['migration'].get('namespace_permissions', [])}")
        resource_tables = config['migration'].get('resource_tables', [])
        logger.info(f"Resource Tables: {len(resource_tables)}")
        for table in resource_tables:
            logger.info(f"  - {table['table']} (namespace_column: {table['namespace_column']})")

        # Run migration
        logger.info("\n" + "=" * 80)
        logger.info("Starting Migration...")
        logger.info("=" * 80)

        migration = NamespaceMigration(iam_db, core_db, config)
        success = migration.run_migration()

        logger.info("\n" + "=" * 80)
        if success:
            logger.info("MIGRATION COMPLETED SUCCESSFULLY")
            if dry_run:
                logger.info("  (All changes were rolled back - DRY RUN mode)")
        else:
            logger.error("MIGRATION FAILED")
        logger.info("=" * 80)

        return 0 if success else 1

    except Exception as e:
        logger.error("\n" + "=" * 80)
        logger.error(f"Migration failed with error: {e}", exc_info=True)
        logger.error("=" * 80)
        return 1

if __name__ == "__main__":
    sys.exit(test_migration())
