"""Main module for asset onboarding migration job."""

from common.logger import init_logger, get_logger
from common.database import DatabaseManager
from asset.migration import AssetOnboardingMigration

logger = get_logger(__name__)


def start_job(spark, config):
    debug_mode = config.get("migration", {}).get("debug_mode", False)
    init_logger(debug_mode)
    logger.info("Starting Asset Onboarding Migration Job")

    try:
        # Initialize migration DB
        bundle_migration_db_conf = config["databases"]["bundle_db"]
        bundle_migration_db = DatabaseManager(bundle_migration_db_conf, debug_mode)
        if not bundle_migration_db.test_connection():
            raise Exception("Failed to connect to migration database")
        logger.info("Migration DB connection successful")

        # Initialize single asset DB
        asset_db_conf = config["databases"]["asset_db"]
        asset_db = DatabaseManager(asset_db_conf, debug_mode)
        if not asset_db.test_connection():
            raise Exception("Failed to connect to asset database")
        logger.info("Asset DB connection successful")

        # Run migration
        migration = AssetOnboardingMigration(bundle_migration_db, asset_db, config)
        success = migration.run_migration()

        if success:
            logger.info("Migration completed successfully")
        else:
            raise Exception("Migration failed")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
