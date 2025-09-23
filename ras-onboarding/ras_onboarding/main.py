"""Main module for asset onboarding migration job."""

from .logger import init_logger, get_logger
from .database import DatabaseManager
from .migration import AssetOnboardingMigration

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

        # Initialize asset DBs
        asset_dbs = {}
        for asset_type, db_conf in config["databases"].get("assets", {}).items():
            dbm = DatabaseManager(db_conf, debug_mode)
            if not dbm.test_connection():
                raise Exception(f"Failed to connect to asset DB for {asset_type}")
            asset_dbs[asset_type] = dbm
        logger.info("Asset DB connections successful")

        # Run migration
        migration = AssetOnboardingMigration(bundle_migration_db, asset_dbs, config)
        success = migration.run_migration()

        if success:
            logger.info("Migration completed successfully")
        else:
            raise Exception("Migration failed")

    except Exception as e:
        logger.error(f"Job failed: {e}")
        raise
