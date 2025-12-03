"""Main module for asset onboarding migration job."""
from ras_onboarding.common.logger import init_logger, get_logger
from ras_onboarding.common.database import DatabaseManager
from ras_onboarding.asset.migration import AssetOnboardingMigration
from ras_onboarding.namespace.migration import NamespaceMigration

logger = get_logger(__name__)


def start_job(spark, config):
    debug_mode = config.get("migration", {}).get("debug_mode", False)
    migration_type = config.get("migration", {}).get("migration_type", "asset")

    init_logger(debug_mode)
    logger.info(f"Starting RAS Onboarding Migration Job - Type: {migration_type}")

    try:
        bundle_db_conf = config["databases"]["bundle_db"]
        bundle_db = DatabaseManager(bundle_db_conf, debug_mode)
        if not bundle_db.test_connection():
            raise Exception("Failed to connect to bundle database")
        logger.info("Bundle DB connection successful")

        asset_db_conf = config["databases"]["asset_db"]
        asset_db = DatabaseManager(asset_db_conf, debug_mode)
        if not asset_db.test_connection():
            raise Exception("Failed to connect to asset database")
        logger.info("Asset DB connection successful")

        if migration_type == "namespace":
            logger.info("Running namespace migration (resource-based permissions)")
            migration = NamespaceMigration(bundle_db, asset_db, bundle_db, config)
        else:
            logger.info("Running asset migration (role-based permissions)")
            migration = AssetOnboardingMigration(bundle_db, asset_db, config)

        success = migration.run_migration()

        if success:
            logger.info("Migration completed successfully")
        else:
            raise Exception("Migration failed")

    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        raise
