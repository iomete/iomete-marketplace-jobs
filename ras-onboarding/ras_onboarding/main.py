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
        # Both migration types use iam_db and core_db
        iam_db_conf = config["databases"]["iam_db"]
        iam_db = DatabaseManager(iam_db_conf, debug_mode)
        if not iam_db.test_connection():
            raise Exception("Failed to connect to IAM database")
        logger.info("IAM DB connection successful")

        core_db_conf = config["databases"]["core_db"]
        core_db = DatabaseManager(core_db_conf, debug_mode)
        if not core_db.test_connection():
            raise Exception("Failed to connect to core database")
        logger.info("Core DB connection successful")

        if migration_type == "namespace":
            logger.info("Running namespace migration (all domain users get permissions)")
            migration = NamespaceMigration(iam_db, core_db, config)
        else:
            logger.info("Running asset migration (role-based permissions)")
            migration = AssetOnboardingMigration(iam_db, core_db, config)

        success = migration.run_migration()

        if success:
            logger.info("Migration completed successfully")
        else:
            raise Exception("Migration failed")

    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        raise
