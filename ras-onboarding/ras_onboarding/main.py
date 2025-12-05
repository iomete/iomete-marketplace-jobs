"""Main module for asset onboarding migration job."""
from ras_onboarding.common.logger import init_logger, get_logger
from ras_onboarding.common.database import DatabaseManager
from ras_onboarding.asset.migration import AssetOnboardingMigration
from ras_onboarding.namespace.migration import NamespaceMigration

logger = get_logger(__name__)


def _validate_database_config(config: dict, migration_type: str) -> None:
    """Validate that the config has the expected database keys for the migration type."""
    databases = config.get("databases", {})

    if migration_type == "namespace":
        required_keys = ["iam_db", "core_db"]
        wrong_keys = ["bundle_db", "asset_db"]
    else:
        required_keys = ["bundle_db", "asset_db"]
        wrong_keys = ["iam_db", "core_db"]

    # Check for missing required keys
    missing = [k for k in required_keys if k not in databases]
    if missing:
        raise ValueError(
            f"Missing required database config for {migration_type} migration: {missing}. "
            f"Expected keys: {required_keys}"
        )

    # Warn if wrong keys are present (might indicate wrong config file)
    present_wrong = [k for k in wrong_keys if k in databases]
    if present_wrong and not all(k in databases for k in required_keys):
        raise ValueError(
            f"Config file appears to be for a different migration type. "
            f"Found {present_wrong} but migration_type is '{migration_type}'. "
            f"Expected database keys: {required_keys}"
        )


def start_job(spark, config):
    debug_mode = config.get("migration", {}).get("debug_mode", False)
    migration_type = config.get("migration", {}).get("migration_type", "asset")

    init_logger(debug_mode)
    logger.info(f"Starting RAS Onboarding Migration Job - Type: {migration_type}")

    try:
        # Validate database configuration matches migration type
        _validate_database_config(config, migration_type)

        if migration_type == "namespace":
            # Namespace migration uses iam_db and core_db
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

            logger.info("Running namespace migration (resource-based permissions)")
            migration = NamespaceMigration(iam_db, core_db, config)
        else:
            # Asset migration uses bundle_db and asset_db
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
