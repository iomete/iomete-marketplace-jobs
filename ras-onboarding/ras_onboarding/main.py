"""Main module for asset onboarding migration job."""
from ras_onboarding.common.logger import init_logger, get_logger
from ras_onboarding.asset.migration import AssetOnboardingMigration
from ras_onboarding.common.utils import get_namespace_domain_and_asset_configs, get_db_conn
from ras_onboarding.namespace.migration import NamespaceMigration
from ras_onboarding.domain.migration import DomainMigration

logger = get_logger(__name__)


def start_job(spark, config):
    debug_mode = config.get("migration", {}).get("debug_mode", False)

    init_logger(debug_mode)
    logger.info("Starting RAS Onboarding Migration Job")

    try:
        # Both migration types use iam_db and core_db
        iam_db_conf = config["databases"]["iam_db"]
        iam_db = get_db_conn(iam_db_conf, debug_mode)

        core_db_conf = config["databases"]["core_db"]
        core_db = get_db_conn(core_db_conf, debug_mode)

        # Build separate configs for namespace, domain, and asset migrations
        namespace_config, domain_config, asset_config = get_namespace_domain_and_asset_configs(config)

        namespace_domains = namespace_config["migration"]["domains"]
        domain_domains = domain_config["migration"]["domains"]
        asset_domains = asset_config["migration"]["domains"]

        overall_success = True

        # Run namespace migration if there are domains with NAMESPACE
        if namespace_domains:
            logger.info("Running namespace migration")
            namespace_migration = NamespaceMigration(iam_db, core_db, namespace_config)
            if not namespace_migration.run_migration():
                logger.error("Namespace migration failed")
                overall_success = False

        # Run domain migration if there are domains with DOMAIN
        if domain_domains:
            logger.info("Running domain migration")
            domain_migration = DomainMigration(iam_db, core_db, domain_config)
            if not domain_migration.run_migration():
                logger.error("Domain migration failed")
                overall_success = False

        # Run asset migration if there are domains with other asset types
        if asset_domains:
            logger.info("Running asset migration")
            asset_migration = AssetOnboardingMigration(iam_db, core_db, asset_config)
            if not asset_migration.run_migration():
                logger.error("Asset migration failed")
                overall_success = False

        if overall_success:
            logger.info("Migration completed successfully")
        else:
            raise Exception("Migration failed")
    except Exception as e:
        logger.error(f"Job failed: {e}", exc_info=True)
        raise
