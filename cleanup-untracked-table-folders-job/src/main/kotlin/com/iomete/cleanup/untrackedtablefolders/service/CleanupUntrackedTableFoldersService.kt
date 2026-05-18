package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

@ApplicationScoped
class CleanupUntrackedTableFoldersService {
    private val logger = Logger.getLogger(CleanupUntrackedTableFoldersService::class.java)

    @Inject
    lateinit var config: ApplicationConfig

    @Inject
    lateinit var catalogDiscoveryService: CatalogDiscoveryService

    fun run() {
        logger.info("Loaded cleanup config: $config")

        validateConfig()

        config.databases.forEach { database ->
            val tables =
                catalogDiscoveryService.discoverTables(
                    catalog = config.catalog,
                    database = database,
                )

            logger.info(
                "Discovered ${tables.size} active table(s) for catalog=${config.catalog}, database=$database"
            )

            tables.forEach { table ->
                logger.info(
                    "Active table discovered: catalog=${table.catalog}, database=${table.database}, table=${table.table}, isTemporary=${table.isTemporary}"
                )
            }
        }

        logger.info(
            "Read-only catalog discovery completed. No ECS listing, comparison, or deletion was performed."
        )
    }

    private fun validateConfig() {
        require(config.catalog.isNotBlank()) {
            "catalog must not be blank"
        }

        require(config.databases.isNotEmpty()) {
            "databases must contain at least one database name"
        }

        if (!config.dryRun && !config.deleteEnabled) {
            throw IllegalArgumentException(
                "delete_enabled must be true when dry_run is false. Refusing to run destructive mode."
            )
        }
    }
}
