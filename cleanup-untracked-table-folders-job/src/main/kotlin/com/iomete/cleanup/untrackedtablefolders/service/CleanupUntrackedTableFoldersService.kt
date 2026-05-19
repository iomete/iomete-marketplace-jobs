package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
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

    @Inject
    lateinit var objectStorageDiscoveryService: ObjectStorageDiscoveryService

    fun run() {
        logger.info("Loaded cleanup config: $config")

        validateConfig()

        config.databases.forEach { database ->
            val discoveredDatabase =
                catalogDiscoveryService.discoverDatabase(
                    catalog = config.catalog,
                    database = database,
                )

            logger.info(
                "Discovered database: catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}, location=${discoveredDatabase.location}"
            )

            logger.info(
                "Discovered ${discoveredDatabase.tables.size} active table(s) for catalog=${config.catalog}, database=$database"
            )

            discoveredDatabase.tables.forEach { table ->
                logger.info(
                    "Active table discovered: catalog=${table.catalog}, database=${table.database}, table=${table.table}, isTemporary=${table.isTemporary}, location=${table.location}"
                )
            }

            if (discoveredDatabase.location.isNullOrBlank()) {
                logger.warn(
                    "Skipping storage folder discovery because database location is missing for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                )
            } else {
                val storageFolders =
                    objectStorageDiscoveryService.listImmediateChildFolders(
                        location = discoveredDatabase.location,
                    )

                logger.info(
                    "Discovered ${storageFolders.size} immediate storage folder(s) under database location=${discoveredDatabase.location}"
                )

                storageFolders.forEach { folder ->
                    logger.info("Storage folder discovered: $folder")
                }
            }
        }

        logger.info(
            "Read-only discovery completed. No comparison or deletion was performed."
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
