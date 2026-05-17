package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

@ApplicationScoped
class CleanupUntrackedTableFoldersService {
    private val logger = Logger.getLogger(CleanupUntrackedTableFoldersService::class.java)

    @Inject
    lateinit var config: ApplicationConfig

    fun run() {
        logger.info("Loaded cleanup config: $config")

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

        logger.info(
            "Skeleton mode only. No REST Catalog lookup, ECS listing, or deletion is implemented yet."
        )
    }
}
