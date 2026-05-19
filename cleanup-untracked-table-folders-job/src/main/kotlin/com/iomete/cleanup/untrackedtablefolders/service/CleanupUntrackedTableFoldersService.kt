package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Duration
import java.time.Instant
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
                    logger.info(
                        "Storage folder discovered: path=${folder.path}, modifiedAt=${Instant.ofEpochMilli(folder.modificationTimeMillis)}"
                    )
                }

                val activeTableLocationSet = discoveredDatabase.tables
                    .mapNotNull { it.location }
                    .map { normalizePath(it) }
                    .toSet()

                val excludedPathSet = config.excludePaths
                    .map { normalizePath(it) }
                    .toSet()

                val cutoffTime = Instant.now().minus(Duration.ofHours(config.olderThanHours))
                val cutoffTimeMillis = cutoffTime.toEpochMilli()

                logger.info(
                    "Applying older_than_hours=${config.olderThanHours}; candidate folders must have modification time at or before $cutoffTime"
                )

                val candidateFolders = storageFolders
                    .filter { normalizePath(it.path) !in activeTableLocationSet }
                    .filter { normalizePath(it.path) !in excludedPathSet }
                    .filter { it.modificationTimeMillis <= cutoffTimeMillis }
                    .sortedBy { it.path }

                logger.info(
                    "Detected ${candidateFolders.size} candidate untracked table folder(s) for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                )

                if (candidateFolders.size > config.maxCandidateFoldersPerDatabase) {
                    logger.warn(
                        "Detected candidate folder count=${candidateFolders.size}, which exceeds max_candidate_folders_per_database=${config.maxCandidateFoldersPerDatabase}. Refusing to continue for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}. Narrow the scope or increase the limit explicitly."
                    )
                    return@forEach
                }

                candidateFolders.forEach { folder ->
                    logger.info(
                        "Dry-run candidate untracked table folder: path=${folder.path}, modifiedAt=${Instant.ofEpochMilli(folder.modificationTimeMillis)}"
                    )
                }
            }
        }

        logger.info(
            "Read-only discovery and candidate detection completed. No deletion was performed."
        )
    }

    private fun normalizePath(path: String): String =
        path.trim().trimEnd('/')

    private fun validateConfig() {
        require(config.catalog.isNotBlank()) {
            "catalog must not be blank"
        }

        require(config.databases.isNotEmpty()) {
            "databases must contain at least one database name"
        }

        require(config.olderThanHours >= 0) {
            "older_than_hours must be greater than or equal to 0"
        }

        require(config.maxCandidateFoldersPerDatabase >= 0) {
            "max_candidate_folders_per_database must be greater than or equal to 0"
        }

        require(config.dryRun) {
            "dry_run must be true because deletion mode is not implemented yet"
        }
    }
}
