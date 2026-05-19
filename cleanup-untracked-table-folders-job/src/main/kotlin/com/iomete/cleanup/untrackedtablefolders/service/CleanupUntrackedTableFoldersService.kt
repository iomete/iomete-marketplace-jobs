package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.candidate.TooManyCandidateFoldersException
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
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

    @Inject
    lateinit var untrackedFolderCandidateDetector: UntrackedFolderCandidateDetector

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
                val storageScanLocation = resolveStorageScanLocation(
                    databaseLocation = discoveredDatabase.location,
                    activeTableLocations = discoveredDatabase.tables.mapNotNull { it.location },
                )

                logger.info(
                    "Using storage scan location=$storageScanLocation for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}, discoveredDatabaseLocation=${discoveredDatabase.location}"
                )

                val storageFolders =
                    objectStorageDiscoveryService.listImmediateChildFolders(
                        location = storageScanLocation,
                    )

                logger.info(
                    "Discovered ${storageFolders.size} immediate storage folder(s) under storage scan location=$storageScanLocation"
                )

                storageFolders.forEach { folder ->
                    logger.info(
                        "Storage folder discovered: path=${folder.path}, modifiedAt=${Instant.ofEpochMilli(folder.modificationTimeMillis)}"
                    )
                }

                val cutoffTime = Instant.now().minus(Duration.ofHours(config.olderThanHours))
                val cutoffTimeMillis = cutoffTime.toEpochMilli()

                logger.info(
                    "Applying older_than_hours=${config.olderThanHours}; candidate folders must have modification time at or before $cutoffTime"
                )

                val candidateFolders =
                    try {
                        untrackedFolderCandidateDetector.detectCandidates(
                            storageFolders = storageFolders,
                            activeTableLocations = discoveredDatabase.tables.mapNotNull { it.location },
                            excludedPaths = config.excludePaths,
                            cutoffTimeMillis = cutoffTimeMillis,
                            maxCandidateFolders = config.maxCandidateFoldersPerDatabase,
                        )
                    } catch (th: TooManyCandidateFoldersException) {
                        logger.warn(
                            "Refusing to continue for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}. Narrow the scope or increase the limit explicitly.",
                            th,
                        )
                        return@forEach
                    }

                logger.info(
                    "Detected ${candidateFolders.size} candidate untracked table folder(s) for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                )

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

    private fun resolveStorageScanLocation(
        databaseLocation: String,
        activeTableLocations: List<String>,
    ): String {
        val inferredScanLocations = activeTableLocations
            .mapNotNull { parentLocation(it) }
            .distinct()
            .sorted()

        return when (inferredScanLocations.size) {
            0 -> {
                logger.warn(
                    "No active table locations were discovered for databaseLocation=$databaseLocation. Falling back to discovered database location for storage scan. This may miss untracked folders if the database location differs from the actual table storage root."
                )
                databaseLocation
            }
            1 -> inferredScanLocations.single()
            else -> {
                logger.warn(
                    "Multiple active table parent locations were discovered for databaseLocation=$databaseLocation: $inferredScanLocations. Falling back to database location."
                )
                databaseLocation
            }
        }
    }

    private fun parentLocation(location: String): String? {
        val normalizedLocation = location.trim().trimEnd('/')
        val lastSlashIndex = normalizedLocation.lastIndexOf('/')

        return if (lastSlashIndex <= 0) {
            null
        } else {
            normalizedLocation.substring(0, lastSlashIndex)
        }
    }

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
