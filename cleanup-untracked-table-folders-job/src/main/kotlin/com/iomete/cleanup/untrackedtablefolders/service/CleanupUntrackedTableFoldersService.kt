
package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService

import com.iomete.cleanup.untrackedtablefolders.candidate.TooManyCandidateFoldersException

import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector

import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig

import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService

import jakarta.enterprise.context.ApplicationScoped

import jakarta.inject.Inject

import java.time.Duration

import java.time.Instant

import java.util.UUID

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

    @Inject

    lateinit var cleanupAuditTableService: CleanupAuditTableService

    fun run() {

        logger.info("Loaded cleanup config: $config")

        validateConfig()

        val runId = UUID.randomUUID().toString()

        logger.info("Cleanup run id: $runId")

        cleanupAuditTableService.ensureAuditTableExists()

        config.databases.forEach { database ->

            val databaseStartTime = Instant.now()

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

                val activeTableLocations = discoveredDatabase.tables.mapNotNull { it.location }.sorted()

                val storageFolderPaths = storageFolders.map { it.path }.sorted()

                val candidateFolderPaths = candidateFolders.map { it.path }.sorted()

                logDryRunSummary(

                    catalog = discoveredDatabase.catalog,

                    database = discoveredDatabase.database,

                    discoveredDatabaseLocation = discoveredDatabase.location,

                    storageScanLocation = storageScanLocation,

                    activeTableLocations = activeTableLocations,

                    storageFolderPaths = storageFolderPaths,

                    candidateFolderPaths = candidateFolderPaths,

                )

                candidateFolders.forEach { folder ->

                    logger.info(

                        "Dry-run candidate untracked table folder: path=${folder.path}, modifiedAt=${Instant.ofEpochMilli(folder.modificationTimeMillis)}"

                    )

                }

                cleanupAuditTableService.writeAuditRecord(

                    CleanupAuditRecord(

                        sparkAppId = cleanupAuditTableService.currentSparkAppId(),

                        runId = runId,

                        initiatedBy = cleanupAuditTableService.currentSparkUser(),

                        catalogName = discoveredDatabase.catalog,

                        databaseName = discoveredDatabase.database,

                        operation = OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS,

                        dryRun = config.dryRun,

                        deleteEnabled = config.deleteEnabled,

                        status = STATUS_SUCCESS,

                        discoveredDatabaseLocation = discoveredDatabase.location,

                        storageScanLocation = storageScanLocation,

                        activeTableCount = activeTableLocations.size.toLong(),

                        storageFolderCount = storageFolderPaths.size.toLong(),

                        candidateFolderCount = candidateFolderPaths.size.toLong(),

                        deletedFolderCount = 0,

                        candidateFolders = candidateFolderPaths,

                        deletedFolders = emptyList(),

                        excludedPaths = config.excludePaths.sorted(),

                        metrics = mapOf(

                            "older_than_hours" to config.olderThanHours.toString(),

                            "max_candidate_folders_per_database" to config.maxCandidateFoldersPerDatabase.toString(),

                            "cutoff_time" to cutoffTime.toString(),

                        ),

                        errorMessage = null,

                        startTime = databaseStartTime,

                        endTime = Instant.now(),

                    )

                )

            }

        }

        logger.info(

            "Read-only discovery and candidate detection completed. No deletion was performed."

        )

    }

    private fun logDryRunSummary(

        catalog: String,

        database: String,

        discoveredDatabaseLocation: String?,

        storageScanLocation: String,

        activeTableLocations: List<String>,

        storageFolderPaths: List<String>,

        candidateFolderPaths: List<String>,

    ) {

        val candidateFolderSet = candidateFolderPaths.toSet()

        val protectedFolderPaths = activeTableLocations.toSet()

        val nonCandidateStorageFolderPaths = storageFolderPaths

            .filter { it !in candidateFolderSet }

            .sorted()

        logger.info("========== Cleanup Untracked Table Folders Dry-Run Summary ==========")

        logger.info("Catalog: $catalog")

        logger.info("Database: $database")

        logger.info("Discovered database location: $discoveredDatabaseLocation")

        logger.info("Storage scan location: $storageScanLocation")

        logger.info("Active table location count: ${activeTableLocations.size}")

        logger.info("Storage folder count: ${storageFolderPaths.size}")

        logger.info("Candidate folder count: ${candidateFolderPaths.size}")

        logger.info("Deletion performed: false")

        logger.info("Protected active table folders:")

        logListOrNone(protectedFolderPaths.sorted())

        logger.info("Storage folders not selected as candidates:")

        logListOrNone(nonCandidateStorageFolderPaths)

        logger.info("Candidate folders that would be deleted if destructive mode were enabled:")

        logListOrNone(candidateFolderPaths)

        logger.info("====================================================================")

    }

    private fun logListOrNone(values: List<String>) {

        if (values.isEmpty()) {

            logger.info("- none")

        } else {

            values.forEach { value -> logger.info("- $value") }

        }

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

    private companion object {

        const val OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS =

            "DISCOVER_UNTRACKED_TABLE_FOLDERS"

        const val STATUS_SUCCESS = "SUCCESS"

    }

}

