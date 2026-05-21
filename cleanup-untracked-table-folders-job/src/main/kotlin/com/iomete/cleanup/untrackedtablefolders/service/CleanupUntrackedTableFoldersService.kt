package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.candidate.TooManyCandidateFoldersException
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDeletionService
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Duration
import java.time.Instant
import java.util.UUID
import org.jboss.logging.Logger

@ApplicationScoped
class CleanupUntrackedTableFoldersService {

    private val logger = Logger.getLogger(CleanupUntrackedTableFoldersService::class.java)
    @Inject lateinit var config: ApplicationConfig
    @Inject lateinit var catalogDiscoveryService: CatalogDiscoveryService
    @Inject lateinit var objectStorageDiscoveryService: ObjectStorageDiscoveryService
    @Inject lateinit var objectStorageDeletionService: ObjectStorageDeletionService
    @Inject lateinit var untrackedFolderCandidateDetector: UntrackedFolderCandidateDetector
    @Inject lateinit var cleanupAuditTableService: CleanupAuditTableService

    fun run() {

        logger.info("Loaded cleanup config: $config")
        val runId = UUID.randomUUID().toString()
        logger.info("Cleanup run id: $runId")
        cleanupAuditTableService.ensureAuditTableExists()

        config.databases.forEach { database ->
            val databaseStartTime = Instant.now()

            try {
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

                    writeDatabaseLocationMissingAuditRecord(
                        runId = runId,
                        databaseStartTime = databaseStartTime,
                        catalogName = discoveredDatabase.catalog,
                        databaseName = discoveredDatabase.database,
                        discoveredDatabaseLocation = discoveredDatabase.location,
                        activeTableCount = discoveredDatabase.tables.size.toLong(),
                        activeTableLocations =
                            discoveredDatabase.tables
                                .mapNotNull { it.location }
                                .sorted(),
                    )
                } else {
                    val storageScanLocation =
                        resolveStorageScanLocation(
                            databaseLocation = discoveredDatabase.location,
                            activeTableLocations =
                                discoveredDatabase.tables.mapNotNull { it.location },
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

                    val activeTableLocations =
                        discoveredDatabase.tables.mapNotNull { it.location }.sorted()

                    val storageFolderPaths = storageFolders.map { it.path }.sorted()

                    val candidateFolders =
                        try {
                            untrackedFolderCandidateDetector.detectCandidates(
                                storageFolders = storageFolders,
                                activeTableLocations =
                                    discoveredDatabase.tables.mapNotNull { it.location },
                                excludedPaths = config.excludePaths,
                                cutoffTimeMillis = cutoffTimeMillis,
                                maxCandidateFolders = config.maxCandidateFoldersPerDatabase,
                            )
                        } catch (th: TooManyCandidateFoldersException) {

                            logger.warn(
                                "Refusing to continue for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}. Narrow the scope or increase the limit explicitly.",
                                th,
                            )
                            val candidateFolderPaths = th.candidateFolderPaths.sorted()

                            writeTooManyCandidateFoldersAuditRecord(
                                runId = runId,
                                databaseStartTime = databaseStartTime,
                                catalogName = discoveredDatabase.catalog,
                                databaseName = discoveredDatabase.database,
                                discoveredDatabaseLocation = discoveredDatabase.location,
                                storageScanLocation = storageScanLocation,
                                activeTableCount = discoveredDatabase.tables.size.toLong(),
                                activeTableLocations = activeTableLocations,
                                storageFolderPaths = storageFolderPaths,
                                candidateFolderPaths = candidateFolderPaths,
                                candidateCount = th.candidateCount.toLong(),
                                cutoffTime = cutoffTime,
                                errorMessage = th.message,
                            )

                            return@forEach
                        }

                    logger.info(
                        "Detected ${candidateFolders.size} candidate untracked table folder(s) for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                    )

                    val candidateFolderPaths = candidateFolders.map { it.path }.sorted()

                    candidateFolders.forEach { folder ->
                        logger.info(
                            "Candidate untracked table folder selected for cleanup: path=${folder.path}, modifiedAt=${
                                Instant.ofEpochMilli(
                                    folder.modificationTimeMillis
                                )
                            }"
                        )
                    }

                    val deletedFolders =
                        when {
                            config.dryRun -> emptyList()

                            !config.deleteEnabled ->
                                throw IllegalStateException(
                                    "delete_enabled must be true before deleting candidate folders"
                                )

                            else -> {
                                val currentActiveTableLocationSet =
                                    catalogDiscoveryService
                                        .discoverDatabase(
                                            catalog = discoveredDatabase.catalog,
                                            database = discoveredDatabase.database,
                                        )
                                        .tables
                                        .mapNotNull { it.location }
                                        .map { StoragePathUtils.normalizeLocation(it) }
                                        .toSet()

                                candidateFolders
                                    .mapNotNull { candidateFolder ->
                                        val normalizedCandidatePath =
                                            StoragePathUtils.normalizeLocation(candidateFolder.path)

                                        if (normalizedCandidatePath in currentActiveTableLocationSet) {
                                            logger.warn(
                                                "Skipping deletion because candidate folder became active before deletion: path=${candidateFolder.path}"
                                            )
                                            null
                                        } else {
                                            objectStorageDeletionService
                                                .deleteFolderRecursively(candidateFolder.path)
                                                .takeIf { it.deleted }
                                                ?.path
                                        }
                                    }
                                    .sorted()
                            }
                        }

                    if (deletedFolders.isNotEmpty()) {
                        logger.warn(
                            "Deleted ${deletedFolders.size} untracked table folder(s) for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                        )
                        deletedFolders.forEach { deletedFolder ->
                            logger.warn("Deleted untracked table folder: path=$deletedFolder")
                        }
                    }

                    logCleanupSummary(
                        catalog = discoveredDatabase.catalog,
                        database = discoveredDatabase.database,
                        discoveredDatabaseLocation = discoveredDatabase.location,
                        storageScanLocation = storageScanLocation,
                        activeTableLocations = activeTableLocations,
                        storageFolderPaths = storageFolderPaths,
                        candidateFolderPaths = candidateFolderPaths,
                        deletedFolderPaths = deletedFolders,
                    )

                    writeSuccessAuditRecord(
                        runId = runId,
                        databaseStartTime = databaseStartTime,
                        catalogName = discoveredDatabase.catalog,
                        databaseName = discoveredDatabase.database,
                        discoveredDatabaseLocation = discoveredDatabase.location,
                        storageScanLocation = storageScanLocation,
                        activeTableCount = discoveredDatabase.tables.size.toLong(),
                        activeTableLocations = activeTableLocations,
                        storageFolderPaths = storageFolderPaths,
                        candidateFolderPaths = candidateFolderPaths,
                        deletedFolderPaths = deletedFolders,
                        cutoffTime = cutoffTime,
                    )
                }
            } catch (th: Throwable) {
                logger.error("Cleanup failed for catalog=${config.catalog}, database=$database", th)

                writeFailedAuditRecord(
                    runId = runId,
                    database = database,
                    databaseStartTime = databaseStartTime,
                    error = th,
                )
            }
        }

        if (config.dryRun) {
            logger.info(
                "Dry-run discovery and candidate detection completed. No deletion was performed."
            )
        } else {
            logger.warn(
                "Deletion mode completed. Candidate folders were deleted only when they passed the configured safety checks."
            )
        }
    }

    private fun logBlankLines(count: Int) {
        repeat(count) { logger.info("") }
    }

    private fun logCleanupSummary(
        catalog: String,
        database: String,
        discoveredDatabaseLocation: String?,
        storageScanLocation: String,
        activeTableLocations: List<String>,
        storageFolderPaths: List<String>,
        candidateFolderPaths: List<String>,
        deletedFolderPaths: List<String>,
    ) {
        val candidateFolderSet = candidateFolderPaths.toSet()
        val protectedFolderPaths = activeTableLocations.toSet()
        val nonCandidateStorageFolderPaths =
            storageFolderPaths.filter { it !in candidateFolderSet }.sorted()
        logBlankLines(3)
        logger.info("========== Cleanup Untracked Table Folders Summary ==========")
        logger.info("Catalog: $catalog")
        logger.info("Configured database: $database")
        logger.info("Discovered database location: $discoveredDatabaseLocation")
        logger.info("Object storage scan root: $storageScanLocation")
        logger.info("Protected catalog active table location count: ${activeTableLocations.size}")
        logger.info("Immediate child storage folders scanned: ${storageFolderPaths.size}")
        logger.info("Untracked candidate folder count: ${candidateFolderPaths.size}")
        logger.info("Deleted untracked folder count: ${deletedFolderPaths.size}")
        logger.info("Deletion performed: ${deletedFolderPaths.isNotEmpty()}")
        logger.info("Protected catalog active table locations:")
        logListOrNone(protectedFolderPaths.sorted())
        logger.info("Storage folders not selected as candidates:")
        logListOrNone(nonCandidateStorageFolderPaths)
        logger.info("Untracked candidate folders selected for cleanup:")
        logListOrNone(candidateFolderPaths)
        logger.info("Deleted folders:")
        logListOrNone(deletedFolderPaths)
        logger.info("============================================================")
        logBlankLines(3)
    }

    private fun logListOrNone(values: List<String>) {
        if (values.isEmpty()) {
            logger.info("- none")
        } else {
            values.take(MAX_AUDIT_PATH_SAMPLE_SIZE).forEach { value ->
                logger.info("- $value")
            }

            if (values.size > MAX_AUDIT_PATH_SAMPLE_SIZE) {
                logger.info("- ... truncated ${values.size - MAX_AUDIT_PATH_SAMPLE_SIZE} additional path(s)")
            }
        }
    }

    private fun auditPathSample(paths: List<String>): String =
        paths.take(MAX_AUDIT_PATH_SAMPLE_SIZE).joinToString("\n")

    private fun isAuditPathSampleTruncated(paths: List<String>): Boolean =
        paths.size > MAX_AUDIT_PATH_SAMPLE_SIZE

    private fun writeDatabaseLocationMissingAuditRecord(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        activeTableCount: Long,
        activeTableLocations: List<String>,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SKIPPED,
            statusReason = "database_location_missing",
            errorMessage = "Database location is missing; storage folder discovery was skipped.",
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            activeTableCount = activeTableCount,
            diagnosticDetails = diagnosticDetails(activeTableLocations = activeTableLocations),
        )
    }

    private fun writeTooManyCandidateFoldersAuditRecord(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        storageScanLocation: String,
        activeTableCount: Long,
        activeTableLocations: List<String>,
        storageFolderPaths: List<String>,
        candidateFolderPaths: List<String>,
        candidateCount: Long,
        cutoffTime: Instant,
        errorMessage: String?,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SKIPPED,
            statusReason = "too_many_candidate_folders",
            errorMessage = errorMessage,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            storageScanLocation = storageScanLocation,
            activeTableCount = activeTableCount,
            storageFolderCount = storageFolderPaths.size.toLong(),
            candidateFolderCount = candidateCount,
            candidateFolders = candidateFolderPaths.take(MAX_AUDIT_PATH_SAMPLE_SIZE),
            cutoffTime = cutoffTime,
            diagnosticDetails =
                diagnosticDetails(
                    activeTableLocations = activeTableLocations,
                    storageFolderPaths = storageFolderPaths,
                    candidateFolderPaths = candidateFolderPaths,
                ),
        )
    }

    private fun writeSuccessAuditRecord(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        storageScanLocation: String,
        activeTableCount: Long,
        activeTableLocations: List<String>,
        storageFolderPaths: List<String>,
        candidateFolderPaths: List<String>,
        deletedFolderPaths: List<String>,
        cutoffTime: Instant,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SUCCESS,
            statusReason = null,
            errorMessage = null,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            storageScanLocation = storageScanLocation,
            activeTableCount = activeTableCount,
            storageFolderCount = storageFolderPaths.size.toLong(),
            candidateFolderCount = candidateFolderPaths.size.toLong(),
            deletedFolderCount = deletedFolderPaths.size.toLong(),
            candidateFolders = candidateFolderPaths,
            deletedFolders = deletedFolderPaths,
            cutoffTime = cutoffTime,
            diagnosticDetails =
                diagnosticDetails(
                    activeTableLocations = activeTableLocations,
                    storageFolderPaths = storageFolderPaths,
                    candidateFolderPaths = candidateFolderPaths,
                    includeNonCandidateStorageFolders = true,
                ),
        )
    }

    private fun diagnosticDetails(
        activeTableLocations: List<String>,
        storageFolderPaths: List<String> = emptyList(),
        candidateFolderPaths: List<String> = emptyList(),
        includeNonCandidateStorageFolders: Boolean = false,
    ): Map<String, String> {
        val details =
            mutableMapOf(
                "active_table_locations_sample" to auditPathSample(activeTableLocations),
                "active_table_locations_truncated" to
                        isAuditPathSampleTruncated(activeTableLocations).toString(),
            )

        if (storageFolderPaths.isNotEmpty()) {
            details["storage_folder_paths_sample"] = auditPathSample(storageFolderPaths)
            details["storage_folder_paths_truncated"] =
                isAuditPathSampleTruncated(storageFolderPaths).toString()
        }

        if (candidateFolderPaths.isNotEmpty()) {
            details["candidate_folder_paths_sample"] = auditPathSample(candidateFolderPaths)
            details["candidate_folder_paths_truncated"] =
                isAuditPathSampleTruncated(candidateFolderPaths).toString()
        }

        if (includeNonCandidateStorageFolders) {
            val candidateFolderSet = candidateFolderPaths.toSet()
            val nonCandidateStorageFolderPaths =
                storageFolderPaths.filter { it !in candidateFolderSet }.sorted()

            details["non_candidate_storage_folder_paths_sample"] =
                auditPathSample(nonCandidateStorageFolderPaths)
            details["non_candidate_storage_folder_paths_truncated"] =
                isAuditPathSampleTruncated(nonCandidateStorageFolderPaths).toString()
        }

        return details
    }

    private fun writeAuditRecord(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        status: String,
        statusReason: String?,
        errorMessage: String?,
        discoveredDatabaseLocation: String? = null,
        storageScanLocation: String = "",
        activeTableCount: Long = 0,
        storageFolderCount: Long = 0,
        candidateFolderCount: Long = 0,
        deletedFolderCount: Long = 0,
        candidateFolders: List<String> = emptyList(),
        deletedFolders: List<String> = emptyList(),
        cutoffTime: Instant? = null,
        diagnosticDetails: Map<String, String> = emptyMap(),
    ) {
        cleanupAuditTableService.writeAuditRecord(
            CleanupAuditRecord(
                runId = runId,
                sparkAppId = cleanupAuditTableService.currentSparkAppId(),
                initiatedBy = cleanupAuditTableService.currentSparkUser(),
                catalogName = catalogName,
                databaseName = databaseName,
                operation = OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS,
                dryRun = config.dryRun,
                deleteEnabled = config.deleteEnabled,
                olderThanHours = config.olderThanHours,
                cutoffTime = cutoffTime,
                maxCandidateFoldersPerDatabase = config.maxCandidateFoldersPerDatabase,
                excludedPaths = config.excludePaths.sorted(),
                status = status,
                statusReason = statusReason,
                errorMessage = errorMessage,
                discoveredDatabaseLocation = discoveredDatabaseLocation,
                storageScanLocation = storageScanLocation,
                activeTableCount = activeTableCount,
                storageFolderCount = storageFolderCount,
                candidateFolderCount = candidateFolderCount,
                deletedFolderCount = deletedFolderCount,
                candidateFolders = candidateFolders,
                deletedFolders = deletedFolders,
                diagnosticDetails = diagnosticDetails,
                startTime = databaseStartTime,
                endTime = Instant.now(),
            )
        )
    }

    private fun writeFailedAuditRecord(
        runId: String,
        database: String,
        databaseStartTime: Instant,
        error: Throwable,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = config.catalog,
            databaseName = database,
            status = STATUS_FAILED,
            statusReason = "unexpected_error",
            errorMessage = error.message ?: error::class.java.name,
        )
    }

    private fun resolveStorageScanLocation(
        databaseLocation: String,
        activeTableLocations: List<String>,
    ): String {
        val inferredScanLocations =
            activeTableLocations.mapNotNull { parentLocation(it) }.distinct().sorted()

        val resolvedScanLocation =
            when (inferredScanLocations.size) {
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

        validateStorageScanLocation(
            databaseLocation = databaseLocation,
            storageScanLocation = resolvedScanLocation,
        )

        return resolvedScanLocation
    }

    private fun validateStorageScanLocation(
        databaseLocation: String,
        storageScanLocation: String,
    ) {
        val allowedRoots = StoragePathUtils.allowedDatabaseRoots(databaseLocation)

        if (!StoragePathUtils.isInsideAnyRoot(storageScanLocation, allowedRoots)) {
            throw IllegalStateException(
                "Resolved storage scan location escapes database boundary. databaseLocation=$databaseLocation, storageScanLocation=$storageScanLocation, allowedRoots=$allowedRoots"
            )
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

    private companion object {

        const val OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS = "DISCOVER_UNTRACKED_TABLE_FOLDERS"
        const val STATUS_SUCCESS = "SUCCESS"
        const val STATUS_SKIPPED = "SKIPPED"
        const val STATUS_FAILED = "FAILED"
        const val MAX_AUDIT_PATH_SAMPLE_SIZE = 100
    }
}
