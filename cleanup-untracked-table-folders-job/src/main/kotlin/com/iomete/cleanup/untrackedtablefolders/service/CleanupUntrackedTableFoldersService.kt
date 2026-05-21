package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.candidate.TooManyCandidateFoldersException
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.catalog.DatabaseNotFoundException
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
                            status = STATUS_SKIPPED,
                            discoveredDatabaseLocation = discoveredDatabase.location,
                            storageScanLocation = "",
                            activeTableCount = discoveredDatabase.tables.size.toLong(),
                            storageFolderCount = 0,
                            candidateFolderCount = 0,
                            deletedFolderCount = 0,
                            candidateFolders = emptyList(),
                            deletedFolders = emptyList(),
                            excludedPaths = config.excludePaths.sorted(),
                            metrics =
                                mapOf(
                                    "skip_reason" to "database_location_missing",
                                    "older_than_hours" to config.olderThanHours.toString(),
                                    "max_candidate_folders_per_database" to
                                            config.maxCandidateFoldersPerDatabase.toString(),
                                    "active_table_locations_sample" to
                                            auditPathSample(
                                                discoveredDatabase.tables
                                                    .mapNotNull { it.location }
                                                    .sorted()
                                            ),
                                    "active_table_locations_truncated" to
                                            isAuditPathSampleTruncated(
                                                discoveredDatabase.tables
                                                    .mapNotNull { it.location }
                                                    .sorted()
                                            ).toString(),
                                ),
                            errorMessage =
                                "Database location is missing; storage folder discovery was skipped.",
                            startTime = databaseStartTime,
                            endTime = Instant.now(),
                        )
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
                                    status = STATUS_SKIPPED,
                                    discoveredDatabaseLocation = discoveredDatabase.location,
                                    storageScanLocation = storageScanLocation,
                                    activeTableCount = activeTableLocations.size.toLong(),
                                    storageFolderCount = storageFolderPaths.size.toLong(),
                                    candidateFolderCount = th.candidateCount.toLong(),
                                    deletedFolderCount = 0,
                                    candidateFolders = candidateFolderPaths.take(MAX_AUDIT_PATH_SAMPLE_SIZE),
                                    deletedFolders = emptyList(),
                                    excludedPaths = config.excludePaths.sorted(),
                                    metrics =
                                        mapOf(
                                            "skip_reason" to "too_many_candidate_folders",
                                            "older_than_hours" to config.olderThanHours.toString(),
                                            "max_candidate_folders_per_database" to
                                                    config.maxCandidateFoldersPerDatabase.toString(),
                                            "cutoff_time" to cutoffTime.toString(),
                                            "active_table_locations_sample" to auditPathSample(activeTableLocations),
                                            "active_table_locations_truncated" to
                                                    isAuditPathSampleTruncated(activeTableLocations).toString(),
                                            "storage_folder_paths_sample" to auditPathSample(storageFolderPaths),
                                            "storage_folder_paths_truncated" to
                                                    isAuditPathSampleTruncated(storageFolderPaths).toString(),
                                            "candidate_folder_paths_sample" to auditPathSample(candidateFolderPaths),
                                            "candidate_folder_paths_truncated" to
                                                    isAuditPathSampleTruncated(candidateFolderPaths).toString(),
                                        ),
                                    errorMessage = th.message,
                                    startTime = databaseStartTime,
                                    endTime = Instant.now(),
                                )
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
                            deletedFolderCount = deletedFolders.size.toLong(),
                            candidateFolders = candidateFolderPaths,
                            deletedFolders = deletedFolders,
                            excludedPaths = config.excludePaths.sorted(),
                            metrics =
                                mapOf(
                                    "older_than_hours" to config.olderThanHours.toString(),
                                    "max_candidate_folders_per_database" to
                                            config.maxCandidateFoldersPerDatabase.toString(),
                                    "cutoff_time" to cutoffTime.toString(),
                                    "active_table_locations_sample" to auditPathSample(activeTableLocations),
                                    "active_table_locations_truncated" to
                                            isAuditPathSampleTruncated(activeTableLocations).toString(),
                                    "storage_folder_paths_sample" to auditPathSample(storageFolderPaths),
                                    "storage_folder_paths_truncated" to
                                            isAuditPathSampleTruncated(storageFolderPaths).toString(),
                                    "non_candidate_storage_folder_paths_sample" to
                                            auditPathSample(
                                                storageFolderPaths
                                                    .filter { it !in candidateFolderPaths.toSet() }
                                                    .sorted()
                                            ),
                                    "non_candidate_storage_folder_paths_truncated" to
                                            isAuditPathSampleTruncated(
                                                storageFolderPaths
                                                    .filter { it !in candidateFolderPaths.toSet() }
                                                    .sorted()
                                            ).toString(),
                                ),
                            errorMessage = null,
                            startTime = databaseStartTime,
                            endTime = Instant.now(),
                        )
                    )
                }
            } catch (th: DatabaseNotFoundException) {
                logger.warn(
                    "Configured database was not found; skipping cleanup for catalog=${config.catalog}, database=$database",
                    th,
                )

                writeDatabaseNotFoundAuditRecord(
                    runId = runId,
                    database = database,
                    databaseStartTime = databaseStartTime,
                    error = th,
                )
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
        logger.info("")

        logger.info("")

        logger.info("")

        logger.info("========== Cleanup Untracked Table Folders Summary ==========")

        logger.info("Catalog: $catalog")

        logger.info("Configured database: $database")

        logger.info("Discovered database location: $discoveredDatabaseLocation")

        logger.info("Object storage scan root: $storageScanLocation")

        logger.info("Catalog active table location count: ${activeTableLocations.size}")

        logger.info("Immediate child storage folders scanned: ${storageFolderPaths.size}")

        logger.info("Untracked candidate folder count: ${candidateFolderPaths.size}")

        logger.info("Deleted folder count: ${deletedFolderPaths.size}")

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

        logger.info("")

        logger.info("")

        logger.info("")

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

    private fun writeDatabaseNotFoundAuditRecord(
        runId: String,
        database: String,
        databaseStartTime: Instant,
        error: Throwable,
    ) {
        writeUnsuccessfulAuditRecord(
            runId = runId,
            database = database,
            databaseStartTime = databaseStartTime,
            status = STATUS_SKIPPED,
            reasonKey = "skip_reason",
            reasonValue = "database_not_found",
            error = error,
        )
    }

    private fun writeFailedAuditRecord(
        runId: String,
        database: String,
        databaseStartTime: Instant,
        error: Throwable,
    ) {
        writeUnsuccessfulAuditRecord(
            runId = runId,
            database = database,
            databaseStartTime = databaseStartTime,
            status = STATUS_FAILED,
            reasonKey = "failure_reason",
            reasonValue = error.message ?: error::class.java.name,
            error = error,
        )
    }

    private fun writeUnsuccessfulAuditRecord(
        runId: String,
        database: String,
        databaseStartTime: Instant,
        status: String,
        reasonKey: String,
        reasonValue: String,
        error: Throwable,
    ) {
        cleanupAuditTableService.writeAuditRecord(
            CleanupAuditRecord(
                sparkAppId = cleanupAuditTableService.currentSparkAppId(),
                runId = runId,
                initiatedBy = cleanupAuditTableService.currentSparkUser(),
                catalogName = config.catalog,
                databaseName = database,
                operation = OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS,
                dryRun = config.dryRun,
                deleteEnabled = config.deleteEnabled,
                status = status,
                discoveredDatabaseLocation = null,
                storageScanLocation = "",
                activeTableCount = 0,
                storageFolderCount = 0,
                candidateFolderCount = 0,
                deletedFolderCount = 0,
                candidateFolders = emptyList(),
                deletedFolders = emptyList(),
                excludedPaths = config.excludePaths.sorted(),
                metrics =
                    mapOf(
                        reasonKey to reasonValue,
                        "older_than_hours" to config.olderThanHours.toString(),
                        "max_candidate_folders_per_database" to
                                config.maxCandidateFoldersPerDatabase.toString(),
                    ),
                errorMessage = error.message ?: error::class.java.name,
                startTime = databaseStartTime,
                endTime = Instant.now(),
            )
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
