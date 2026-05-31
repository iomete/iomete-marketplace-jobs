package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditDiagnosticDetailsBuilder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.candidate.TooManyCandidateFoldersException
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.catalog.DatabaseNotFoundException
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummary
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummaryLogger
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDeletionService
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import com.iomete.cleanup.untrackedtablefolders.storage.StorageScanLocationResolver
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
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
    @Inject lateinit var auditDiagnosticDetailsBuilder: CleanupAuditDiagnosticDetailsBuilder
    @Inject lateinit var cleanupSummaryLogger: CleanupSummaryLogger
    @Inject lateinit var storageScanLocationResolver: StorageScanLocationResolver

    fun run() {

        logger.info("Loaded cleanup config: $config")
        val runId = UUID.randomUUID().toString()
        logger.info("Cleanup run id: $runId")
        cleanupAuditTableService.logRuntimeIdentityEnvVars()
        cleanupAuditTableService.ensureAuditTableExists()

        config.databases.forEach { database ->
            val databaseStartTime = Instant.now()
            var effectiveExcludedPathsForAudit = normalizedConfiguredExcludePaths()
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

                if (discoveredDatabase.tables.mapNotNull { it.location }.isEmpty()) {
                    logger.warn(
                        "Skipping cleanup because database has no active tables with discoverable locations in the catalog. " +
                                "Cleanup requires at least one active table location to anchor the safety check. " +
                                "catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}, totalCatalogTables=${discoveredDatabase.tables.size}"
                    )

                    writeNoActiveTablesAuditRecord(
                        runId = runId,
                        databaseStartTime = databaseStartTime,
                        catalogName = discoveredDatabase.catalog,
                        databaseName = discoveredDatabase.database,
                        discoveredDatabaseLocation = discoveredDatabase.location,
                    )

                    return@forEach
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
                        storageScanLocationResolver.resolve(
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

                    val effectiveExcludedPaths =
                        effectiveExcludedPaths(
                            database = discoveredDatabase.database,
                            storageScanLocation = storageScanLocation,
                        )

                    effectiveExcludedPathsForAudit = effectiveExcludedPaths

                    if (effectiveExcludedPaths.isNotEmpty()) {
                        logger.info(
                            "Using ${effectiveExcludedPaths.size} effective excluded path(s) for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                        )
                        effectiveExcludedPaths.forEach { excludedPath ->
                            logger.info("Effective excluded path: $excludedPath")
                        }
                    }

                    val storageFolderPaths = storageFolders.map { it.path }.sorted()

                    val candidateFolders =
                        try {
                            untrackedFolderCandidateDetector.detectCandidates(
                                storageFolders = storageFolders,
                                activeTableLocations =
                                    discoveredDatabase.tables.mapNotNull { it.location },
                                excludedPaths = effectiveExcludedPaths,
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
                                excludedPaths = effectiveExcludedPaths,
                                errorMessage = th.message,
                            )

                            return@forEach
                        }

                    logger.info(
                        "Detected ${candidateFolders.size} candidate untracked table folder(s) for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                    )

                    val candidateFolderPaths = candidateFolders.map { it.path }.sorted()
                    val candidateSizeStatsByFolder =
                        collectCandidateSizeStatsByFolder(candidateFolderPaths)
                    val candidateSizeStats: StorageSizeStats? =
                        if (config.collectSizeStatistics) {
                            sumSizeStats(candidateSizeStatsByFolder.values)
                        } else {
                            null
                        }

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
                                val currentActiveTableLocations =
                                    catalogDiscoveryService
                                        .discoverDatabase(
                                            catalog = discoveredDatabase.catalog,
                                            database = discoveredDatabase.database,
                                        )
                                        .tables
                                        .mapNotNull { it.location }
                                        .map { StoragePathUtils.normalizeLocation(it) }

                                candidateFolders
                                    .mapNotNull { candidateFolder ->
                                        val normalizedCandidatePath =
                                            StoragePathUtils.normalizeLocation(candidateFolder.path)

                                        val claimedByActiveTable =
                                            currentActiveTableLocations.any { activeLocation ->
                                                StoragePathUtils.isSameOrChildLocation(
                                                    candidateLocation = activeLocation,
                                                    rootLocation = normalizedCandidatePath,
                                                )
                                            }

                                        if (claimedByActiveTable) {
                                            logger.warn(
                                                "Skipping deletion because candidate folder is or contains an active table location: path=${candidateFolder.path}"
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

                    val deletedSizeStats: StorageSizeStats? =
                        if (config.collectSizeStatistics) {
                            sumSizeStats(deletedFolders.mapNotNull { candidateSizeStatsByFolder[it] })
                        } else {
                            null
                        }

                    if (deletedFolders.isNotEmpty()) {
                        logger.warn(
                            "Deleted ${deletedFolders.size} untracked table folder(s) for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                        )
                        deletedFolders.forEach { deletedFolder ->
                            logger.warn("Deleted untracked table folder: path=$deletedFolder")
                        }
                    }

                    cleanupSummaryLogger.logCleanupSummary(
                        CleanupSummary(
                            catalog = discoveredDatabase.catalog,
                            database = discoveredDatabase.database,
                            discoveredDatabaseLocation = discoveredDatabase.location,
                            storageScanLocation = storageScanLocation,
                            activeTableLocations = activeTableLocations,
                            storageFolderPaths = storageFolderPaths,
                            excludedPaths = effectiveExcludedPaths,
                            candidateFolderPaths = candidateFolderPaths,
                            candidateSizeStats = candidateSizeStats,
                            deletedFolderPaths = deletedFolders,
                            deletedSizeStats = deletedSizeStats,
                        )
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
                        candidateSizeStats = candidateSizeStats,
                        deletedFolderPaths = deletedFolders,
                        deletedSizeStats = deletedSizeStats,
                        cutoffTime = cutoffTime,
                        excludedPaths = effectiveExcludedPaths,
                    )
                }
            } catch (th: DatabaseNotFoundException) {
                logger.warn(
                    "Configured database was not found; skipping cleanup for catalog=${config.catalog}, database=$database"
                )
                logger.debug(
                    "Configured database was not found details for catalog=${config.catalog}, database=$database",
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
                    excludedPaths = effectiveExcludedPathsForAudit,
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

    private fun collectCandidateSizeStatsByFolder(
        candidateFolderPaths: List<String>,
    ): Map<String, StorageSizeStats> {
        if (candidateFolderPaths.isEmpty()) {
            return emptyMap()
        }

        if (!config.collectSizeStatistics) {
            logger.info(
                "Skipping size statistics because collect_size_statistics=false. Candidate and deleted size audit fields will be NULL."
            )
            return emptyMap()
        }

        logger.info(
            "Collecting size statistics for ${candidateFolderPaths.size} candidate folder(s). This may take time for folders with many objects. To skip this step, set collect_size_statistics=false."
        )

        val result = mutableMapOf<String, StorageSizeStats>()
        var failedFolderCount = 0

        candidateFolderPaths.forEach { candidateFolderPath ->
            try {
                result[candidateFolderPath] =
                    objectStorageDiscoveryService.collectSizeStats(listOf(candidateFolderPath))
            } catch (th: Throwable) {
                failedFolderCount += 1
                logger.warn(
                    "Failed to collect size statistics for candidate folder; recording unknown size and continuing without aborting cleanup: path=$candidateFolderPath",
                    th,
                )
            }
        }

        if (failedFolderCount > 0) {
            logger.warn(
                "Size statistics collection failed for $failedFolderCount of ${candidateFolderPaths.size} candidate folder(s). Candidate and deleted size audit fields exclude the failed folders."
            )
        }

        return result
    }

    private fun sumSizeStats(stats: Iterable<StorageSizeStats>): StorageSizeStats =
        stats.fold(StorageSizeStats.ZERO) { total, current ->
            StorageSizeStats(
                objectCount = total.objectCount + current.objectCount,
                totalSizeBytes = total.totalSizeBytes + current.totalSizeBytes,
            )
        }

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
            diagnosticDetails = auditDiagnosticDetailsBuilder.build(activeTableLocations = activeTableLocations),
        )
    }

    private fun writeNoActiveTablesAuditRecord(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SKIPPED,
            statusReason = "no_active_tables_in_database",
            errorMessage =
                "Database has no active tables with discoverable locations in the catalog. " +
                        "Cleanup was skipped to avoid wholesale deletion of immediate child folders.",
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            activeTableCount = 0,
            diagnosticDetails = auditDiagnosticDetailsBuilder.build(activeTableLocations = emptyList()),
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
        excludedPaths: List<String>,
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
            excludedPaths = excludedPaths,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(
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
        candidateSizeStats: StorageSizeStats?,
        deletedFolderPaths: List<String>,
        deletedSizeStats: StorageSizeStats?,
        cutoffTime: Instant,
        excludedPaths: List<String>,
    ) {
        val candidateFolderPathSet = candidateFolderPaths.toSet()
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
            candidateObjectCount = candidateSizeStats?.objectCount,
            candidateTotalSizeBytes = candidateSizeStats?.totalSizeBytes,
            deletedFolderCount = deletedFolderPaths.size.toLong(),
            deletedObjectCount = deletedSizeStats?.objectCount,
            deletedTotalSizeBytes = deletedSizeStats?.totalSizeBytes,
            candidateFolders = candidateFolderPaths,
            deletedFolders = deletedFolderPaths,
            cutoffTime = cutoffTime,
            excludedPaths = excludedPaths,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(
                    activeTableLocations = activeTableLocations,
                    storageFolderPaths = storageFolderPaths,
                    candidateFolderPaths = candidateFolderPaths,
                    nonCandidateStorageFolderPaths =
                        storageFolderPaths.filter { it !in candidateFolderPathSet }.sorted(),
                ),
        )
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
        candidateObjectCount: Long? = null,
        candidateTotalSizeBytes: Long? = null,
        deletedFolderCount: Long = 0,
        deletedObjectCount: Long? = null,
        deletedTotalSizeBytes: Long? = null,
        candidateFolders: List<String> = emptyList(),
        deletedFolders: List<String> = emptyList(),
        cutoffTime: Instant? = null,
        excludedPaths: List<String> = normalizedConfiguredExcludePaths(),
        diagnosticDetails: Map<String, String> = emptyMap(),
    ) {
        cleanupAuditTableService.writeAuditRecord(
            CleanupAuditRecord(
                runId = runId,
                sparkAppId = cleanupAuditTableService.currentSparkAppId(),
                runtimeComputeId = cleanupAuditTableService.currentRuntimeComputeId(),
                runtimeComputeNamespace = cleanupAuditTableService.currentRuntimeComputeNamespace(),
                runtimeDomain = cleanupAuditTableService.currentRuntimeDomain(),
                runtimeUser = cleanupAuditTableService.currentRuntimeUser(),
                externalJobId = cleanupAuditTableService.currentExternalJobId(),
                platformStartedBy = cleanupAuditTableService.currentPlatformStartedBy(),
                catalogName = catalogName,
                databaseName = databaseName,
                operation = OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS,
                dryRun = config.dryRun,
                deleteEnabled = config.deleteEnabled,
                olderThanHours = config.olderThanHours,
                cutoffTime = cutoffTime,
                maxCandidateFoldersPerDatabase = config.maxCandidateFoldersPerDatabase,
                excludedPaths = excludedPaths.sorted(),
                status = status,
                statusReason = statusReason,
                errorMessage = errorMessage,
                discoveredDatabaseLocation = discoveredDatabaseLocation,
                storageScanLocation = storageScanLocation,
                activeTableCount = activeTableCount,
                storageFolderCount = storageFolderCount,
                candidateFolderCount = candidateFolderCount,
                candidateObjectCount = candidateObjectCount,
                candidateTotalSizeBytes = candidateTotalSizeBytes,
                deletedFolderCount = deletedFolderCount,
                deletedObjectCount = deletedObjectCount,
                deletedTotalSizeBytes = deletedTotalSizeBytes,
                candidateFolders = candidateFolders,
                deletedFolders = deletedFolders,
                diagnosticDetails = diagnosticDetails,
                startTime = databaseStartTime,
                endTime = Instant.now(),
            )
        )
    }

    private fun writeDatabaseNotFoundAuditRecord(
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
            status = STATUS_SKIPPED,
            statusReason = "database_not_found",
            errorMessage = error.message ?: error::class.java.name,
        )
    }

    private fun writeFailedAuditRecord(
        runId: String,
        database: String,
        databaseStartTime: Instant,
        excludedPaths: List<String> = normalizedConfiguredExcludePaths(),
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
            excludedPaths = excludedPaths,
        )
    }

    private fun normalizedConfiguredExcludePaths(): List<String> =
        config.excludePaths
            .map { StoragePathUtils.normalizeLocation(it) }
            .distinct()
            .sorted()

    private fun effectiveExcludedPaths(
        database: String,
        storageScanLocation: String,
    ): List<String> =
        (normalizedConfiguredExcludePaths() + resolvedExcludeDatabaseFolderPaths(database, storageScanLocation))
            .map { StoragePathUtils.normalizeLocation(it) }
            .distinct()
            .sorted()

    private fun resolvedExcludeDatabaseFolderPaths(
        database: String,
        storageScanLocation: String,
    ): List<String> =
        config.excludeDatabaseFolders.mapNotNull { excludedDatabaseFolder ->
            val parts = excludedDatabaseFolder.split(".", limit = 2)
            if (parts.size != 2) return@mapNotNull null

            val excludedDatabase = parts[0]
            val excludedFolder = parts[1]

            if (excludedDatabase != database) return@mapNotNull null
            StoragePathUtils.normalizeLocation("$storageScanLocation/$excludedFolder")
        }

    private companion object {

        const val OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS = "DISCOVER_UNTRACKED_TABLE_FOLDERS"
        const val STATUS_SUCCESS = "SUCCESS"
        const val STATUS_SKIPPED = "SKIPPED"
        const val STATUS_FAILED = "FAILED"
        const val MAX_AUDIT_PATH_SAMPLE_SIZE = 100
    }
}
