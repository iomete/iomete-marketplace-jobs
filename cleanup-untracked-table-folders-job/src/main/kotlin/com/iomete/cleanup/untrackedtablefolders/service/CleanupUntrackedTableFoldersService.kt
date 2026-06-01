package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecorder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.candidate.TooManyCandidateFoldersException
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.catalog.DatabaseNotFoundException
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummary
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummaryLogger
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateDeletionGate
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateSizeStatCollector
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
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
    @Inject lateinit var untrackedFolderCandidateDetector: UntrackedFolderCandidateDetector
    @Inject lateinit var cleanupAuditTableService: CleanupAuditTableService
    @Inject lateinit var cleanupAuditRecorder: CleanupAuditRecorder
    @Inject lateinit var cleanupSummaryLogger: CleanupSummaryLogger
    @Inject lateinit var storageScanLocationResolver: StorageScanLocationResolver
    @Inject lateinit var excludePathResolver: ExcludePathResolver
    @Inject lateinit var candidateSizeStatCollector: CandidateSizeStatCollector
    @Inject lateinit var candidateDeletionGate: CandidateDeletionGate

    fun run() {

        logger.info("Loaded cleanup config: $config")
        val runId = UUID.randomUUID().toString()
        logger.info("Cleanup run id: $runId")
        cleanupAuditTableService.logRuntimeIdentityEnvVars()
        cleanupAuditTableService.ensureAuditTableExists()

        config.databases.forEach { database ->
            val databaseStartTime = Instant.now()
            var effectiveExcludedPathsForAudit = excludePathResolver.normalizedConfiguredExcludePaths()
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

                    cleanupAuditRecorder.recordNoActiveTables(
                        runId = runId,
                        databaseStartTime = databaseStartTime,
                        catalogName = discoveredDatabase.catalog,
                        databaseName = discoveredDatabase.database,
                        discoveredDatabaseLocation = discoveredDatabase.location,
                        excludedPaths = excludePathResolver.normalizedConfiguredExcludePaths(),
                    )

                    return@forEach
                }

                if (discoveredDatabase.location.isNullOrBlank()) {
                    logger.warn(
                        "Skipping storage folder discovery because database location is missing for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                    )

                    cleanupAuditRecorder.recordDatabaseLocationMissing(
                        runId = runId,
                        databaseStartTime = databaseStartTime,
                        catalogName = discoveredDatabase.catalog,
                        databaseName = discoveredDatabase.database,
                        discoveredDatabaseLocation = discoveredDatabase.location,
                        errorMessage = "Database location is missing; storage folder discovery was skipped.",
                        excludedPaths = excludePathResolver.normalizedConfiguredExcludePaths(),
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
                        excludePathResolver.effectiveExcludedPaths(
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

                            cleanupAuditRecorder.recordTooManyCandidateFolders(
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
                                cutoffTime = cutoffTime,
                                excludedPaths = effectiveExcludedPaths,
                                errorMessage = th.message ?: th::class.java.name,
                            )

                            return@forEach
                        }

                    logger.info(
                        "Detected ${candidateFolders.size} candidate untracked table folder(s) for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                    )

                    val candidateFolderPaths = candidateFolders.map { it.path }.sorted()
                    val candidateSizeStatsByFolder =
                        candidateSizeStatCollector.collectPerFolder(candidateFolderPaths)
                    val candidateSizeStats: StorageSizeStats? =
                        if (config.collectSizeStatistics) {
                            candidateSizeStatCollector.sum(candidateSizeStatsByFolder.values)
                        } else {
                            null
                        }

                    candidateFolders.forEach { folder ->
                        val modifiedAt = Instant.ofEpochMilli(folder.modificationTimeMillis)
                        logger.info(
                            "Candidate untracked table folder selected for cleanup: path=${folder.path}, modifiedAt=$modifiedAt"
                        )
                    }

                    val deletedFolders =
                        candidateDeletionGate.deleteCandidates(
                            catalog = discoveredDatabase.catalog,
                            database = discoveredDatabase.database,
                            candidateFolders = candidateFolders,
                        )

                    val deletedSizeStats: StorageSizeStats? =
                        if (config.collectSizeStatistics) {
                            candidateSizeStatCollector.sum(
                                deletedFolders.mapNotNull { candidateSizeStatsByFolder[it] }
                            )
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

                    cleanupAuditRecorder.recordSuccess(
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

                cleanupAuditRecorder.recordDatabaseNotFound(
                    runId = runId,
                    database = database,
                    databaseStartTime = databaseStartTime,
                    error = th,
                )
            } catch (th: Throwable) {
                logger.error("Cleanup failed for catalog=${config.catalog}, database=$database", th)

                cleanupAuditRecorder.recordFailed(
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

}
