package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecorder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.audit.DeletionBlockReason
import com.iomete.cleanup.untrackedtablefolders.audit.OwnershipChecks
import com.iomete.cleanup.untrackedtablefolders.candidate.StorageFolderReconciliation
import com.iomete.cleanup.untrackedtablefolders.candidate.TooManyCandidateFoldersException
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.catalog.DatabaseNotFoundException
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummary
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummaryLogger
import com.iomete.cleanup.untrackedtablefolders.logging.NoVerifiedLocationSummary
import com.iomete.cleanup.untrackedtablefolders.logging.TooManyFoldersSummary
import com.iomete.cleanup.untrackedtablefolders.logging.UnresolvedTableDetail
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateDeletionGate
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateDeletionResult
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateSizeStatCollector
import com.iomete.cleanup.untrackedtablefolders.storage.CleanupRootListing
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.storage.PartialDeletionException
import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import com.iomete.cleanup.untrackedtablefolders.storage.StorageScanLocationPlanner
import com.iomete.cleanup.untrackedtablefolders.storage.StorageScanLocationResolver
import com.iomete.cleanup.untrackedtablefolders.storage.UnsafeScanRootException
import com.iomete.cleanup.untrackedtablefolders.storage.UnsafeScanRootReason
import com.iomete.cleanup.untrackedtablefolders.storage.managedRootRefused
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
    @Inject lateinit var storageScanLocationPlanner: StorageScanLocationPlanner
    @Inject lateinit var reportOnlyRootScanner: ReportOnlyRootScanner

    fun run() {

        logger.info("Loaded cleanup config: $config")
        val runId = UUID.randomUUID().toString()
        logger.info("Cleanup run id: $runId")
        cleanupAuditTableService.logRuntimeIdentityEnvVars()
        cleanupAuditTableService.ensureAuditTableExists()

        var auditFailure: Throwable? = null
        val otherStorageIndex = OtherStorageIndex(catalogDiscoveryService)

        fun recordAudit(
            database: String,
            write: () -> Unit,
        ) {
            try {
                write()
            } catch (th: Throwable) {
                logger.error(
                    "Failed to write the cleanup audit row for catalog=${config.catalog}, database=$database. No further folders will be deleted in this run.",
                    th,
                )
                if (auditFailure == null) {
                    auditFailure = th
                }
            }
        }

        config.databases.forEach { database ->
            val databaseStartTime = Instant.now()
            var effectiveExcludedPathsForAudit = excludePathResolver.normalizedConfiguredExcludePaths()
            var unscannedTableLocationsForAudit: List<String>? = null
            var scanPlanForAudit: ScanPlanReport? = null
            var unresolvedTablesForAudit = emptyList<String>()
            var ownershipChecksForAudit = OwnershipChecks()
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

                val unresolvedTableNames = discoveredDatabase.unresolvedTables.map { it.qualifiedName }.sorted()
                unresolvedTablesForAudit = unresolvedTableNames
                val unresolvedTableDetails =
                    discoveredDatabase.unresolvedTables.associate { table ->
                        table.qualifiedName to UnresolvedTableDetail(table.unresolvedReason.orEmpty(), table.knownLocation)
                    }

                if (unresolvedTableNames.isNotEmpty()) {
                    logger.warn(
                        "${unresolvedTableNames.size} catalog table(s) could not be resolved to a storage location. " +
                            "No folder in this database is eligible for deletion. " +
                            "catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                    )
                    unresolvedTableNames.forEach { logger.warn("Unresolved catalog table: $it") }
                }

                // With no verified location there is nothing to compare folders against, so scanning cannot change the outcome.
                if (unresolvedTableNames.isNotEmpty() && discoveredDatabase.tables.all { it.location == null }) {
                    logger.warn(
                        "Not scanning storage because no table has a verified storage location. " +
                            "catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}, totalCatalogTables=${discoveredDatabase.tables.size}"
                    )

                    cleanupSummaryLogger.logNoVerifiedLocationReport(
                        NoVerifiedLocationSummary(
                            catalog = discoveredDatabase.catalog,
                            database = discoveredDatabase.database,
                            discoveredDatabaseLocation = discoveredDatabase.location,
                            catalogTableCount = discoveredDatabase.tables.size,
                            unresolvedTableNames = unresolvedTableNames,
                            dryRun = config.dryRun,
                            unresolvedTableDetails = unresolvedTableDetails,
                        )
                    )

                    recordAudit(database) {
                        cleanupAuditRecorder.recordOwnershipUnverified(
                            runId = runId,
                            databaseStartTime = databaseStartTime,
                            catalogName = discoveredDatabase.catalog,
                            databaseName = discoveredDatabase.database,
                            discoveredDatabaseLocation = discoveredDatabase.location,
                            storageScanLocation = "",
                            activeTableCount = discoveredDatabase.tables.size.toLong(),
                            unresolvedTables = unresolvedTableNames,
                            activeTableLocations = emptyList(),
                            storageFolderPaths = emptyList(),
                            potentiallyUntrackedFolderPaths = emptyList(),
                            cutoffTime = null,
                            excludedPaths = excludePathResolver.normalizedConfiguredExcludePaths(),
                        )
                    }

                    return@forEach
                }

                if (discoveredDatabase.tables.mapNotNull { it.location }.isEmpty()) {
                    logger.warn(
                        "Skipping cleanup because database has no active tables with discoverable locations in the catalog. " +
                                "Cleanup requires at least one active table location to anchor the safety check. " +
                                "catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}, totalCatalogTables=${discoveredDatabase.tables.size}"
                    )

                    recordAudit(database) {
                        cleanupAuditRecorder.recordNoActiveTables(
                            runId = runId,
                            databaseStartTime = databaseStartTime,
                            catalogName = discoveredDatabase.catalog,
                            databaseName = discoveredDatabase.database,
                            discoveredDatabaseLocation = discoveredDatabase.location,
                            excludedPaths = excludePathResolver.normalizedConfiguredExcludePaths(),
                        )
                    }

                    return@forEach
                }

                if (discoveredDatabase.location.isNullOrBlank()) {
                    logger.warn(
                        "Skipping storage folder discovery because database location is missing for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                    )

                    recordAudit(database) {
                        cleanupAuditRecorder.recordDatabaseLocationMissing(
                            runId = runId,
                            databaseStartTime = databaseStartTime,
                            catalogName = discoveredDatabase.catalog,
                            databaseName = discoveredDatabase.database,
                            discoveredDatabaseLocation = discoveredDatabase.location,
                            errorMessage = "Database location is missing; storage folder discovery was skipped.",
                            excludedPaths = excludePathResolver.normalizedConfiguredExcludePaths(),
                            activeTableCount = discoveredDatabase.tables.size.toLong(),
                            unresolvedTables = unresolvedTableNames,
                            activeTableLocations =
                                discoveredDatabase.tables
                                    .mapNotNull { it.location }
                                    .sorted(),
                        )
                    }
                } else {
                    val sharedWarehouseRoots = catalogDiscoveryService.sharedWarehouseRoots(discoveredDatabase.catalog)
                    val resolvedScanLocation =
                        try {
                            storageScanLocationResolver.resolve(
                                databaseLocation = discoveredDatabase.location,
                                activeTableLocations =
                                    discoveredDatabase.tables.mapNotNull { it.location },
                                sharedWarehouseRoots = sharedWarehouseRoots,
                            )
                        } catch (th: UnsafeScanRootException) {
                            logger.warn(
                                "Refusing to scan storage for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}: reason=${th.reason.code}. ${th.message}"
                            )

                            val reportOnlyPlan =
                                reportOnlyPlanForRefusedDatabase(discoveredDatabase, th.reason, sharedWarehouseRoots)

                            recordAudit(database) {
                                cleanupAuditRecorder.recordUnsafeScanRoot(
                                    runId = runId,
                                    databaseStartTime = databaseStartTime,
                                    catalogName = discoveredDatabase.catalog,
                                    databaseName = discoveredDatabase.database,
                                    discoveredDatabaseLocation = discoveredDatabase.location,
                                    activeTableCount = discoveredDatabase.tables.size.toLong(),
                                    unresolvedTables = unresolvedTableNames,
                                    activeTableLocations =
                                        discoveredDatabase.tables
                                            .mapNotNull { it.location }
                                            .sorted(),
                                    reason = th.reason.code,
                                    errorMessage = th.message ?: th.reason.code,
                                    excludedPaths = excludePathResolver.normalizedConfiguredExcludePaths(),
                                    scanPlan = reportOnlyPlan,
                                )
                            }

                            return@forEach
                        }
                    val activeTableLocations =
                        discoveredDatabase.tables.mapNotNull { it.location }.sorted()

                    val cleanupRoots =
                        storageScanLocationResolver.cleanupRoots(
                            resolvedScanLocation = resolvedScanLocation,
                            databaseLocation = discoveredDatabase.location,
                            activeTableLocations = activeTableLocations,
                            derivedRestNamespaceRoot = { derivedRestNamespaceRoot(discoveredDatabase) },
                        )
                    logger.info(
                        "Using storage scan location=${cleanupRoots.locations.joinToString()} for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}, discoveredDatabaseLocation=${discoveredDatabase.location}, managedRootStatus=${cleanupRoots.managedRootStatus}"
                    )

                    val cleanupRootListings = listCleanupRoots(discoveredDatabase.catalog, cleanupRoots.locations)
                    val listedRoots = cleanupRootListings.filter { it.outcome != ScanRootOutcome.LISTING_FAILED }
                    val listedRootLocations = listedRoots.map { it.location }
                    // The audit row keeps one storage_scan_location: the first cleanup root that was listed.
                    val storageScanLocation = listedRootLocations.first()
                    val managedRootStatus =
                        if (cleanupRootListings.any { it.outcome == ScanRootOutcome.LISTING_FAILED && it.location != resolvedScanLocation }) {
                            managedRootRefused(ScanRootOutcome.LISTING_FAILED.code)
                        } else {
                            cleanupRoots.managedRootStatus
                        }

                    // Cleanup roots never overlap, but a folder is still counted and deleted at most once.
                    val storageFolders =
                        listedRoots
                            .flatMap { it.folders }
                            .distinctBy { StoragePathUtils.normalizeLocation(it.path) }
                            .sortedBy { it.path }
                    logger.info(
                        "Discovered ${storageFolders.size} immediate storage folder(s) under storage scan location=${listedRootLocations.joinToString()}"
                    )
                    storageFolders.forEach { folder ->
                        logger.info(
                            "Storage folder discovered: path=${folder.path}"
                        )
                    }

                    val cutoffTime = Instant.now().minus(Duration.ofHours(config.olderThanHours))

                    val cutoffTimeMillis = cutoffTime.toEpochMilli()

                    logger.info(
                        "Applying older_than_hours=${config.olderThanHours}; the newest object in a candidate folder must be at or before $cutoffTime"
                    )

                    val unscannedTableLocations =
                        activeTableLocations.filterNot { tableLocation ->
                            listedRootLocations.any { StoragePathUtils.isSameOrChildLocation(tableLocation, it) }
                        }

                    unscannedTableLocationsForAudit = unscannedTableLocations

                    if (unscannedTableLocations.isNotEmpty()) {
                        logger.warn(
                            "Storage scan covers part of the database only: ${unscannedTableLocations.size} table location(s) are outside storage scan location=${listedRootLocations.joinToString()} for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                        )
                    }

                    val scanPlan =
                        planStorageRoots(
                            discoveredDatabase = discoveredDatabase,
                            cleanupRoots = cleanupRoots.locations,
                            databaseLocation = discoveredDatabase.location,
                            sharedWarehouseRoots = sharedWarehouseRoots,
                            cleanupRootListings =
                                cleanupRootListings.associate { listing ->
                                    StoragePathUtils.normalizeLocation(listing.location) to
                                        CleanupRootListing(
                                            outcome = listing.outcome,
                                            folderCount = listing.folders.size.takeIf { listing.outcome == ScanRootOutcome.SCANNED },
                                        )
                                },
                        ).copy(cleanupRoots = listedRootLocations, managedRootStatus = managedRootStatus)

                    scanPlanForAudit = scanPlan

                    // Without a visible table under it, a cleanup root gives nothing to check ownership against.
                    val cleanupRootHasVisibleTable =
                        listedRootLocations.all { root -> activeTableLocations.any { StoragePathUtils.isSameOrChildLocation(it, root) } }

                    val effectiveExcludedPaths =
                        listedRootLocations
                            .flatMap { root ->
                                excludePathResolver.effectiveExcludedPaths(
                                    database = discoveredDatabase.database,
                                    storageScanLocation = root,
                                )
                            }.distinct()
                            .sorted()

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

                    // Other storage is checked under every listed root that has folders. A check that fails under one
                    // root blocks deletion in all of them.
                    val checkedRoots = listedRoots.filter { it.folders.isNotEmpty() }.map { it.location }

                    // Null when the storage of other databases cannot be established under any cleanup root.
                    val otherDatabaseStoragePerRoot =
                        checkedRoots.map { root ->
                            otherDatabaseStorage(
                                catalog = discoveredDatabase.catalog,
                                database = discoveredDatabase.database,
                                cleanupRoot = root,
                                databaseLocations = otherStorageIndex.databaseLocations(discoveredDatabase.catalog),
                            )
                        }
                    val otherDatabaseStorage =
                        if (otherDatabaseStoragePerRoot.any { it == null }) null else otherDatabaseStoragePerRoot.flatMap { it.orEmpty() }.distinct()
                    otherDatabaseStorage.orEmpty().forEach { location ->
                        logger.warn("Protecting storage of another database under storage scan location=${listedRootLocations.joinToString()}: $location")
                    }

                    // A known location strictly inside a scan root covers at most one listed folder. A broader one would
                    // hide every folder, so it is not used for matching.
                    val (matchableKnownLocations, ignoredKnownLocations) =
                        discoveredDatabase.tables.mapNotNull { it.knownLocation }.partition { knownLocation ->
                            runCatching {
                                listedRootLocations.any { root ->
                                    StoragePathUtils.isSameOrChildLocation(knownLocation, root) &&
                                        StoragePathUtils.normalizeLocation(knownLocation) != StoragePathUtils.normalizeLocation(root)
                                }
                            }.getOrDefault(false)
                        }
                    if (ignoredKnownLocations.isNotEmpty()) {
                        logger.warn(
                            "Not using ${ignoredKnownLocations.size} unresolved table location(s) for folder matching because they are not inside storage scan location=${listedRootLocations.joinToString()}: $ignoredKnownLocations"
                        )
                    }

                    val ownTableStorage =
                        discoveredDatabase.tables.flatMap { listOfNotNull(it.location) + it.writeLocations } + matchableKnownLocations

                    // Reading other tables describes every table the job can see, so it is skipped when no folder is
                    // left for them to protect, or when unknown storage of other databases already blocks deletion.
                    val otherTablesNeeded =
                        otherDatabaseStorage != null &&
                            untrackedFolderCandidateDetector
                                .unmatchedFolders(
                                    storageFolders = storageFolders,
                                    activeTableLocations = ownTableStorage,
                                    excludedPaths = effectiveExcludedPaths + otherDatabaseStorage,
                                    stage = "the check for folders that other tables could use",
                                ).isNotEmpty()
                    val otherTableStorage =
                        if (otherTablesNeeded) {
                            OtherTableStorage.combine(checkedRoots.map { otherTableStorage(discoveredDatabase, it, otherStorageIndex) })
                        } else {
                            OtherTableStorage(paths = emptyList(), unresolvedTables = emptyList())
                        }
                    val firstReadChecks =
                        OwnershipChecks(
                            otherDatabaseFolders = storageFolders.isNotEmpty() && otherDatabaseStorage != null,
                            otherTables = otherTablesNeeded && otherTableStorage.paths != null,
                        )
                    ownershipChecksForAudit = firstReadChecks
                    otherTableStorage.paths.orEmpty().forEach { location ->
                        logger.warn("Protecting storage of another table or catalog under storage scan location=${listedRootLocations.joinToString()}: $location")
                    }
                    val protectedPaths = effectiveExcludedPaths + otherDatabaseStorage.orEmpty() + otherTableStorage.paths.orEmpty()

                    val detectedReconciliation =
                        try {
                            untrackedFolderCandidateDetector.detectCandidates(
                                storageFolders = storageFolders,
                                activeTableLocations = ownTableStorage,
                                excludedPaths = protectedPaths,
                                maxCandidateFolders = config.maxCandidateFoldersPerDatabase,
                                unresolvedTableCount = unresolvedTableNames.size,
                            )
                        } catch (th: TooManyCandidateFoldersException) {
                            val unresolvedOwnershipNote =
                                "${unresolvedTableNames.size} catalog table(s) also have unresolved storage ownership. Raising the limit will not make these folders deletable."
                                    .takeIf { unresolvedTableNames.isNotEmpty() }

                            logger.warn(
                                "Refusing to continue for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}. ${unresolvedOwnershipNote ?: "Narrow the scope or increase the limit explicitly."}",
                                th,
                            )
                            val candidateFolderPaths = th.candidateFolderPaths.sorted()

                            cleanupSummaryLogger.logTooManyFoldersReport(
                                TooManyFoldersSummary(
                                    catalog = discoveredDatabase.catalog,
                                    database = discoveredDatabase.database,
                                    discoveredDatabaseLocation = discoveredDatabase.location,
                                    storageScanLocation = storageScanLocation,
                                    cleanupRoots = listedRootLocations,
                                    catalogTableCount = discoveredDatabase.tables.size,
                                    verifiedTableCount = activeTableLocations.size,
                                    unresolvedTableNames = unresolvedTableNames,
                                    storageFolderCount = storageFolderPaths.size,
                                    unmatchedFolderPaths = candidateFolderPaths,
                                    maxCandidateFolders = config.maxCandidateFoldersPerDatabase,
                                    dryRun = config.dryRun,
                                    unresolvedTableDetails = unresolvedTableDetails,
                                    ownershipChecks = firstReadChecks,
                                )
                            )

                            recordAudit(database) {
                                cleanupAuditRecorder.recordTooManyCandidateFolders(
                                    runId = runId,
                                    databaseStartTime = databaseStartTime,
                                    catalogName = discoveredDatabase.catalog,
                                    databaseName = discoveredDatabase.database,
                                    discoveredDatabaseLocation = discoveredDatabase.location,
                                    storageScanLocation = storageScanLocation,
                                    activeTableCount = discoveredDatabase.tables.size.toLong(),
                                    unresolvedTables = unresolvedTableNames,
                                    activeTableLocations = activeTableLocations,
                                    storageFolderPaths = storageFolderPaths,
                                    candidateFolderPaths = candidateFolderPaths,
                                    cutoffTime = cutoffTime,
                                    excludedPaths = effectiveExcludedPaths,
                                    errorMessage = listOfNotNull(th.message ?: th::class.java.name, unresolvedOwnershipNote).joinToString(". "),
                                    unscannedTableLocations = unscannedTableLocations,
                                    scanPlan = scanPlan,
                                    ownershipChecks = firstReadChecks,
                                )
                            }

                            return@forEach
                        }

                    val detectedFolderPaths = detectedReconciliation.folders.map { it.path }.sorted()
                    val candidateFolderStatistics =
                        candidateSizeStatCollector.collectPerFolder(
                            catalog = discoveredDatabase.catalog,
                            candidateFolderPaths = detectedFolderPaths,
                        )
                    val candidateSizeStatsByFolder = candidateFolderStatistics.statsByFolder
                    val inspectionOutcomes = candidateFolderStatistics.folderPathsByOutcome(detectedFolderPaths)

                    val reconciliation =
                        untrackedFolderCandidateDetector.applyAgeCutoff(
                            reconciliation = detectedReconciliation,
                            folderStatistics = candidateFolderStatistics,
                            cutoffTimeMillis = cutoffTimeMillis,
                        )

                    val unmatchedFolders = reconciliation.folders
                    logger.info(
                        "Detected ${unmatchedFolders.size} storage folder(s) with no verified catalog owner for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}"
                    )

                    val unmatchedFolderPaths = unmatchedFolders.map { it.path }.sorted()
                    val candidateSizeStats =
                        candidateSizeStatCollector.sum(unmatchedFolderPaths.mapNotNull { candidateSizeStatsByFolder[it] })

                    unmatchedFolders.forEach { folder ->
                        logger.info("Storage folder with no verified catalog owner: path=${folder.path}")
                    }

                    val firstBlockReason =
                        when {
                            reconciliation !is StorageFolderReconciliation.DeletionEligible -> null
                            auditFailure != null -> DeletionBlockReason.AUDIT_INTEGRITY_LOST
                            !cleanupRootHasVisibleTable -> DeletionBlockReason.CLEANUP_ROOT_HAS_NO_VISIBLE_TABLE
                            otherDatabaseStorage == null -> DeletionBlockReason.OTHER_DATABASE_STORAGE_UNKNOWN
                            else -> otherTableStorage.blockReason
                        }

                    // The first read of other databases and catalogs is shared by the whole run, so another database can
                    // claim a candidate folder before this one deletes. It is read again, unshared, right before deleting.
                    val finalOtherStorage =
                        if (
                            firstBlockReason == null &&
                            reconciliation is StorageFolderReconciliation.DeletionEligible &&
                            reconciliation.folders.isNotEmpty() &&
                            !config.dryRun
                        ) {
                            finalOtherStorageCheck(
                                discoveredDatabase = discoveredDatabase,
                                cleanupRoots = checkedRoots,
                                firstReadPaths = otherDatabaseStorage.orEmpty() + otherTableStorage.paths.orEmpty(),
                            )
                        } else {
                            null
                        }
                    val deletionBlockReason = firstBlockReason ?: finalOtherStorage?.blockReason
                    val ownershipChecks = firstReadChecks.copy(finalRecheck = finalOtherStorage?.paths != null)
                    ownershipChecksForAudit = ownershipChecks

                    val deletionResult =
                        when (reconciliation) {
                            is StorageFolderReconciliation.DeletionEligible ->
                                if (deletionBlockReason != null) {
                                    val detail =
                                        when (deletionBlockReason) {
                                            DeletionBlockReason.AUDIT_INTEGRITY_LOST -> "an audit write failed earlier in this run."
                                            DeletionBlockReason.CLEANUP_ROOT_HAS_NO_VISIBLE_TABLE ->
                                                "no visible table is stored under storage scan location=${listedRootLocations.joinToString()}."
                                            DeletionBlockReason.OTHER_DATABASE_STORAGE_UNKNOWN ->
                                                "the storage of other databases could not be established."
                                            DeletionBlockReason.OTHER_TABLE_UNRESOLVED ->
                                                "these tables of other databases or catalogs could not be resolved to a storage location: " +
                                                    "${(finalOtherStorage ?: otherTableStorage).unresolvedTables}."
                                        }
                                    val stage = if (finalOtherStorage != null) " at the final check before deletion" else ""
                                    logger.warn(
                                        "Not deleting any folder for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}$stage: $detail"
                                    )
                                    CandidateDeletionResult()
                                } else {
                                    try {
                                        candidateDeletionGate.deleteCandidates(
                                            catalog = discoveredDatabase.catalog,
                                            database = discoveredDatabase.database,
                                            reconciliation = reconciliation,
                                            excludedPaths = protectedPaths + finalOtherStorage?.paths.orEmpty(),
                                        )
                                    } catch (th: PartialDeletionException) {
                                        logger.error(
                                            "Deletion stopped for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}: ${th.deletedPaths.size} folder(s) were deleted before the failure, deletedPaths=${th.deletedPaths}, failedPath=${th.failedPath}",
                                            th,
                                        )

                                        recordAudit(database) {
                                            cleanupAuditRecorder.recordDeletionFailed(
                                                runId = runId,
                                                databaseStartTime = databaseStartTime,
                                                catalogName = discoveredDatabase.catalog,
                                                databaseName = discoveredDatabase.database,
                                                discoveredDatabaseLocation = discoveredDatabase.location,
                                                storageScanLocation = storageScanLocation,
                                                activeTableCount = discoveredDatabase.tables.size.toLong(),
                                                activeTableLocations = activeTableLocations,
                                                storageFolderPaths = storageFolderPaths,
                                                candidateFolderPaths = unmatchedFolderPaths,
                                                candidateSizeStats = candidateSizeStats,
                                                deletedFolderPaths = th.deletedPaths,
                                                deletedSizeStats =
                                                    candidateSizeStatCollector.sum(
                                                        th.deletedPaths.mapNotNull { candidateSizeStatsByFolder[it] }
                                                    ),
                                                failedFolderPath = th.failedPath,
                                                cutoffTime = cutoffTime,
                                                excludedPaths = effectiveExcludedPaths,
                                                unscannedTableLocations = unscannedTableLocations,
                                                scanPlan = scanPlan,
                                                inspectionOutcomes = inspectionOutcomes,
                                                notFoundAtDeletionPaths = th.notFoundPaths,
                                                error = th,
                                                ownershipChecks = ownershipChecks,
                                            )
                                        }

                                        return@forEach
                                    }
                                }

                            is StorageFolderReconciliation.OwnershipUnverified -> CandidateDeletionResult()
                        }

                    val deletedFolders = deletionResult.deletedPaths

                    // No candidate overlapped the first read, so an overlap here is storage claimed since then. The
                    // deletion gate skips these folders and logs the owner it found.
                    val keptAtFinalCheckPaths =
                        if (deletionBlockReason == null) {
                            val finalPaths = finalOtherStorage?.paths.orEmpty()
                            unmatchedFolderPaths.filter { folder -> finalPaths.any { StoragePathUtils.overlaps(folder, it) } }
                        } else {
                            emptyList()
                        }

                    val deletedSizeStats =
                        candidateSizeStatCollector.sum(deletedFolders.mapNotNull { candidateSizeStatsByFolder[it] })

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
                            cleanupRoots = listedRootLocations,
                            catalogTableCount = discoveredDatabase.tables.size,
                            unresolvedTableNames = unresolvedTableNames,
                            activeTableLocations = activeTableLocations,
                            storageFolderPaths = storageFolderPaths,
                            excludedPaths = effectiveExcludedPaths,
                            unmatchedFolderPaths = unmatchedFolderPaths,
                            unmatchedSizeStats = candidateSizeStats,
                            dryRun = config.dryRun,
                            deletionEligible = reconciliation is StorageFolderReconciliation.DeletionEligible,
                            deletionBlockReason = deletionBlockReason,
                            deletedFolderPaths = deletedFolders,
                            deletedSizeStats = deletedSizeStats,
                            unscannedTableLocations = unscannedTableLocations,
                            scanPlan = scanPlan,
                            inspectionOutcomes = inspectionOutcomes,
                            notFoundAtDeletionPaths = deletionResult.notFoundPaths,
                            unresolvedTableDetails = unresolvedTableDetails,
                            keptAtFinalCheckPaths = keptAtFinalCheckPaths,
                            ownershipChecks = ownershipChecks,
                        )
                    )

                    when (reconciliation) {
                        is StorageFolderReconciliation.DeletionEligible ->
                            if (deletionBlockReason != null) {
                                recordAudit(database) {
                                    cleanupAuditRecorder.recordDeletionBlocked(
                                        reason = deletionBlockReason,
                                        runId = runId,
                                        databaseStartTime = databaseStartTime,
                                        catalogName = discoveredDatabase.catalog,
                                        databaseName = discoveredDatabase.database,
                                        discoveredDatabaseLocation = discoveredDatabase.location,
                                        storageScanLocation = storageScanLocation,
                                        activeTableCount = discoveredDatabase.tables.size.toLong(),
                                        activeTableLocations = activeTableLocations,
                                        storageFolderPaths = storageFolderPaths,
                                        candidateFolderPaths = unmatchedFolderPaths,
                                        candidateSizeStats = candidateSizeStats,
                                        cutoffTime = cutoffTime,
                                        excludedPaths = effectiveExcludedPaths,
                                        unscannedTableLocations = unscannedTableLocations,
                                        scanPlan = scanPlan,
                                        inspectionOutcomes = inspectionOutcomes,
                                        ownershipChecks = ownershipChecks,
                                    )
                                }
                            } else {
                                recordAudit(database) {
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
                                        candidateFolderPaths = unmatchedFolderPaths,
                                        candidateSizeStats = candidateSizeStats,
                                        deletedFolderPaths = deletedFolders,
                                        deletedSizeStats = deletedSizeStats,
                                        cutoffTime = cutoffTime,
                                        excludedPaths = effectiveExcludedPaths,
                                        unscannedTableLocations = unscannedTableLocations,
                                        scanPlan = scanPlan,
                                        inspectionOutcomes = inspectionOutcomes,
                                        notFoundAtDeletionPaths = deletionResult.notFoundPaths,
                                        keptAtFinalCheckPaths = keptAtFinalCheckPaths,
                                        ownershipChecks = ownershipChecks,
                                    )
                                }
                            }

                        is StorageFolderReconciliation.OwnershipUnverified ->
                            recordAudit(database) {
                                cleanupAuditRecorder.recordOwnershipUnverified(
                                    runId = runId,
                                    databaseStartTime = databaseStartTime,
                                    catalogName = discoveredDatabase.catalog,
                                    databaseName = discoveredDatabase.database,
                                    discoveredDatabaseLocation = discoveredDatabase.location,
                                    storageScanLocation = storageScanLocation,
                                    activeTableCount = discoveredDatabase.tables.size.toLong(),
                                    unresolvedTables = unresolvedTableNames,
                                    activeTableLocations = activeTableLocations,
                                    storageFolderPaths = storageFolderPaths,
                                    potentiallyUntrackedFolderPaths = unmatchedFolderPaths,
                                    cutoffTime = cutoffTime,
                                    excludedPaths = effectiveExcludedPaths,
                                    unscannedTableLocations = unscannedTableLocations,
                                    scanPlan = scanPlan,
                                    inspectionOutcomes = inspectionOutcomes,
                                    ownershipChecks = ownershipChecks,
                                )
                            }
                    }
                }
            } catch (th: DatabaseNotFoundException) {
                logger.warn(
                    "Configured database was not found; skipping cleanup for catalog=${config.catalog}, database=$database"
                )
                logger.debug(
                    "Configured database was not found details for catalog=${config.catalog}, database=$database",
                    th,
                )

                recordAudit(database) {
                    cleanupAuditRecorder.recordDatabaseNotFound(
                        runId = runId,
                        database = database,
                        databaseStartTime = databaseStartTime,
                        error = th,
                        ownershipChecks = ownershipChecksForAudit,
                    )
                }
            } catch (th: Throwable) {
                logger.error("Cleanup failed for catalog=${config.catalog}, database=$database", th)

                recordAudit(database) {
                    cleanupAuditRecorder.recordFailed(
                        runId = runId,
                        database = database,
                        databaseStartTime = databaseStartTime,
                        excludedPaths = effectiveExcludedPathsForAudit,
                        unscannedTableLocations = unscannedTableLocationsForAudit,
                        scanPlan = scanPlanForAudit,
                        unresolvedTables = unresolvedTablesForAudit,
                        error = th,
                        ownershipChecks = ownershipChecksForAudit,
                    )
                }
            }
        }

        auditFailure?.let { throw AuditIntegrityLostException(it) }

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

    // A database's location and REST namespace folder are where its tables go by default, not claims on
    // everything below them. Inside the cleanup root they are protected whole. Above it, only the table that Hive
    // placement would put on the root's path matters; REST folders add a unique suffix, so that table cannot be
    // found by name and the storage is unknown. database is this job's database in that catalog, or null for another
    // catalog.
    private fun otherDatabaseStorage(
        catalog: String,
        database: String?,
        cleanupRoot: String,
        databaseLocations: Result<Map<String, String?>>,
    ): List<String>? {
        try {
            val root = StoragePathUtils.normalizeLocation(cleanupRoot)
            val locations = databaseLocations.getOrThrow()
            val otherDatabases = otherDatabases(locations.keys, database) ?: return null

            return locations
                .filterKeys { it in otherDatabases }
                .flatMap { (otherDatabase, location) ->
                    if (location == null || !StoragePathUtils.isQualifiedLocation(location)) {
                        return null
                    }
                    val hiveRoot = StoragePathUtils.normalizeLocation(location)
                    val restNamespaceRoot = catalogDiscoveryService.restNamespaceRoot(catalog, otherDatabase)
                    // The managed REST catalog stores this database under <warehouse>/<database>. With no warehouse in
                    // the session, that folder is unknown and could be the cleanup root.
                    if (restNamespaceRoot?.status == "no_warehouse") {
                        logger.warn(
                            "Catalog=$catalog is a managed REST catalog with no warehouse in the Spark session, so the folder of database=$otherDatabase is unknown"
                        )
                        return null
                    }
                    val restRoot = restNamespaceRoot?.location?.let(StoragePathUtils::normalizeLocation)

                    listOfNotNull(hiveRoot, restRoot).flatMap { storage ->
                        when {
                            StoragePathUtils.isSameOrChildLocation(storage, root) -> listOf(storage)
                            !StoragePathUtils.isSameOrChildLocation(root, storage) -> emptyList()
                            storage == restRoot -> return null
                            else -> {
                                val folderOnRootPath = root.removePrefix("$storage/").substringBefore('/')
                                val tableLocation =
                                    catalogDiscoveryService.tableLocationIfExists(catalog, otherDatabase, folderOnRootPath)
                                if (tableLocation != null && !StoragePathUtils.isQualifiedLocation(tableLocation)) {
                                    return null
                                }
                                listOfNotNull(tableLocation)
                            }
                        }
                    }
                }
        } catch (error: Exception) {
            logger.warn("Could not establish the storage of other databases for catalog=$catalog, cleanup root=$cleanupRoot", error)
            return null
        }
    }

    // The listing shows how the catalog treats letter case: an exact name is this database; otherwise one entry that
    // differs only in case is this database as the catalog normalized it. More than one is ambiguous, which gives null.
    private fun otherDatabases(
        databases: Set<String>,
        database: String?,
    ): Set<String>? {
        if (database == null) {
            return databases
        }
        val sameName = databases.filter { it.equals(database, ignoreCase = true) }
        val ownName = sameName.firstOrNull { it == database } ?: sameName.singleOrNull()
        if (ownName == null && sameName.isNotEmpty()) {
            return null
        }
        return databases - setOfNotNull(ownName)
    }

    // Any table of another database, or of another session catalog with a warehouse in the cleanup root's bucket, can
    // be stored in the root after a rename, a register or an explicit location. A warehouse without a bucket, or no
    // warehouse in the session, could be that bucket, so those catalogs are read too. A table with unknown storage
    // could be anywhere. Tables hidden from the job and catalogs outside its session are not seen.
    private fun otherTableStorage(
        discoveredDatabase: DiscoveredDatabase,
        cleanupRoot: String,
        index: OtherStorageIndex,
    ): OtherTableStorage {
        val startedAt = System.nanoTime()
        try {
            val root = StoragePathUtils.normalizeLocation(cleanupRoot)
            val otherCatalogs =
                catalogDiscoveryService.sessionCatalogWarehouses().filter { (catalog, warehouses) ->
                    catalog != discoveredDatabase.catalog &&
                        (warehouses.isEmpty() || warehouses.any { !StoragePathUtils.isQualifiedLocation(it) || StoragePathUtils.isSameBucket(it, root) })
                }

            val paths = mutableListOf<String>()
            otherCatalogs.forEach { (catalog, warehouses) ->
                paths += warehouses.filter { StoragePathUtils.isSameOrChildLocation(it, root) }.map(StoragePathUtils::normalizeLocation)
                paths += otherDatabaseStorage(catalog, null, root, index.databaseLocations(catalog)) ?: return OtherTableStorage(null, emptyList())
            }

            val tables =
                (mapOf(discoveredDatabase.catalog to discoveredDatabase.database) + otherCatalogs.mapValues { null })
                    .flatMap { (catalog, database) ->
                        val databases = otherDatabases(index.databaseLocations(catalog).getOrThrow().keys, database)
                            ?: return OtherTableStorage(null, emptyList())
                        databases.flatMap { index.tables(catalog, it).getOrThrow() }
                    }
            paths += tables.flatMap { listOfNotNull(it.location) + it.writeLocations }.filter { StoragePathUtils.overlaps(it, root) }

            logger.info(
                "Checked ${tables.size} table(s) of other databases and ${otherCatalogs.size} other catalog(s) for cleanup root=$cleanupRoot " +
                    "in ${(System.nanoTime() - startedAt) / 1_000_000} ms"
            )
            return OtherTableStorage(paths.distinct().sorted(), tables.filter { it.isUnresolved }.map { it.qualifiedName }.sorted())
        } catch (error: Exception) {
            logger.warn("Could not read the tables of other databases for catalog=${discoveredDatabase.catalog}, cleanup root=$cleanupRoot", error)
            return OtherTableStorage(null, emptyList())
        }
    }

    // The same checks as the first read under every cleanup root, with nothing reused from it. Paths are null when any
    // of it could not be read under any root.
    private fun finalOtherStorageCheck(
        discoveredDatabase: DiscoveredDatabase,
        cleanupRoots: List<String>,
        firstReadPaths: List<String>,
    ): OtherTableStorage {
        val index = OtherStorageIndex(catalogDiscoveryService)
        val storage =
            OtherTableStorage.combine(
                cleanupRoots.map { cleanupRoot ->
                    val databaseStorage =
                        otherDatabaseStorage(
                            catalog = discoveredDatabase.catalog,
                            database = discoveredDatabase.database,
                            cleanupRoot = cleanupRoot,
                            databaseLocations = index.databaseLocations(discoveredDatabase.catalog),
                        ) ?: return OtherTableStorage(null, emptyList())
                    val tableStorage = otherTableStorage(discoveredDatabase, cleanupRoot, index)
                    tableStorage.copy(paths = tableStorage.paths?.let { databaseStorage + it })
                },
            )

        (storage.paths.orEmpty() - firstReadPaths.toSet()).forEach { location ->
            logger.warn("Protecting storage found at the final check before deletion, under storage scan location=${cleanupRoots.joinToString()}: $location")
        }
        return storage
    }

    // One cleanup root keeps the strict listing, where a missing or unreadable root fails the database. With two, a
    // root that cannot be listed is left out and the other is still cleaned. Only when none can be listed does the
    // database fail.
    private fun listCleanupRoots(
        catalog: String,
        roots: List<String>,
    ): List<CleanupRootFolders> {
        roots.singleOrNull()?.let { root ->
            return listOf(CleanupRootFolders(root, ScanRootOutcome.SCANNED, objectStorageDiscoveryService.listImmediateChildFolders(catalog, root)))
        }

        val failures = mutableListOf<Exception>()
        val listings =
            roots.map { root ->
                try {
                    objectStorageDiscoveryService.listImmediateChildFoldersIfPresent(catalog, root)
                        ?.let { CleanupRootFolders(root, ScanRootOutcome.SCANNED, it) }
                        ?: CleanupRootFolders(root, ScanRootOutcome.ABSENT, emptyList())
                } catch (error: Exception) {
                    logger.warn("Could not list cleanup root for catalog=$catalog, root=$root. Nothing under it is deleted in this run.", error)
                    failures += error
                    CleanupRootFolders(root, ScanRootOutcome.LISTING_FAILED, emptyList())
                }
            }

        if (failures.size == roots.size) {
            throw failures.first().also { first -> failures.drop(1).forEach(first::addSuppressed) }
        }
        return listings
    }

    // Only a folder this catalog derives for the database can make the folder without .db a cleanup root.
    private fun derivedRestNamespaceRoot(discoveredDatabase: DiscoveredDatabase): String? =
        try {
            catalogDiscoveryService
                .restNamespaceRoot(discoveredDatabase.catalog, discoveredDatabase.database)
                ?.takeIf { it.status == "derived" }
                ?.location
                ?.takeIf { it.isNotBlank() }
        } catch (error: Exception) {
            logger.warn(
                "Could not derive the REST namespace root for catalog=${discoveredDatabase.catalog}, database=${discoveredDatabase.database}, so it is not used as a cleanup root",
                error,
            )
            null
        }

    // Report-only planning must never change deletion, so a failure here is only reported.
    private fun planStorageRoots(
        discoveredDatabase: DiscoveredDatabase,
        cleanupRoots: List<String>,
        databaseLocation: String?,
        sharedWarehouseRoots: List<String>,
        cleanupRootListings: Map<String, CleanupRootListing> = emptyMap(),
    ): ScanPlanReport {
        val catalog = discoveredDatabase.catalog
        val database = discoveredDatabase.database
        val activeTableLocations = discoveredDatabase.tables.mapNotNull { it.location }.sorted()

        return try {
            val restRoot = catalogDiscoveryService.restNamespaceRoot(catalog, database)
            if (restRoot != null && restRoot.location == null) {
                logger.warn(
                    "REST namespace root was not derived for catalog=$catalog, database=$database: ${restRoot.status}. Only visible table locations cover that catalog's storage."
                )
            }

            val roots =
                storageScanLocationPlanner.plan(
                    cleanupRoots = cleanupRoots,
                    databaseLocation = databaseLocation,
                    restNamespaceRoot = restRoot?.location?.takeIf { it.isNotBlank() },
                    approvedRoots = config.reportOnlyStorageRoots[database].orEmpty(),
                    activeTableLocations = activeTableLocations,
                    warehouseRoots = (sharedWarehouseRoots + catalogDiscoveryService.knownWarehouseRoots()).distinct(),
                )

            reportOnlyRootScanner
                .scan(
                    catalog = catalog,
                    database = database,
                    roots = roots,
                    activeTableLocations = activeTableLocations,
                    cleanupRootListings = cleanupRootListings,
                )
                .copy(
                    restNamespaceRootStatus = restRoot?.status,
                    unresolvedTableCount = discoveredDatabase.unresolvedTables.size,
                )
        } catch (error: Exception) {
            logger.warn("Could not plan report-only storage roots for catalog=$catalog, database=$database", error)
            ScanPlanReport(roots = emptyList(), uncoveredTableLocations = emptyList(), planningError = error.toString())
        }
    }

    // A refused database keeps its SKIPPED row; report-only roots are added only when their scope can still be checked.
    private fun reportOnlyPlanForRefusedDatabase(
        discoveredDatabase: DiscoveredDatabase,
        reason: UnsafeScanRootReason,
        sharedWarehouseRoots: List<String>,
    ): ScanPlanReport? {
        val databaseLocation =
            when (reason) {
                UnsafeScanRootReason.SCAN_ROOT_OUTSIDE_DATABASE_BOUNDARY -> discoveredDatabase.location
                UnsafeScanRootReason.DATABASE_LOCATION_IS_BUCKET_ROOT,
                UnsafeScanRootReason.DATABASE_LOCATION_CONTAINS_WAREHOUSE_ROOT,
                -> null
                UnsafeScanRootReason.CATALOG_WAREHOUSE_UNKNOWN,
                UnsafeScanRootReason.INVALID_STORAGE_LOCATION,
                -> return null
            }

        return planStorageRoots(
            discoveredDatabase = discoveredDatabase,
            cleanupRoots = emptyList(),
            databaseLocation = databaseLocation,
            sharedWarehouseRoots = sharedWarehouseRoots,
        ).also { cleanupSummaryLogger.logScanPlan(discoveredDatabase.catalog, discoveredDatabase.database, it) }
    }
}

// paths is null when any of it could not be read.
private data class OtherTableStorage(
    val paths: List<String>?,
    val unresolvedTables: List<String>,
) {
    val blockReason: DeletionBlockReason?
        get() =
            when {
                paths == null -> DeletionBlockReason.OTHER_DATABASE_STORAGE_UNKNOWN
                unresolvedTables.isNotEmpty() -> DeletionBlockReason.OTHER_TABLE_UNRESOLVED
                else -> null
            }

    companion object {
        // One read per cleanup root. A root that could not be read leaves the paths of every root unknown.
        fun combine(perRoot: List<OtherTableStorage>): OtherTableStorage =
            OtherTableStorage(
                paths = if (perRoot.any { it.paths == null }) null else perRoot.flatMap { it.paths.orEmpty() }.distinct().sorted(),
                unresolvedTables = perRoot.flatMap { it.unresolvedTables }.distinct().sorted(),
            )
    }
}

// A complete read is kept and shared by every configured database, because it can describe every visible table.
// A failed read, or one with an unreadable database location or an unresolved table, is not kept, so a brief failure
// does not block the databases processed after it. It still blocks the database that read it.
private class OtherStorageIndex(
    private val catalogDiscoveryService: CatalogDiscoveryService,
) {
    private val databaseLocations = mutableMapOf<String, Map<String, String?>>()
    private val tables = mutableMapOf<Pair<String, String>, List<DiscoveredTable>>()

    fun databaseLocations(catalog: String): Result<Map<String, String?>> =
        databaseLocations[catalog]?.let { Result.success(it) }
            ?: runCatching { catalogDiscoveryService.visibleDatabaseLocations(catalog) }
                .onSuccess { if (null !in it.values) databaseLocations[catalog] = it }

    fun tables(
        catalog: String,
        database: String,
    ): Result<List<DiscoveredTable>> =
        tables[catalog to database]?.let { Result.success(it) }
            ?: runCatching { catalogDiscoveryService.discoverTables(catalog, database) }
                .onSuccess { if (it.none(DiscoveredTable::isUnresolved)) tables[catalog to database] = it }
}

private data class CleanupRootFolders(
    val location: String,
    val outcome: ScanRootOutcome,
    val folders: List<StorageFolder>,
)

class AuditIntegrityLostException(
    cause: Throwable,
) : IllegalStateException(
    "An audit write failed during this run, so no folders were deleted in the databases processed after it.",
    cause,
)
