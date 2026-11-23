package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.candidate.HeldFolders
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.SafeText
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.DeletionFenceReport
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant

@ApplicationScoped
class CleanupAuditRecorder {

    @Inject lateinit var config: ApplicationConfig
    @Inject lateinit var cleanupAuditTableService: CleanupAuditTableService
    @Inject lateinit var auditDiagnosticDetailsBuilder: CleanupAuditDiagnosticDetailsBuilder
    @Inject lateinit var excludePathResolver: ExcludePathResolver

    fun recordDatabaseLocationMissing(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        activeTableCount: Long,
        unresolvedTables: List<String>,
        activeTableLocations: List<String>,
        errorMessage: String,
        excludedPaths: List<String>,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SKIPPED,
            statusReason = "database_location_missing",
            errorMessage = errorMessage,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            activeTableCount = activeTableCount,
            unresolvedTableCount = unresolvedTables.size.toLong(),
            excludedPaths = excludedPaths,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(activeTableLocations = activeTableLocations, unresolvedTables = unresolvedTables),
        )
    }

    fun recordUnsafeScanRoot(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        activeTableCount: Long,
        unresolvedTables: List<String>,
        activeTableLocations: List<String>,
        reason: String,
        errorMessage: String,
        excludedPaths: List<String>,
        scanPlan: ScanPlanReport? = null,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SKIPPED,
            statusReason = reason,
            errorMessage = errorMessage,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            activeTableCount = activeTableCount,
            unresolvedTableCount = unresolvedTables.size.toLong(),
            excludedPaths = excludedPaths,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(activeTableLocations = activeTableLocations, unresolvedTables = unresolvedTables) +
                    auditDiagnosticDetailsBuilder.scanPlanDetails(scanPlan),
        )
    }

    fun recordNoActiveTables(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        excludedPaths: List<String>,
        activeTableCount: Long,
        activeTableLocations: List<String>,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SKIPPED,
            statusReason = "no_active_tables_in_database",
            errorMessage = "Database has no active table that stores files at a known location; cleanup skipped to avoid deleting an unanchored storage root.",
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            activeTableCount = activeTableCount,
            excludedPaths = excludedPaths,
            diagnosticDetails = auditDiagnosticDetailsBuilder.build(activeTableLocations = activeTableLocations),
        )
    }

    fun recordOwnershipUnverified(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        storageScanLocation: String,
        activeTableCount: Long,
        unresolvedTables: List<String>,
        activeTableLocations: List<String>,
        storageFolderPaths: List<String>,
        potentiallyUntrackedFolderPaths: List<String>,
        cutoffTime: Instant?,
        excludedPaths: List<String>,
        unscannedTableLocations: List<String> = emptyList(),
        scanPlan: ScanPlanReport? = null,
        inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>> = emptyMap(),
        ownershipChecks: OwnershipChecks = OwnershipChecks(),
        wouldBeHeldByDeletionFloorPaths: List<String> = emptyList(),
        tooNewPaths: List<String> = emptyList(),
        storage: StorageEvidence = StorageEvidence.NONE,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_BLOCKED,
            statusReason = "unresolved_catalog_ownership",
            errorMessage = null,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            storageScanLocation = storageScanLocation,
            activeTableCount = activeTableCount,
            unresolvedTableCount = unresolvedTables.size.toLong(),
            potentiallyUntrackedFolderCount = potentiallyUntrackedFolderPaths.size.toLong(),
            storageFolderCount = storageFolderPaths.size.toLong(),
            cutoffTime = cutoffTime,
            excludedPaths = excludedPaths,
            ownershipChecks = ownershipChecks,
            storage = storage,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(
                    activeTableLocations = activeTableLocations,
                    storageFolderPaths = storageFolderPaths,
                    unresolvedTables = unresolvedTables,
                    potentiallyUntrackedFolderPaths = potentiallyUntrackedFolderPaths,
                    unscannedTableLocations = unscannedTableLocations,
                ) + auditDiagnosticDetailsBuilder.scanPlanDetails(scanPlan) +
                    auditDiagnosticDetailsBuilder.inspectionOutcomeDetails(inspectionOutcomes) +
                    holdDetails(inspectionOutcomes, tooNewPaths, DeletionFenceReport(), storage) +
                    auditDiagnosticDetailsBuilder.wouldBeHeldByDeletionFloorDetails(wouldBeHeldByDeletionFloorPaths),
        )
    }

    fun recordTooManyCandidateFolders(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        storageScanLocation: String,
        activeTableCount: Long,
        unresolvedTables: List<String>,
        activeTableLocations: List<String>,
        storageFolderPaths: List<String>,
        candidateFolderPaths: List<String>,
        cutoffTime: Instant,
        excludedPaths: List<String>,
        errorMessage: String,
        unscannedTableLocations: List<String> = emptyList(),
        scanPlan: ScanPlanReport? = null,
        ownershipChecks: OwnershipChecks = OwnershipChecks(),
    ) {
        // With unresolved tables no folder is a confirmed candidate, but the count that exceeded the limit stays visible.
        val (confirmedFolderPaths, unverifiedFolderPaths) =
            if (unresolvedTables.isEmpty()) {
                candidateFolderPaths to emptyList()
            } else {
                emptyList<String>() to candidateFolderPaths
            }

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
            unresolvedTableCount = unresolvedTables.size.toLong(),
            potentiallyUntrackedFolderCount = unverifiedFolderPaths.size.toLong(),
            storageFolderCount = storageFolderPaths.size.toLong(),
            candidateFolderCount = confirmedFolderPaths.size.toLong(),
            candidateFolders = confirmedFolderPaths.take(MAX_AUDIT_PATH_SAMPLE_SIZE),
            cutoffTime = cutoffTime,
            excludedPaths = excludedPaths,
            ownershipChecks = ownershipChecks,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(
                    activeTableLocations = activeTableLocations,
                    storageFolderPaths = storageFolderPaths,
                    candidateFolderPaths = confirmedFolderPaths,
                    unresolvedTables = unresolvedTables,
                    potentiallyUntrackedFolderPaths = unverifiedFolderPaths,
                    unscannedTableLocations = unscannedTableLocations,
                ) + auditDiagnosticDetailsBuilder.scanPlanDetails(scanPlan),
        )
    }

    fun recordSuccess(
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
        unscannedTableLocations: List<String> = emptyList(),
        scanPlan: ScanPlanReport? = null,
        inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>> = emptyMap(),
        notFoundAtDeletionPaths: List<String> = emptyList(),
        keptAtFinalCheckPaths: List<String> = emptyList(),
        ownershipChecks: OwnershipChecks = OwnershipChecks(),
        wouldBeHeldByDeletionFloorPaths: List<String> = emptyList(),
        deletionFence: DeletionFenceReport = DeletionFenceReport(),
        tooNewPaths: List<String> = emptyList(),
        storage: StorageEvidence = StorageEvidence.NONE,
    ) {
        val candidateFolderPathSet = candidateFolderPaths.toSet()

        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SUCCESS,
            statusReason = if (unscannedTableLocations.isEmpty()) null else "partial_scan_coverage",
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
            ownershipChecks = ownershipChecks,
            storage = storage,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(
                    activeTableLocations = activeTableLocations,
                    storageFolderPaths = storageFolderPaths,
                    candidateFolderPaths = candidateFolderPaths,
                    nonCandidateStorageFolderPaths =
                        storageFolderPaths.filter { it !in candidateFolderPathSet }.sorted(),
                    unscannedTableLocations = unscannedTableLocations,
                ) + auditDiagnosticDetailsBuilder.scanPlanDetails(scanPlan) +
                    auditDiagnosticDetailsBuilder.inspectionOutcomeDetails(inspectionOutcomes) +
                    auditDiagnosticDetailsBuilder.notFoundAtDeletionDetails(notFoundAtDeletionPaths) +
                    auditDiagnosticDetailsBuilder.keptAtFinalCheckDetails(keptAtFinalCheckPaths) +
                    auditDiagnosticDetailsBuilder.wouldBeHeldByDeletionFloorDetails(wouldBeHeldByDeletionFloorPaths) +
                    auditDiagnosticDetailsBuilder.deletionFenceDetails(deletionFence) +
                    holdDetails(inspectionOutcomes, tooNewPaths, deletionFence, storage),
        )
    }

    fun recordDeletionFailed(
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
        failedFolderPath: String?,
        cutoffTime: Instant,
        excludedPaths: List<String>,
        unscannedTableLocations: List<String>,
        scanPlan: ScanPlanReport? = null,
        inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>> = emptyMap(),
        notFoundAtDeletionPaths: List<String> = emptyList(),
        error: Throwable,
        ownershipChecks: OwnershipChecks = OwnershipChecks(),
        deletionFence: DeletionFenceReport = DeletionFenceReport(),
        tooNewPaths: List<String> = emptyList(),
        storage: StorageEvidence = StorageEvidence.NONE,
    ) {
        writeDeletionEndedEarly(
            statusReason = "deletion_failed",
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            storageScanLocation = storageScanLocation,
            activeTableCount = activeTableCount,
            activeTableLocations = activeTableLocations,
            storageFolderPaths = storageFolderPaths,
            candidateFolderPaths = candidateFolderPaths,
            candidateSizeStats = candidateSizeStats,
            deletedFolderPaths = deletedFolderPaths,
            deletedSizeStats = deletedSizeStats,
            failedFolderPath = failedFolderPath,
            cutoffTime = cutoffTime,
            excludedPaths = excludedPaths,
            unscannedTableLocations = unscannedTableLocations,
            scanPlan = scanPlan,
            inspectionOutcomes = inspectionOutcomes,
            notFoundAtDeletionPaths = notFoundAtDeletionPaths,
            error = error,
            ownershipChecks = ownershipChecks,
            deletionFence = deletionFence,
            tooNewPaths = tooNewPaths,
            storage = storage,
        )
    }

    // A safety recheck stopped deletion after folders, or objects of one folder, were already deleted.
    fun recordDeletionStopped(
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
        unscannedTableLocations: List<String>,
        scanPlan: ScanPlanReport?,
        inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>>,
        notFoundAtDeletionPaths: List<String>,
        error: Throwable,
        ownershipChecks: OwnershipChecks,
        deletionFence: DeletionFenceReport,
        tooNewPaths: List<String>,
        storage: StorageEvidence = StorageEvidence.NONE,
    ) {
        writeDeletionEndedEarly(
            statusReason = "deletion_stopped_at_recheck",
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            storageScanLocation = storageScanLocation,
            activeTableCount = activeTableCount,
            activeTableLocations = activeTableLocations,
            storageFolderPaths = storageFolderPaths,
            candidateFolderPaths = candidateFolderPaths,
            candidateSizeStats = candidateSizeStats,
            deletedFolderPaths = deletedFolderPaths,
            deletedSizeStats = deletedSizeStats,
            failedFolderPath = null,
            cutoffTime = cutoffTime,
            excludedPaths = excludedPaths,
            unscannedTableLocations = unscannedTableLocations,
            scanPlan = scanPlan,
            inspectionOutcomes = inspectionOutcomes,
            notFoundAtDeletionPaths = notFoundAtDeletionPaths,
            error = error,
            ownershipChecks = ownershipChecks,
            deletionFence = deletionFence,
            tooNewPaths = tooNewPaths,
            storage = storage,
        )
    }

    private fun writeDeletionEndedEarly(
        statusReason: String,
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
        failedFolderPath: String?,
        cutoffTime: Instant,
        excludedPaths: List<String>,
        unscannedTableLocations: List<String>,
        scanPlan: ScanPlanReport?,
        inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>>,
        notFoundAtDeletionPaths: List<String>,
        error: Throwable,
        ownershipChecks: OwnershipChecks,
        deletionFence: DeletionFenceReport,
        tooNewPaths: List<String>,
        storage: StorageEvidence,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_FAILED,
            statusReason = statusReason,
            errorMessage = SafeText.errorMessage(error),
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
            ownershipChecks = ownershipChecks,
            storage = storage,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(
                    activeTableLocations = activeTableLocations,
                    storageFolderPaths = storageFolderPaths,
                    candidateFolderPaths = candidateFolderPaths,
                    unscannedTableLocations = unscannedTableLocations,
                    reportCompleteScanCoverage = true,
                    deletionFailedFolderPath = failedFolderPath,
                ) + auditDiagnosticDetailsBuilder.scanPlanDetails(scanPlan) +
                    auditDiagnosticDetailsBuilder.inspectionOutcomeDetails(inspectionOutcomes) +
                    auditDiagnosticDetailsBuilder.notFoundAtDeletionDetails(notFoundAtDeletionPaths) +
                    auditDiagnosticDetailsBuilder.deletionFenceDetails(deletionFence) +
                    holdDetails(inspectionOutcomes, tooNewPaths, deletionFence, storage),
        )
    }

    fun recordDeletionBlocked(
        reason: DeletionBlockReason,
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
        cutoffTime: Instant,
        excludedPaths: List<String>,
        unscannedTableLocations: List<String>,
        scanPlan: ScanPlanReport?,
        inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>> = emptyMap(),
        ownershipChecks: OwnershipChecks = OwnershipChecks(),
        wouldBeHeldByDeletionFloorPaths: List<String> = emptyList(),
        deletionFence: DeletionFenceReport = DeletionFenceReport(),
        tooNewPaths: List<String> = emptyList(),
        storage: StorageEvidence = StorageEvidence.NONE,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_BLOCKED,
            statusReason = reason.code,
            errorMessage = null,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            storageScanLocation = storageScanLocation,
            activeTableCount = activeTableCount,
            storageFolderCount = storageFolderPaths.size.toLong(),
            candidateFolderCount = candidateFolderPaths.size.toLong(),
            candidateObjectCount = candidateSizeStats?.objectCount,
            candidateTotalSizeBytes = candidateSizeStats?.totalSizeBytes,
            candidateFolders = candidateFolderPaths,
            cutoffTime = cutoffTime,
            excludedPaths = excludedPaths,
            ownershipChecks = ownershipChecks,
            storage = storage,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(
                    activeTableLocations = activeTableLocations,
                    storageFolderPaths = storageFolderPaths,
                    candidateFolderPaths = candidateFolderPaths,
                    unscannedTableLocations = unscannedTableLocations,
                ) + auditDiagnosticDetailsBuilder.scanPlanDetails(scanPlan) +
                    auditDiagnosticDetailsBuilder.inspectionOutcomeDetails(inspectionOutcomes) +
                    auditDiagnosticDetailsBuilder.wouldBeHeldByDeletionFloorDetails(wouldBeHeldByDeletionFloorPaths) +
                    auditDiagnosticDetailsBuilder.deletionFenceDetails(deletionFence) +
                    holdDetails(inspectionOutcomes, tooNewPaths, deletionFence, storage),
        )
    }

    // The database can also disappear at the pre-delete recheck, after other storage was read, so the row keeps
    // whatever checks had finished.
    fun recordDatabaseNotFound(
        runId: String,
        database: String,
        databaseStartTime: Instant,
        error: Throwable,
        ownershipChecks: OwnershipChecks = OwnershipChecks(),
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = config.catalog,
            databaseName = database,
            status = STATUS_SKIPPED,
            statusReason = "database_not_found",
            errorMessage = SafeText.errorMessage(error),
            ownershipChecks = ownershipChecks,
        )
    }

    fun recordFailed(
        runId: String,
        database: String,
        databaseStartTime: Instant,
        excludedPaths: List<String> = excludePathResolver.normalizedConfiguredExcludePaths(),
        unscannedTableLocations: List<String>? = null,
        scanPlan: ScanPlanReport? = null,
        unresolvedTables: List<String>,
        error: Throwable,
        ownershipChecks: OwnershipChecks = OwnershipChecks(),
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = config.catalog,
            databaseName = database,
            status = STATUS_FAILED,
            statusReason = "unexpected_error",
            errorMessage = SafeText.errorMessage(error),
            unresolvedTableCount = unresolvedTables.size.toLong(),
            excludedPaths = excludedPaths,
            ownershipChecks = ownershipChecks,
            diagnosticDetails =
                unscannedTableLocations
                    ?.let { auditDiagnosticDetailsBuilder.scanCoverageDetails(it, reportCompleteScanCoverage = true) }
                    .orEmpty() + auditDiagnosticDetailsBuilder.scanPlanDetails(scanPlan) +
                    auditDiagnosticDetailsBuilder.unresolvedTableDetails(unresolvedTables),
        )
    }

    /**
     * Written in deletion mode before the first delete of a database, naming the folders about to be deleted. If the run
     * ends before its final row, later runs see the intent and confirm none of these folders from earlier rows.
     */
    fun recordDeletionIntent(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        storageScanLocation: String,
        folderPaths: List<String>,
        cutoffTime: Instant,
        excludedPaths: List<String>,
        ownershipChecks: OwnershipChecks,
        storage: StorageEvidence,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = AUDIT_STATUS_INTENT,
            statusReason = "deletion_intent",
            errorMessage = null,
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            storageScanLocation = storageScanLocation,
            candidateFolderCount = folderPaths.size.toLong(),
            candidateFolders = folderPaths,
            cutoffTime = cutoffTime,
            excludedPaths = excludedPaths,
            ownershipChecks = ownershipChecks,
            storage = storage,
        )
    }

    /** Whether this run's intent row for the database reads back. False without a job id, as nothing could find it. */
    fun deletionIntentRecorded(
        runId: String,
        catalogName: String,
        databaseName: String,
    ): Boolean {
        val jobId = cleanupAuditTableService.currentExternalJobId() ?: return false
        return cleanupAuditTableService.intentRecorded(runId, SafeText.path(jobId), SafeText.path(catalogName), SafeText.path(databaseName))
    }

    private fun holdDetails(
        inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>>,
        tooNewPaths: List<String>,
        deletionFence: DeletionFenceReport,
        storage: StorageEvidence,
    ): Map<String, String> =
        auditDiagnosticDetailsBuilder.tooNewDetails(tooNewPaths) +
            auditDiagnosticDetailsBuilder.storageHoldDetails(storage.holds) +
            auditDiagnosticDetailsBuilder.heldFolderDetails(HeldFolders.of(inspectionOutcomes, tooNewPaths, deletionFence, storage.holds))

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
        unresolvedTableCount: Long = 0,
        potentiallyUntrackedFolderCount: Long = 0,
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
        excludedPaths: List<String> = excludePathResolver.normalizedConfiguredExcludePaths(),
        ownershipChecks: OwnershipChecks = OwnershipChecks(),
        diagnosticDetails: Map<String, String> = emptyMap(),
        storage: StorageEvidence = StorageEvidence.NONE,
    ) {
        // Every text value is made safe here, the one place rows are built. Names and paths keep their text in a form
        // that reads back exactly (SafeText.path). A multi-line diagnostic value is checked line by line, so its lines
        // stay separate; its entries were made safe when it was built, so this last pass keeps their escapes as is.
        cleanupAuditTableService.writeAuditRecord(
            CleanupAuditRecord(
                runId = runId,
                sparkAppId = cleanupAuditTableService.currentSparkAppId()?.let(SafeText::path),
                runtimeComputeId = cleanupAuditTableService.currentRuntimeComputeId()?.let(SafeText::path),
                runtimeComputeNamespace = cleanupAuditTableService.currentRuntimeComputeNamespace()?.let(SafeText::path),
                runtimeDomain = cleanupAuditTableService.currentRuntimeDomain()?.let(SafeText::path),
                runtimeUser = cleanupAuditTableService.currentRuntimeUser()?.let(SafeText::path),
                externalJobId = cleanupAuditTableService.currentExternalJobId()?.let(SafeText::path),
                platformStartedBy = cleanupAuditTableService.currentPlatformStartedBy()?.let(SafeText::path),
                catalogName = SafeText.path(catalogName),
                databaseName = SafeText.path(databaseName),
                operation = OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS,
                dryRun = config.dryRun,
                deleteEnabled = config.deleteEnabled,
                // The age the run used. The configured value is in diagnostic_details.
                olderThanHours = config.effectiveOlderThanHours(),
                cutoffTime = cutoffTime,
                maxCandidateFoldersPerDatabase = config.maxCandidateFoldersPerDatabase,
                excludedPaths = excludedPaths.sorted().map(SafeText::path),
                status = status,
                statusReason = statusReason?.let(SafeText::path),
                errorMessage = errorMessage?.let { SafeText.text(it, SafeText.MAX_AUDIT_ERROR_LENGTH) },
                discoveredDatabaseLocation = discoveredDatabaseLocation?.let(SafeText::path),
                storageScanLocation = SafeText.path(storageScanLocation),
                activeTableCount = activeTableCount,
                unresolvedTableCount = unresolvedTableCount,
                potentiallyUntrackedFolderCount = potentiallyUntrackedFolderCount,
                storageFolderCount = storageFolderCount,
                candidateFolderCount = candidateFolderCount,
                candidateObjectCount = candidateObjectCount,
                candidateTotalSizeBytes = candidateTotalSizeBytes,
                deletedFolderCount = deletedFolderCount,
                deletedObjectCount = deletedObjectCount,
                deletedTotalSizeBytes = deletedTotalSizeBytes,
                candidateFolders = candidateFolders.map(SafeText::path),
                deletedFolders = deletedFolders.map(SafeText::path),
                diagnosticDetails =
                    (
                        auditDiagnosticDetailsBuilder.ownershipCheckDetails(ownershipChecks) +
                            auditDiagnosticDetailsBuilder.ageDetails(config.olderThanHours) +
                            diagnosticDetails
                    )
                        .mapValues { (_, value) -> value.split('\n').joinToString("\n") { SafeText.safetyNet(it) } },
                startTime = databaseStartTime,
                endTime = Instant.now(),
                candidateFingerprints = storage.fingerprints?.entries?.associate { (path, fingerprint) -> SafeText.path(path) to SafeText.path(fingerprint) },
                prevRunId = storage.holds.history.newestRunId,
            )
        )
    }

    private companion object {
        const val OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS = "DISCOVER_UNTRACKED_TABLE_FOLDERS"
        const val STATUS_SUCCESS = "SUCCESS"
        const val STATUS_SKIPPED = "SKIPPED"
        const val STATUS_BLOCKED = "BLOCKED"
        const val STATUS_FAILED = "FAILED"
    }
}
