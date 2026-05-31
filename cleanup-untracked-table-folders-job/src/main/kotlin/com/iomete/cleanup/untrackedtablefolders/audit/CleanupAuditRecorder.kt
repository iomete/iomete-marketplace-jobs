package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.time.Instant

@ApplicationScoped
class CleanupAuditRecorder {

    @Inject lateinit var config: ApplicationConfig
    @Inject lateinit var cleanupAuditTableService: CleanupAuditTableService
    @Inject lateinit var auditDiagnosticDetailsBuilder: CleanupAuditDiagnosticDetailsBuilder

    fun recordDatabaseLocationMissing(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        activeTableCount: Long,
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
            excludedPaths = excludedPaths,
            diagnosticDetails =
                auditDiagnosticDetailsBuilder.build(activeTableLocations = activeTableLocations),
        )
    }

    fun recordNoActiveTables(
        runId: String,
        databaseStartTime: Instant,
        catalogName: String,
        databaseName: String,
        discoveredDatabaseLocation: String?,
        excludedPaths: List<String>,
    ) {
        writeAuditRecord(
            runId = runId,
            databaseStartTime = databaseStartTime,
            catalogName = catalogName,
            databaseName = databaseName,
            status = STATUS_SKIPPED,
            statusReason = "no_active_tables_in_database",
            errorMessage = "Database has no active table locations; cleanup skipped to avoid deleting an unanchored storage root.",
            discoveredDatabaseLocation = discoveredDatabaseLocation,
            activeTableCount = 0,
            excludedPaths = excludedPaths,
            diagnosticDetails = auditDiagnosticDetailsBuilder.build(activeTableLocations = emptyList()),
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
        activeTableLocations: List<String>,
        storageFolderPaths: List<String>,
        candidateFolderPaths: List<String>,
        cutoffTime: Instant,
        excludedPaths: List<String>,
        errorMessage: String,
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
            candidateFolderCount = candidateFolderPaths.size.toLong(),
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

    fun recordDatabaseNotFound(
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

    fun recordFailed(
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

    private fun normalizedConfiguredExcludePaths(): List<String> =
        config.excludePaths
            .map { StoragePathUtils.normalizeLocation(it) }
            .distinct()
            .sorted()

    private companion object {
        const val OPERATION_DISCOVER_UNTRACKED_TABLE_FOLDERS = "DISCOVER_UNTRACKED_TABLE_FOLDERS"
        const val STATUS_SUCCESS = "SUCCESS"
        const val STATUS_SKIPPED = "SKIPPED"
        const val STATUS_FAILED = "FAILED"
        const val MAX_AUDIT_PATH_SAMPLE_SIZE = 100
    }
}
