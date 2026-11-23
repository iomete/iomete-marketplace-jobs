package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.candidate.HeldFolders
import com.iomete.cleanup.untrackedtablefolders.config.DELETION_MIN_AGE_HOURS
import com.iomete.cleanup.untrackedtablefolders.logging.SafeText
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.DeletionFenceReport
import com.iomete.cleanup.untrackedtablefolders.storage.FenceKeepReason
import com.iomete.cleanup.untrackedtablefolders.storage.FolderObjectListing
import com.iomete.cleanup.untrackedtablefolders.storage.OWNERSHIP_SNAPSHOT_MAX_AGE
import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped

@ApplicationScoped
class CleanupAuditDiagnosticDetailsBuilder {

    fun build(
        activeTableLocations: List<String>,
        storageFolderPaths: List<String> = emptyList(),
        candidateFolderPaths: List<String> = emptyList(),
        nonCandidateStorageFolderPaths: List<String> = emptyList(),
        unresolvedTables: List<String> = emptyList(),
        potentiallyUntrackedFolderPaths: List<String> = emptyList(),
        unscannedTableLocations: List<String> = emptyList(),
        reportCompleteScanCoverage: Boolean = false,
        deletionFailedFolderPath: String? = null,
    ): Map<String, String> {
        val details = mutableMapOf(
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

        details.putAll(unresolvedTableDetails(unresolvedTables))

        if (potentiallyUntrackedFolderPaths.isNotEmpty()) {
            details["potentially_untracked_folder_paths_sample"] = auditPathSample(potentiallyUntrackedFolderPaths)
            details["potentially_untracked_folder_paths_truncated"] =
                isAuditPathSampleTruncated(potentiallyUntrackedFolderPaths).toString()
        }

        details.putAll(scanCoverageDetails(unscannedTableLocations, reportCompleteScanCoverage))

        if (deletionFailedFolderPath != null) {
            details["deletion_failed_folder_path"] = SafeText.path(deletionFailedFolderPath)
        }

        if (nonCandidateStorageFolderPaths.isNotEmpty()) {
            details["non_candidate_storage_folder_paths_sample"] =
                auditPathSample(nonCandidateStorageFolderPaths)
            details["non_candidate_storage_folder_paths_truncated"] =
                isAuditPathSampleTruncated(nonCandidateStorageFolderPaths).toString()
        }

        return details
    }

    // Stated on every row so a clean result is never read as proof that no table anywhere uses the folder.
    fun ownershipCheckDetails(checks: OwnershipChecks): Map<String, String> =
        unreadableStorageDetails(checks.unreadableStorage) + mapOf(
            "ownership_checks" to
                listOfNotNull(
                    "visible_tables_of_this_database",
                    "write_paths_of_visible_tables_of_this_database",
                    "default_folders_of_visible_other_databases".takeIf { checks.otherDatabaseFolders },
                    "configured_exclusions",
                    "visible_tables_of_other_databases".takeIf { checks.otherTables },
                    "visible_storage_of_same_bucket_session_catalogs".takeIf { checks.otherTables },
                    "final_recheck_before_deletion".takeIf { checks.finalRecheck },
                ).joinToString(","),
            "ownership_not_checked" to
                listOfNotNull(
                    "default_folders_of_other_databases".takeUnless { checks.otherDatabaseFolders },
                    "other_database_tables".takeUnless { checks.otherTables },
                    "same_bucket_session_catalogs".takeUnless { checks.otherTables },
                    "final_recheck_before_deletion".takeUnless { checks.finalRecheck },
                    "other_bucket_session_catalogs",
                    "catalogs_outside_session",
                    "hidden_tables",
                    "nested_namespace_tables",
                    "files_outside_table_folders",
                ).joinToString(","),
        )

    private fun unreadableStorageDetails(unreadableStorage: List<String>): Map<String, String> =
        if (unreadableStorage.isEmpty()) {
            emptyMap()
        } else {
            mapOf(
                // Each entry names what could not be read and the error class, so it is free text, not a path.
                "ownership_unreadable_sample" to unreadableStorage.take(MAX_AUDIT_PATH_SAMPLE_SIZE).joinToString("\n") { SafeText.text(it) },
                "ownership_unreadable_truncated" to isAuditPathSampleTruncated(unreadableStorage).toString(),
            )
        }

    fun unresolvedTableDetails(unresolvedTables: List<String>): Map<String, String> =
        if (unresolvedTables.isEmpty()) {
            emptyMap()
        } else {
            mapOf(
                "unresolved_tables_sample" to auditPathSample(unresolvedTables),
                "unresolved_tables_truncated" to isAuditPathSampleTruncated(unresolvedTables).toString(),
            )
        }

    // Failure rows report a complete scan as a zero count, so a missing count means coverage was never evaluated.
    fun scanCoverageDetails(
        unscannedTableLocations: List<String>,
        reportCompleteScanCoverage: Boolean = false,
    ): Map<String, String> =
        when {
            unscannedTableLocations.isNotEmpty() ->
                mapOf(
                    "unscanned_table_location_count" to unscannedTableLocations.size.toString(),
                    "unscanned_table_locations_sample" to auditPathSample(unscannedTableLocations),
                    "unscanned_table_locations_truncated" to isAuditPathSampleTruncated(unscannedTableLocations).toString(),
                )

            reportCompleteScanCoverage -> mapOf("unscanned_table_location_count" to "0")
            else -> emptyMap()
        }

    // No scan_plan_status means planning was not attempted. A failed plan never changes the row's status.
    fun scanPlanDetails(scanPlan: ScanPlanReport?): Map<String, String> {
        if (scanPlan == null) {
            return emptyMap()
        }

        // Cleanup roots are chosen before planning, so they are recorded even when planning failed.
        val cleanupRootDetails =
            buildMap {
                if (scanPlan.cleanupRoots.isNotEmpty()) {
                    put("cleanup_roots", scanPlan.cleanupRoots.joinToString("\n") { SafeText.path(it) })
                }
                scanPlan.managedRootStatus?.let { put("managed_root_status", it) }
            }

        if (scanPlan.planningError != null) {
            return mapOf(
                "scan_plan_status" to "failed",
                "scan_plan_error" to SafeText.text(scanPlan.planningError, MAX_SCAN_PLAN_ERROR_LENGTH),
            ) + cleanupRootDetails
        }

        val rootLines =
            scanPlan.roots.map { "${it.root.location}=${it.role}:${it.status}:visible_tables=${it.visibleTableCount}" }
        val reportOnlyFolders = scanPlan.reportOnlyFolderPaths
        val uncovered = scanPlan.uncoveredTableLocations

        val details =
            mutableMapOf(
                "scan_plan_status" to "planned",
                "ownership_basis" to "visible_catalog_tables",
                "scan_root_count" to scanPlan.roots.size.toString(),
                "scan_root_not_scanned_count" to scanPlan.roots.count { it.outcome == ScanRootOutcome.NOT_SCANNED }.toString(),
                "scan_roots_sample" to auditPathSample(rootLines),
                "scan_roots_truncated" to isAuditPathSampleTruncated(rootLines).toString(),
                "scan_plan_coverage" to if (scanPlan.complete) "complete" else "partial",
                "scan_plan_uncovered_table_location_count" to uncovered.size.toString(),
                "report_only_folder_count" to reportOnlyFolders.size.toString(),
            )

        if (uncovered.isNotEmpty()) {
            details["scan_plan_uncovered_table_locations_sample"] = auditPathSample(uncovered)
            details["scan_plan_uncovered_table_locations_truncated"] = isAuditPathSampleTruncated(uncovered).toString()
        }

        if (reportOnlyFolders.isNotEmpty()) {
            details["report_only_folders_sample"] = auditPathSample(reportOnlyFolders)
            details["report_only_folders_truncated"] = isAuditPathSampleTruncated(reportOnlyFolders).toString()
        }

        scanPlan.restNamespaceRootStatus?.let { details["rest_namespace_root"] = it }

        // Plan roots are normalized; the counts follow the order of cleanup_roots.
        val folderCountByRoot = scanPlan.roots.filter { it.root.cleanupRoot }.associate { it.root.location to it.listedFolderCount }
        val cleanupRootFolderCounts =
            scanPlan.cleanupRoots.mapNotNull { root ->
                folderCountByRoot[runCatching { StoragePathUtils.normalizeLocation(root) }.getOrNull()]?.let { "$root=$it" }
            }
        if (cleanupRootFolderCounts.isNotEmpty()) {
            details["cleanup_root_folder_counts"] = cleanupRootFolderCounts.joinToString("\n") { SafeText.path(it) }
        }

        return details + cleanupRootDetails
    }

    fun inspectionOutcomeDetails(inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>>): Map<String, String> =
        CandidateInspectionOutcome.entries
            .flatMap { outcome -> countedPathSample(outcome.code, inspectionOutcomes[outcome].orEmpty()) }
            .toMap()

    fun tooNewDetails(tooNewPaths: List<String>): Map<String, String> = countedPathSample(HeldFolders.TOO_NEW, tooNewPaths).toMap()

    // One place for every reason a folder with no verified owner was kept; each reason's own keys list the folders.
    fun heldFolderDetails(held: HeldFolders): Map<String, String> =
        if (held.count == 0) {
            emptyMap()
        } else {
            mapOf(
                "held_folder_count" to held.count.toString(),
                "held_folder_counts" to held.countsText("\n"),
            )
        }

    fun notFoundAtDeletionDetails(notFoundAtDeletionPaths: List<String>): Map<String, String> =
        countedPathSample("folder_not_found_at_deletion", notFoundAtDeletionPaths).toMap()

    fun keptAtFinalCheckDetails(keptAtFinalCheckPaths: List<String>): Map<String, String> =
        countedPathSample("folder_kept_at_final_check", keptAtFinalCheckPaths).toMap()

    // older_than_hours holds the age the run used, so the configured value is kept here. Rows written before the
    // minimum existed have neither key; their older_than_hours is both the configured and the used value.
    fun ageDetails(configuredOlderThanHours: Long): Map<String, String> =
        mapOf(
            "configured_older_than_hours" to configuredOlderThanHours.toString(),
            "deletion_min_age_hours" to DELETION_MIN_AGE_HOURS.toString(),
        )

    fun wouldBeHeldByDeletionFloorDetails(paths: List<String>): Map<String, String> =
        countedPathSample("would_be_held_by_deletion_floor", paths).toMap()

    // Keys are relative to the folder. Each listed key is in exactly one of deleted (confirmed), remaining (not
    // attempted, or still there after its delete returned), unknown_state (the delete threw or was not waited for, so
    // it may still complete) and vanished (gone before the job deleted it). Unlisted keys were found after the listed
    // ones were deleted. Directories are full paths: kept ones are still there, and an unknown_state one's delete may
    // still complete. Unless directory_cleanup is finished, an object written after the listing may be unreported.
    fun deletionFenceDetails(report: DeletionFenceReport): Map<String, String> {
        val details = mutableMapOf<String, String>()
        FenceKeepReason.entries.forEach { reason ->
            details.putAll(countedPathSample("folder_kept_${reason.code}", report.keptPaths[reason].orEmpty()))
        }
        report.partialFolder?.let { folder ->
            val prefix = FolderObjectListing.folderPrefix(folder.folderPath) + "/"
            details["partially_deleted_folder_path"] = SafeText.path(folder.folderPath)
            details["partially_deleted_folder_listed_object_count"] = folder.listedObjectCount.toString()
            details.putAll(partialKeySet("deleted", folder.deletedKeys))
            details.putAll(partialKeySet("remaining", folder.remainingKeys))
            details.putAll(partialKeySet("unknown_state", folder.unknownKeys))
            details.putAll(partialKeySet("vanished", folder.vanishedKeys))
            details.putAll(partialKeySet("unlisted", folder.leftoverObjectPaths.map { it.removePrefix(prefix) }))
            details["partially_deleted_folder_directory_cleanup"] = folder.directoryCleanup.code
            details.putAll(countedPathSample("partially_deleted_folder_kept_directory", folder.leftoverDirectoryPaths))
            details.putAll(countedPathSample("partially_deleted_folder_unknown_state_directory", folder.unknownDirectoryPaths))
        }
        details.putAll(countedPathSample("deleted_folder_kept_directory", report.keptDirectoryPaths))
        details.putAll(countedPathSample("folder_not_attempted", report.notAttemptedPaths))
        report.stop?.let {
            details["deletion_stop_cause"] = it.cause.code
            details["ownership_snapshot_max_age_seconds"] = OWNERSHIP_SNAPSHOT_MAX_AGE.seconds.toString()
        }
        if (report.ownershipRefreshCount > 0) {
            details["ownership_refresh_count"] = report.ownershipRefreshCount.toString()
        }
        return details
    }

    private fun partialKeySet(
        set: String,
        keys: List<String>,
    ): List<Pair<String, String>> =
        listOf("partially_deleted_folder_${set}_object_count" to keys.size.toString()) +
            if (keys.isEmpty()) {
                emptyList()
            } else {
                listOf(
                    "partially_deleted_folder_${set}_keys_sample" to auditPathSample(keys),
                    "partially_deleted_folder_${set}_keys_truncated" to isAuditPathSampleTruncated(keys).toString(),
                )
            }

    private fun countedPathSample(
        key: String,
        paths: List<String>,
    ): List<Pair<String, String>> =
        if (paths.isEmpty()) {
            emptyList()
        } else {
            listOf(
                "${key}_count" to paths.size.toString(),
                "${key}_paths_sample" to auditPathSample(paths),
                "${key}_paths_truncated" to isAuditPathSampleTruncated(paths).toString(),
            )
        }

    // Entries are made safe before they are joined, so a newline in a name cannot split it into two entries.
    private fun auditPathSample(paths: List<String>): String =
        paths.take(MAX_AUDIT_PATH_SAMPLE_SIZE).joinToString("\n") { SafeText.path(it) }

    private fun isAuditPathSampleTruncated(paths: List<String>): Boolean =
        paths.size > MAX_AUDIT_PATH_SAMPLE_SIZE

    private companion object {
        const val MAX_SCAN_PLAN_ERROR_LENGTH = 500
    }
}
