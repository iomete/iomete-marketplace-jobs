package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootOutcome
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

        if (unresolvedTables.isNotEmpty()) {
            details["unresolved_tables_sample"] = auditPathSample(unresolvedTables)
            details["unresolved_tables_truncated"] = isAuditPathSampleTruncated(unresolvedTables).toString()
        }

        if (potentiallyUntrackedFolderPaths.isNotEmpty()) {
            details["potentially_untracked_folder_paths_sample"] = auditPathSample(potentiallyUntrackedFolderPaths)
            details["potentially_untracked_folder_paths_truncated"] =
                isAuditPathSampleTruncated(potentiallyUntrackedFolderPaths).toString()
        }

        details.putAll(scanCoverageDetails(unscannedTableLocations, reportCompleteScanCoverage))

        if (deletionFailedFolderPath != null) {
            details["deletion_failed_folder_path"] = deletionFailedFolderPath
        }

        if (nonCandidateStorageFolderPaths.isNotEmpty()) {
            details["non_candidate_storage_folder_paths_sample"] =
                auditPathSample(nonCandidateStorageFolderPaths)
            details["non_candidate_storage_folder_paths_truncated"] =
                isAuditPathSampleTruncated(nonCandidateStorageFolderPaths).toString()
        }

        return details
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

        if (scanPlan.planningError != null) {
            return mapOf(
                "scan_plan_status" to "failed",
                "scan_plan_error" to scanPlan.planningError.take(MAX_SCAN_PLAN_ERROR_LENGTH),
            )
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

        return details
    }

    private fun auditPathSample(paths: List<String>): String =
        paths.take(MAX_AUDIT_PATH_SAMPLE_SIZE).joinToString("\n")

    private fun isAuditPathSampleTruncated(paths: List<String>): Boolean =
        paths.size > MAX_AUDIT_PATH_SAMPLE_SIZE

    private companion object {
        const val MAX_SCAN_PLAN_ERROR_LENGTH = 500
    }
}
