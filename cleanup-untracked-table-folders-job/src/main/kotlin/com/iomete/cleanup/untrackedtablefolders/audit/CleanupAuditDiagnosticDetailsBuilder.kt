package com.iomete.cleanup.untrackedtablefolders.audit

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

        if (nonCandidateStorageFolderPaths.isNotEmpty()) {
            details["non_candidate_storage_folder_paths_sample"] =
                auditPathSample(nonCandidateStorageFolderPaths)
            details["non_candidate_storage_folder_paths_truncated"] =
                isAuditPathSampleTruncated(nonCandidateStorageFolderPaths).toString()
        }

        return details
    }

    private fun auditPathSample(paths: List<String>): String =
        paths.take(MAX_AUDIT_PATH_SAMPLE_SIZE).joinToString("\n")

    private fun isAuditPathSampleTruncated(paths: List<String>): Boolean =
        paths.size > MAX_AUDIT_PATH_SAMPLE_SIZE
}
