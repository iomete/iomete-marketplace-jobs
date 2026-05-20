package com.iomete.cleanup.untrackedtablefolders.audit

import java.time.Instant

data class CleanupAuditRecord(
    val sparkAppId: String?,
    val runId: String,
    val initiatedBy: String?,
    val catalogName: String,
    val databaseName: String,
    val operation: String,
    val dryRun: Boolean,
    val deleteEnabled: Boolean,
    val status: String,
    val discoveredDatabaseLocation: String?,
    val storageScanLocation: String,
    val activeTableCount: Long,
    val storageFolderCount: Long,
    val candidateFolderCount: Long,
    val deletedFolderCount: Long,
    val candidateFolders: List<String>,
    val deletedFolders: List<String>,
    val excludedPaths: List<String>,
    val metrics: Map<String, String>,
    val errorMessage: String?,
    val startTime: Instant,
    val endTime: Instant,
)
