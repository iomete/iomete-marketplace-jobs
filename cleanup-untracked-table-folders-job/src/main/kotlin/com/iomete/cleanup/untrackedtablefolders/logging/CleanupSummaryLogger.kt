package com.iomete.cleanup.untrackedtablefolders.logging

import com.iomete.cleanup.untrackedtablefolders.audit.DeletionBlockReason
import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

data class CleanupSummary(
    val catalog: String,
    val database: String,
    val discoveredDatabaseLocation: String?,
    val storageScanLocation: String,
    val catalogTableCount: Int,
    val unresolvedTableNames: List<String>,
    val activeTableLocations: List<String>,
    val storageFolderPaths: List<String>,
    val excludedPaths: List<String>,
    val unmatchedFolderPaths: List<String>,
    val unmatchedSizeStats: StorageSizeStats?,
    val dryRun: Boolean,
    val deletionEligible: Boolean,
    val deletionBlockReason: DeletionBlockReason? = null,
    val deletedFolderPaths: List<String>,
    val deletedSizeStats: StorageSizeStats?,
    val unscannedTableLocations: List<String> = emptyList(),
    val scanPlan: ScanPlanReport? = null,
)

@ApplicationScoped
class CleanupSummaryLogger {

    private val logger = Logger.getLogger(CleanupSummaryLogger::class.java)

    fun logCleanupSummary(summary: CleanupSummary) {
        buildReportLines(summary).forEach { logger.info(it) }
    }

    fun logScanPlan(
        catalog: String,
        database: String,
        scanPlan: ScanPlanReport,
    ) {
        buildScanPlanLines(catalog, database, scanPlan).forEach { logger.info(it) }
    }

    internal fun buildScanPlanLines(
        catalog: String,
        database: String,
        scanPlan: ScanPlanReport,
    ): List<String> =
        buildList {
            add(SEPARATOR)
            add(" STORAGE ROOT REPORT - $catalog.$database")
            add(SEPARATOR)
            add("")
            add("  Cleanup was skipped for this database. Nothing was deleted.")
            addScanPlan(scanPlan)
            add(SEPARATOR)
        }

    internal fun buildReportLines(summary: CleanupSummary): List<String> = buildList {
        val unmatchedFolderSet = summary.unmatchedFolderPaths.toSet()
        // The report is written after deletion, so a path it cannot compare must not make it throw.
        val tableFolderPaths =
            summary.storageFolderPaths.filter { folder ->
                summary.activeTableLocations.any { runCatching { StoragePathUtils.overlaps(folder, it) }.getOrDefault(false) }
            }
        val otherSkippedFolderPaths = summary.storageFolderPaths - tableFolderPaths.toSet() - unmatchedFolderSet

        repeat(BLANK_LINES_AROUND_REPORT) { add("") }

        add(SEPARATOR)
        add(" CLEANUP REPORT - ${summary.catalog}.${summary.database}")
        add(if (summary.dryRun) " Mode: dry run, nothing is deleted" else " Mode: deletion enabled")
        add(SEPARATOR)
        add("")
        add("CATALOG")
        add("")
        add("  Tables found                                     ${summary.catalogTableCount}")
        add("  Tables with verified storage locations           ${summary.activeTableLocations.size}")
        add("  Tables whose storage location could not be read  ${summary.unresolvedTableNames.size}")

        if (summary.unresolvedTableNames.isNotEmpty()) {
            add("")
            add("TABLES WHOSE STORAGE LOCATION COULD NOT BE RESOLVED")
            add("")
            add("  These tables exist in the catalog, but their Iceberg metadata or storage")
            add("  location could not be read.")
            add("")
            addListOrNone(summary.unresolvedTableNames)
        }

        add("")
        add("STORAGE")
        add("")
        add("  Database location                                ${summary.discoveredDatabaseLocation}")
        add("  Folders scanned under                            ${summary.storageScanLocation}")
        add("  Folders discovered                               ${summary.storageFolderPaths.size}")
        add("  Folders belonging to verified catalog tables     ${tableFolderPaths.size}")
        add("  Folders protected or skipped for other reasons   ${otherSkippedFolderPaths.size}")
        add("  Folders with no verified catalog owner           ${summary.unmatchedFolderPaths.size}")
        add("  Table locations outside the scanned root         ${summary.unscannedTableLocations.size}")
        add(
            if (summary.unmatchedSizeStats != null) {
                "  Size of folders with no verified owner           ${formatBytes(summary.unmatchedSizeStats.totalSizeBytes)} across ${summary.unmatchedSizeStats.objectCount} object(s)"
            } else {
                "  Size of folders with no verified owner           not available"
            }
        )

        add("")
        add("STORAGE FOLDERS PROTECTED BY A VERIFIED CATALOG TABLE")
        add("")
        addListOrNone(summary.activeTableLocations.sorted())

        add("")
        add("STORAGE FOLDERS EXCLUDED BY CONFIGURATION")
        add("")
        addListOrNone(summary.excludedPaths.sorted())

        if (otherSkippedFolderPaths.isNotEmpty()) {
            add("")
            add("STORAGE FOLDERS PROTECTED OR SKIPPED FOR OTHER REASONS")
            add("")
            add("  These folders are not deleted. They are excluded, belong to another database or to a")
            add("  table's write path, are framework working folders, or are too new or of unknown age.")
            add("")
            addListOrNone(otherSkippedFolderPaths)
        }

        if (summary.unscannedTableLocations.isNotEmpty()) {
            add("")
            add("TABLE LOCATIONS OUTSIDE THE SCANNED ROOT")
            add("")
            add("  Storage around these table locations was not checked in this run.")
            add("")
            addListOrNone(summary.unscannedTableLocations)
        }

        summary.scanPlan?.let { addScanPlan(it) }

        add("")
        if (summary.deletionBlockReason != null) {
            add("UNTRACKED STORAGE FOLDERS (DELETION BLOCKED)")
            add("")
            add("  These folders are not referenced by the discovered catalog tables, but deletion")
            add("  was blocked for this database. See DELETION below.")
        } else if (summary.deletionEligible) {
            add("UNTRACKED STORAGE FOLDERS")
            add("")
            add("  These folders are not referenced by the discovered catalog tables and are")
            add("  eligible for cleanup under the configured safety rules.")
        } else {
            add("POTENTIALLY UNTRACKED STORAGE FOLDERS")
            add("")
            add("  These folders are not referenced by any table whose storage location could")
            add("  be verified.")
            add("")
            add("  Because ${summary.unresolvedTableNames.size} catalog table(s) have unknown storage locations,")
            add("  these ${summary.unmatchedFolderPaths.size} unmatched storage folders cannot be proven safe to delete.")
        }
        add("")
        addListOrNone(summary.unmatchedFolderPaths)

        add("")
        add("DELETION")
        add("")
        if (summary.deletionBlockReason != null) {
            add("  BLOCKED")
            add("")
            add("  Reason: ${summary.deletionBlockReason.code}")
            deletionBlockExplanation(summary.deletionBlockReason).forEach { add("  $it") }
            add("  No folder in this database was deleted.")
        } else if (summary.deletionEligible && summary.dryRun) {
            add("  Dry run. No folder was deleted.")
        } else if (summary.deletionEligible) {
            add("  Folders deleted                                  ${summary.deletedFolderPaths.size}")
            add(
                if (summary.deletedSizeStats != null) {
                    "  Size deleted                                     ${formatBytes(summary.deletedSizeStats.totalSizeBytes)} across ${summary.deletedSizeStats.objectCount} object(s)"
                } else {
                    "  Size deleted                                     not available"
                }
            )
            add("")
            addListOrNone(summary.deletedFolderPaths)
        } else {
            add("  BLOCKED")
            add("")
            add("  Reason:")
            add("  ${summary.unresolvedTableNames.size} catalog table(s) have unresolved storage ownership.")
            add("  No folder in this database was deleted.")
        }

        add(SEPARATOR)
        repeat(BLANK_LINES_AROUND_REPORT) { add("") }
    }

    private fun MutableList<String>.addScanPlan(scanPlan: ScanPlanReport) {
        add("")
        add("STORAGE ROOTS")
        add("")

        if (scanPlan.planningError != null) {
            add("  Storage roots could not be planned, so no other root was listed. Deletion is not affected.")
            add("  Error: ${scanPlan.planningError}")
            return
        }

        add("  Folders are deleted only under the cleanup root. Other roots are listed for review only.")
        add("  Coverage is ${if (scanPlan.complete) "complete" else "partial"} for the tables visible to this job.")
        if (scanPlan.restNamespaceRootStatus != null && scanPlan.restNamespaceRootStatus != "derived") {
            add("  The REST namespace root was not derived: ${scanPlan.restNamespaceRootStatus}.")
        }
        add("")
        val (notScanned, listed) = scanPlan.roots.partition { it.outcome == ScanRootOutcome.NOT_SCANNED }
        (listed + notScanned.take(MAX_LOGGED_NOT_SCANNED_ROOTS)).forEach { root ->
            add("- ${root.root.location}  ${root.role}  ${root.status}  visible tables: ${root.visibleTableCount}")
        }
        if (notScanned.size > MAX_LOGGED_NOT_SCANNED_ROOTS) {
            add("- ... ${notScanned.size - MAX_LOGGED_NOT_SCANNED_ROOTS} more root(s) not scanned")
        }

        if (scanPlan.uncoveredTableLocations.isNotEmpty()) {
            add("")
            add("TABLE LOCATIONS OUTSIDE EVERY LISTED ROOT")
            add("")
            addListOrNone(scanPlan.uncoveredTableLocations)
        }

        add("")
        add("FOLDERS IN REPORT-ONLY ROOTS NOT MATCHED TO A VISIBLE TABLE")
        add("")
        add("  These folders are not deleted. A table this job cannot see may still own them.")
        add("")
        addListOrNone(scanPlan.reportOnlyFolderPaths)
    }

    private fun deletionBlockExplanation(reason: DeletionBlockReason): List<String> =
        when (reason) {
            DeletionBlockReason.AUDIT_INTEGRITY_LOST ->
                listOf("An audit write failed earlier in this run, so no further folders are deleted in it.")
            DeletionBlockReason.CLEANUP_ROOT_HAS_NO_VISIBLE_TABLE ->
                listOf(
                    "No table this job can see is stored under the cleanup root, so nothing in it can be",
                    "checked against a known table.",
                )
            DeletionBlockReason.OTHER_DATABASE_STORAGE_UNKNOWN ->
                listOf("The storage of other databases could not be established, so it cannot be protected.")
        }

    private fun MutableList<String>.addListOrNone(values: List<String>) {
        if (values.isEmpty()) {
            add("- none")
            return
        }

        values.take(MAX_LOG_PATH_SAMPLE_SIZE).forEach { add("- $it") }

        if (values.size > MAX_LOG_PATH_SAMPLE_SIZE) {
            add("- ... truncated ${values.size - MAX_LOG_PATH_SAMPLE_SIZE} additional path(s)")
        }
    }

    private fun formatBytes(bytes: Long): String {
        val units = listOf("B", "KB", "MB", "GB", "TB", "PB")
        var value = bytes.toDouble()
        var unitIndex = 0

        while (value >= 1024 && unitIndex < units.lastIndex) {
            value /= 1024
            unitIndex += 1
        }

        return if (unitIndex == 0) {
            "${value.toLong()} ${units[unitIndex]}"
        } else {
            String.format("%.2f %s", value, units[unitIndex])
        }
    }

    private companion object {
        const val MAX_LOG_PATH_SAMPLE_SIZE = 100
        const val MAX_LOGGED_NOT_SCANNED_ROOTS = 25
        const val BLANK_LINES_AROUND_REPORT = 3
        const val SEPARATOR = "================================================================"
    }
}
