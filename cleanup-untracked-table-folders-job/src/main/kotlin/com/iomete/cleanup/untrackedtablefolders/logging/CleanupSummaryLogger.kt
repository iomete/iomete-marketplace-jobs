package com.iomete.cleanup.untrackedtablefolders.logging

import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
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
    val deletionEligible: Boolean,
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
            addScanPlan(database, scanPlan)
            add(SEPARATOR)
        }

    internal fun buildReportLines(summary: CleanupSummary): List<String> = buildList {
        val unmatchedFolderSet = summary.unmatchedFolderPaths.toSet()
        val matchedFolderPaths = summary.storageFolderPaths.filter { it !in unmatchedFolderSet }

        repeat(BLANK_LINES_AROUND_REPORT) { add("") }

        add(SEPARATOR)
        add(" CLEANUP REPORT - ${summary.catalog}.${summary.database}")
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
        add("  Folders belonging to verified catalog tables     ${matchedFolderPaths.size}")
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

        if (summary.unscannedTableLocations.isNotEmpty()) {
            add("")
            add("TABLE LOCATIONS OUTSIDE THE SCANNED ROOT")
            add("")
            add("  Storage around these table locations was not checked in this run.")
            add("")
            addListOrNone(summary.unscannedTableLocations)
        }

        summary.scanPlan?.let { addScanPlan(summary.database, it) }

        add("")
        if (summary.deletionEligible) {
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
        if (summary.deletionEligible) {
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

    private fun MutableList<String>.addScanPlan(
        database: String,
        scanPlan: ScanPlanReport,
    ) {
        add("")
        add("STORAGE ROOTS")
        add("")
        add("  Folders are deleted only under the cleanup root. Other roots are listed for review only.")
        add("  Coverage is ${if (scanPlan.complete) "complete" else "partial"} for the tables visible to this job.")
        add("")
        scanPlan.roots.forEach { root ->
            add("- ${root.root.location}  ${root.role}  ${root.status}  visible tables: ${root.visibleTableCount}")
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

        if (scanPlan.unapprovedTableParents.isNotEmpty()) {
            val roots = (scanPlan.acceptedApprovedRoots + scanPlan.unapprovedTableParents).distinct().sorted()
            add("")
            add("LOCATIONS THAT CAN BE APPROVED FOR REPORT-ONLY LISTING")
            add("")
            add("  Approve a location only if it belongs to this database. Approval lists it for review;")
            add("  it never makes anything in it eligible for deletion. To approve, set this in the job config:")
            add("")
            add("  \"report_only_storage_roots\": {")
            add("    \"$database\": [")
            roots.forEachIndexed { index, root -> add("      \"$root\"" + if (index < roots.lastIndex) "," else "") }
            add("    ]")
            add("  }")
        }
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
        const val BLANK_LINES_AROUND_REPORT = 3
        const val SEPARATOR = "================================================================"
    }
}
