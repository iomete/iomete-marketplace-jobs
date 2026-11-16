package com.iomete.cleanup.untrackedtablefolders.logging

import com.iomete.cleanup.untrackedtablefolders.audit.DeletionBlockReason
import com.iomete.cleanup.untrackedtablefolders.audit.OwnershipChecks
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.MANAGED_ROOT_REFUSED_PREFIX
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
    val inspectionOutcomes: Map<CandidateInspectionOutcome, List<String>> = emptyMap(),
    val notFoundAtDeletionPaths: List<String> = emptyList(),
    val unresolvedTableDetails: Map<String, UnresolvedTableDetail> = emptyMap(),
    val keptAtFinalCheckPaths: List<String> = emptyList(),
    val ownershipChecks: OwnershipChecks = OwnershipChecks(),
    val cleanupRoots: List<String> = listOf(storageScanLocation),
)

data class NoVerifiedLocationSummary(
    val catalog: String,
    val database: String,
    val discoveredDatabaseLocation: String?,
    val catalogTableCount: Int,
    val unresolvedTableNames: List<String>,
    val dryRun: Boolean,
    val unresolvedTableDetails: Map<String, UnresolvedTableDetail> = emptyMap(),
)

data class TooManyFoldersSummary(
    val catalog: String,
    val database: String,
    val discoveredDatabaseLocation: String?,
    val storageScanLocation: String,
    val catalogTableCount: Int,
    val verifiedTableCount: Int,
    val unresolvedTableNames: List<String>,
    val storageFolderCount: Int,
    val unmatchedFolderPaths: List<String>,
    val maxCandidateFolders: Int,
    val dryRun: Boolean,
    val unresolvedTableDetails: Map<String, UnresolvedTableDetail> = emptyMap(),
    val ownershipChecks: OwnershipChecks = OwnershipChecks(),
    val cleanupRoots: List<String> = listOf(storageScanLocation),
)

// knownLocation is evidence only. It is never a verified table location.
data class UnresolvedTableDetail(
    val reason: String,
    val knownLocation: String?,
)

@ApplicationScoped
class CleanupSummaryLogger {

    // One statement per report, so log viewers that sort by timestamp cannot interleave its lines.
    private val logger = Logger.getLogger(CleanupSummaryLogger::class.java)

    fun logCleanupSummary(summary: CleanupSummary) {
        logger.info(buildReportLines(summary).joinToString("\n"))
    }

    fun logScanPlan(
        catalog: String,
        database: String,
        scanPlan: ScanPlanReport,
    ) {
        logger.info(buildScanPlanLines(catalog, database, scanPlan).joinToString("\n"))
    }

    fun logNoVerifiedLocationReport(summary: NoVerifiedLocationSummary) {
        logger.info(buildNoVerifiedLocationReportLines(summary).joinToString("\n"))
    }

    fun logTooManyFoldersReport(summary: TooManyFoldersSummary) {
        logger.info(buildTooManyFoldersReportLines(summary).joinToString("\n"))
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

    internal fun buildNoVerifiedLocationReportLines(summary: NoVerifiedLocationSummary): List<String> =
        buildList {
            addReportStart(
                summary.catalog,
                summary.database,
                summary.dryRun,
                summary.catalogTableCount,
                verifiedTableCount = 0,
                summary.unresolvedTableNames,
                summary.unresolvedTableDetails,
                OwnershipChecks(),
            )

            add("")
            add("STORAGE")
            add("")
            add("  Database location                                ${summary.discoveredDatabaseLocation}")
            add("")
            add("  Storage was not scanned. No table has a verified location, so storage folders")
            add("  cannot be compared with the catalog.")

            add("")
            add("DELETION")
            add("")
            add("  BLOCKED")
            add("")
            add("  Reason:")
            add("  ${summary.unresolvedTableNames.size} catalog table(s) have unresolved storage ownership.")
            add("  No folder in this database was deleted.")

            add(SEPARATOR)
            repeat(BLANK_LINES_AROUND_REPORT) { add("") }
        }

    internal fun buildTooManyFoldersReportLines(summary: TooManyFoldersSummary): List<String> =
        buildList {
            addReportStart(
                summary.catalog,
                summary.database,
                summary.dryRun,
                summary.catalogTableCount,
                summary.verifiedTableCount,
                summary.unresolvedTableNames,
                summary.unresolvedTableDetails,
                summary.ownershipChecks,
            )

            add("")
            add("STORAGE")
            add("")
            add("  Database location                                ${summary.discoveredDatabaseLocation}")
            addFoldersScannedUnder(summary.cleanupRoots)
            add("  Folders discovered                               ${summary.storageFolderCount}")
            add("  Folders with no verified catalog owner           ${summary.unmatchedFolderPaths.size}")
            add("")
            add("  Folder contents were not inspected because the folder limit was exceeded.")

            add("")
            add("FOLDERS WITH NO VERIFIED CATALOG OWNER (NOT INSPECTED)")
            add("")
            add("  Their age and size were not checked, and none of them was deleted.")
            add("")
            addListOrNone(summary.unmatchedFolderPaths)

            add("")
            add("DELETION")
            add("")
            add("  SKIPPED")
            add("")
            add("  Reason: too_many_candidate_folders")
            add("  ${summary.unmatchedFolderPaths.size} folder(s) exceed max_candidate_folders_per_database=${summary.maxCandidateFolders}.")
            if (summary.unresolvedTableNames.isNotEmpty()) {
                add("  ${summary.unresolvedTableNames.size} catalog table(s) also have unresolved storage ownership, so raising")
                add("  the limit will not make these folders deletable.")
            } else {
                add("  Review the folders above. Protect folders to keep with exclude_paths, or raise the limit.")
            }
            add("  No folder in this database was deleted.")

            add(SEPARATOR)
            repeat(BLANK_LINES_AROUND_REPORT) { add("") }
        }

    internal fun buildReportLines(summary: CleanupSummary): List<String> = buildList {
        val unmatchedFolderSet = summary.unmatchedFolderPaths.toSet()
        // The report is written after deletion, so a path it cannot compare must not make it throw.
        val tableFolderPaths =
            summary.storageFolderPaths.filter { folder ->
                summary.activeTableLocations.any { runCatching { StoragePathUtils.overlaps(folder, it) }.getOrDefault(false) }
            }
        val inspectedFolderPaths = summary.inspectionOutcomes.values.flatten().toSet()
        val otherSkippedFolderPaths =
            summary.storageFolderPaths - tableFolderPaths.toSet() - unmatchedFolderSet - inspectedFolderPaths
        // Several tables can share one location, so path lists are shown once each.
        val unscannedTableLocations = summary.unscannedTableLocations.distinct()

        addReportStart(
            summary.catalog,
            summary.database,
            summary.dryRun,
            summary.catalogTableCount,
            summary.activeTableLocations.size,
            summary.unresolvedTableNames,
            summary.unresolvedTableDetails,
            summary.ownershipChecks,
        )

        add("")
        add("STORAGE")
        add("")
        add("  Database location                                ${summary.discoveredDatabaseLocation}")
        addFoldersScannedUnder(summary.cleanupRoots)
        add("  Folders discovered                               ${summary.storageFolderPaths.size}")
        add("  Folders belonging to verified catalog tables     ${tableFolderPaths.size}")
        add("  Folders protected or skipped for other reasons   ${otherSkippedFolderPaths.size}")
        inspectionOutcomesInReportOrder(summary).forEach { (outcome, paths) ->
            add("  ${inspectionOutcomeLabel(outcome).padEnd(STORAGE_LABEL_WIDTH)}${paths.size}")
        }
        add("  Folders with no verified catalog owner           ${summary.unmatchedFolderPaths.size}")
        add("  ${(if (summary.cleanupRoots.size > 1) "Table locations outside the scanned roots" else "Table locations outside the scanned root").padEnd(STORAGE_LABEL_WIDTH)}${unscannedTableLocations.size}")
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
        addListOrNone(summary.activeTableLocations.distinct().sorted())

        add("")
        add("STORAGE FOLDERS EXCLUDED BY CONFIGURATION")
        add("")
        addListOrNone(summary.excludedPaths.distinct().sorted())

        if (otherSkippedFolderPaths.isNotEmpty()) {
            add("")
            add("STORAGE FOLDERS PROTECTED OR SKIPPED FOR OTHER REASONS")
            add("")
            add("  These folders are not deleted. They are excluded, belong to another database, to a table's")
            add("  write path, or to an unresolved table, are framework working folders, or are too new.")
            add("")
            addListOrNone(otherSkippedFolderPaths)
        }

        val inspectionOutcomes = inspectionOutcomesInReportOrder(summary)
        if (inspectionOutcomes.isNotEmpty()) {
            add("")
            add("STORAGE FOLDERS SKIPPED AFTER INSPECTION")
            add("")
            add("  These folders have no verified catalog owner, but they are not deleted. Inspection found")
            add("  them missing, could not read them, found them empty, or could not establish their age.")
            inspectionOutcomes.forEach { (outcome, paths) ->
                add("")
                add("  ${outcome.code}: ${inspectionOutcomeExplanation(outcome)}")
                add("")
                addListOrNone(paths)
            }
        }

        if (unscannedTableLocations.isNotEmpty()) {
            add("")
            add(if (summary.cleanupRoots.size > 1) "TABLE LOCATIONS OUTSIDE THE SCANNED ROOTS" else "TABLE LOCATIONS OUTSIDE THE SCANNED ROOT")
            add("")
            add("  Storage around these table locations was not checked in this run.")
            add("")
            addListOrNone(unscannedTableLocations)
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
            if (summary.notFoundAtDeletionPaths.isNotEmpty()) {
                add("  Folders not found at deletion                    ${summary.notFoundAtDeletionPaths.size}")
            }
            if (summary.keptAtFinalCheckPaths.isNotEmpty()) {
                add("  Folders kept by the final check                  ${summary.keptAtFinalCheckPaths.size}")
            }
            add("")
            addListOrNone(summary.deletedFolderPaths)
            if (summary.notFoundAtDeletionPaths.isNotEmpty()) {
                add("")
                add("  folder_not_found_at_deletion: passed inspection, but was gone when deletion started.")
                add("  Still counted as a candidate, not as deleted.")
                add("")
                addListOrNone(summary.notFoundAtDeletionPaths)
            }
            if (summary.keptAtFinalCheckPaths.isNotEmpty()) {
                add("")
                add("  folder_kept_at_final_check: another database, catalog or table claimed the folder when")
                add("  the job read their storage again just before deleting. Still counted as a candidate,")
                add("  not as deleted.")
                add("")
                addListOrNone(summary.keptAtFinalCheckPaths)
            }
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

    private fun MutableList<String>.addReportStart(
        catalog: String,
        database: String,
        dryRun: Boolean,
        catalogTableCount: Int,
        verifiedTableCount: Int,
        unresolvedTableNames: List<String>,
        unresolvedTableDetails: Map<String, UnresolvedTableDetail>,
        ownershipChecks: OwnershipChecks,
    ) {
        repeat(BLANK_LINES_AROUND_REPORT) { add("") }

        add(SEPARATOR)
        add(" CLEANUP REPORT - $catalog.$database")
        add(if (dryRun) " Mode: dry run, nothing is deleted" else " Mode: deletion enabled")
        add(SEPARATOR)
        add("")
        add("OWNERSHIP CHECKS")
        add("")
        addOwnershipCheckLines(ownershipChecks)
        add("  See \"What deletion mode can and cannot prove\" in the job README.")
        add("")
        add("CATALOG")
        add("")
        add("  Tables found                                     $catalogTableCount")
        add("  Tables with verified storage locations           $verifiedTableCount")
        add("  Tables whose storage location could not be read  ${unresolvedTableNames.size}")

        if (unresolvedTableNames.isNotEmpty()) {
            add("")
            add("TABLES WHOSE STORAGE LOCATION COULD NOT BE RESOLVED")
            add("")
            add("  These tables exist in the catalog, but their Iceberg metadata or storage")
            add("  location could not be read. A known folder inside the scanned root is not listed as")
            add("  unmatched. It is evidence only, not a verified location.")
            add("")
            unresolvedTableNames.take(MAX_LOG_PATH_SAMPLE_SIZE).forEach { name ->
                add("- $name")
                unresolvedTableDetails[name]?.let { detail ->
                    add("    reason: ${detail.reason.lineSequence().first().trim().take(MAX_REASON_LENGTH)}")
                    detail.knownLocation?.let { add("    known folder: $it") }
                }
            }
            if (unresolvedTableNames.size > MAX_LOG_PATH_SAMPLE_SIZE) {
                add("- ... truncated ${unresolvedTableNames.size - MAX_LOG_PATH_SAMPLE_SIZE} additional table(s)")
            }
        }
    }

    // Says the same as the ownership_checks and ownership_not_checked audit keys of the same database.
    private fun MutableList<String>.addOwnershipCheckLines(checks: OwnershipChecks) {
        addWrapped(
            "Checked",
            listOfNotNull(
                "tables of this database the job can see",
                "their Iceberg write paths",
                "default folders of other databases the job can see".takeIf { checks.otherDatabaseFolders },
                "configured exclusions",
                "tables of other databases the job can see".takeIf { checks.otherTables },
                "same-bucket catalogs of the Spark session".takeIf { checks.otherTables },
                "a final read of other storage right before deleting".takeIf { checks.finalRecheck },
            ),
        )
        addWrapped(
            "Not checked",
            listOfNotNull(
                "default folders of other databases".takeUnless { checks.otherDatabaseFolders },
                "tables of other databases".takeUnless { checks.otherTables },
                "same-bucket catalogs of the Spark session".takeUnless { checks.otherTables },
                "a final read of other storage right before deleting".takeUnless { checks.finalRecheck },
                "catalogs in another bucket or outside the Spark session",
                "tables the job cannot see",
                "tables of nested namespaces",
                "files a table uses outside its own folder",
            ),
        )
    }

    private fun MutableList<String>.addWrapped(
        label: String,
        items: List<String>,
    ) {
        val indent = " ".repeat(OWNERSHIP_LABEL_WIDTH)
        var line = "  $label".padEnd(OWNERSHIP_LABEL_WIDTH)
        items.forEachIndexed { index, item ->
            val text = if (index < items.lastIndex) "$item," else item
            if (index > 0 && line.length + 1 + text.length > MAX_OWNERSHIP_LINE_LENGTH) {
                add(line)
                line = indent + text
            } else {
                line += if (index > 0) " $text" else text
            }
        }
        add(line)
    }

    private fun MutableList<String>.addFoldersScannedUnder(cleanupRoots: List<String>) {
        cleanupRoots.forEachIndexed { index, root ->
            add("  ${(if (index == 0) "Folders scanned under" else "").padEnd(STORAGE_LABEL_WIDTH)}$root")
        }
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

        if (scanPlan.roots.count { it.root.cleanupRoot } > 1) {
            add("  Folders are deleted only under the cleanup roots. Other roots are listed for review only.")
            if (scanPlan.complete) {
                add("  Coverage is complete: every table this job can see is under a cleanup root.")
            } else {
                add("  Coverage is partial: a table this job can see is outside every cleanup root or has an")
                add("  unknown location, or a root could not be listed.")
            }
        } else {
            add("  Folders are deleted only under the cleanup root. Other roots are listed for review only.")
            if (scanPlan.complete) {
                add("  Coverage is complete: every table this job can see is under the cleanup root.")
            } else {
                add("  Coverage is partial: a table this job can see is outside the cleanup root or has an")
                add("  unknown location, or a root could not be listed.")
            }
        }
        add("  Coverage counts only tables this job can see. It is not an ownership check.")
        if (scanPlan.restNamespaceRootStatus != null && scanPlan.restNamespaceRootStatus != "derived") {
            add("  The REST namespace root was not derived: ${scanPlan.restNamespaceRootStatus}.")
        }
        scanPlan.managedRootStatus?.takeIf { it.startsWith(MANAGED_ROOT_REFUSED_PREFIX) }?.let { status ->
            add("  The database folder without .db is not a cleanup root: ${status.removePrefix(MANAGED_ROOT_REFUSED_PREFIX)}.")
        }
        add("")
        val (notScanned, listed) = scanPlan.roots.partition { it.outcome == ScanRootOutcome.NOT_SCANNED }
        (listed + notScanned.take(MAX_LOGGED_NOT_SCANNED_ROOTS)).forEach { root ->
            val status = if (root.outcome == ScanRootOutcome.SCANNED && root.listedFolderCount == 0) "scanned, no folders" else root.status
            // Tables at any depth, so a broad rejected root such as a bucket root would show a misleading count.
            val tableCount = if (root.root.rejection == null) "  visible tables at any depth: ${root.visibleTableCount}" else ""
            add("- ${root.root.location}  ${root.role}  $status$tableCount")
        }
        if (notScanned.size > MAX_LOGGED_NOT_SCANNED_ROOTS) {
            add("- ... ${notScanned.size - MAX_LOGGED_NOT_SCANNED_ROOTS} more root(s) not scanned")
        }

        if (scanPlan.uncoveredTableLocations.isNotEmpty()) {
            add("")
            add("TABLE LOCATIONS OUTSIDE EVERY LISTED ROOT")
            add("")
            addListOrNone(scanPlan.uncoveredTableLocations.distinct())
        }

        add("")
        add("FOLDERS IN REPORT-ONLY ROOTS NOT MATCHED TO A VISIBLE TABLE")
        add("")
        add("  These folders are not deleted. A table this job cannot see may still own them.")
        add("")
        addListOrNone(scanPlan.reportOnlyFolderPaths)
    }

    private fun inspectionOutcomesInReportOrder(summary: CleanupSummary): List<Pair<CandidateInspectionOutcome, List<String>>> =
        CandidateInspectionOutcome.entries.mapNotNull { outcome ->
            summary.inspectionOutcomes[outcome]?.takeIf { it.isNotEmpty() }?.let { outcome to it }
        }

    private fun inspectionOutcomeLabel(outcome: CandidateInspectionOutcome): String =
        when (outcome) {
            CandidateInspectionOutcome.FOLDER_NOT_FOUND -> "Folders no longer present when inspected"
            CandidateInspectionOutcome.STATISTICS_UNAVAILABLE -> "Folders whose contents could not be listed"
            CandidateInspectionOutcome.EMPTY_FOLDER -> "Empty folders"
            CandidateInspectionOutcome.UNKNOWN_AGE -> "Folders of unknown age"
        }

    private fun inspectionOutcomeExplanation(outcome: CandidateInspectionOutcome): String =
        when (outcome) {
            CandidateInspectionOutcome.FOLDER_NOT_FOUND -> "listed under the cleanup root, but not found when inspected."
            CandidateInspectionOutcome.STATISTICS_UNAVAILABLE -> "the contents could not be listed. See the WARN log for the error."
            CandidateInspectionOutcome.EMPTY_FOLDER -> "no objects. Empty folders, including their directory markers, are not deleted."
            CandidateInspectionOutcome.UNKNOWN_AGE -> "at least one object has no valid modification time."
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
            DeletionBlockReason.OTHER_TABLE_UNRESOLVED ->
                listOf(
                    "A table of another database or catalog could not be resolved to its storage, so its files",
                    "could be in any folder. The job log names the table.",
                )
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
        const val STORAGE_LABEL_WIDTH = 49
        const val OWNERSHIP_LABEL_WIDTH = 15
        const val MAX_OWNERSHIP_LINE_LENGTH = 95
        const val MAX_REASON_LENGTH = 300
        const val SEPARATOR = "================================================================"
    }
}
