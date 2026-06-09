package com.iomete.cleanup.untrackedtablefolders.logging

import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

data class CleanupSummary(
    val catalog: String,
    val database: String,
    val discoveredDatabaseLocation: String?,
    val storageScanLocation: String,
    val activeTableLocations: List<String>,
    val storageFolderPaths: List<String>,
    val excludedPaths: List<String>,
    val candidateFolderPaths: List<String>,
    val candidateSizeStats: StorageSizeStats?,
    val deletedFolderPaths: List<String>,
    val deletedSizeStats: StorageSizeStats?,
)

@ApplicationScoped
class CleanupSummaryLogger {

    private val logger = Logger.getLogger(CleanupSummaryLogger::class.java)

    fun logCleanupSummary(summary: CleanupSummary) {
        val candidateFolderSet = summary.candidateFolderPaths.toSet()
        val protectedFolderPaths = summary.activeTableLocations.toSet()
        val nonCandidateStorageFolderPaths =
            summary.storageFolderPaths.filter { it !in candidateFolderSet }.sorted()

        logBlankLines(3)
        logger.info("========== Cleanup Untracked Table Folders Summary ==========")
        logger.info("Catalog: ${summary.catalog}")
        logger.info("Configured database: ${summary.database}")
        logger.info("Discovered database location: ${summary.discoveredDatabaseLocation}")
        logger.info("Object storage scan root: ${summary.storageScanLocation}")
        logger.info("Protected catalog active table location count: ${summary.activeTableLocations.size}")
        logger.info("Immediate child storage folders scanned: ${summary.storageFolderPaths.size}")
        logger.info("Untracked candidate folder count: ${summary.candidateFolderPaths.size}")
        logger.info(
            if (summary.candidateSizeStats != null) {
                "Estimated candidate size: ${formatBytes(summary.candidateSizeStats.totalSizeBytes)} across ${summary.candidateSizeStats.objectCount} object(s)"
            } else {
                "Estimated candidate size: skipped because collect_size_statistics=false"
            }
        )
        logger.info("Deleted untracked folder count: ${summary.deletedFolderPaths.size}")
        logger.info(
            if (summary.deletedSizeStats != null) {
                "Deleted size: ${formatBytes(summary.deletedSizeStats.totalSizeBytes)} across ${summary.deletedSizeStats.objectCount} object(s)"
            } else {
                "Deleted size: skipped because collect_size_statistics=false"
            }
        )
        logger.info("Deletion performed: ${summary.deletedFolderPaths.isNotEmpty()}")
        logger.info("Protected catalog active table locations:")
        logListOrNone(protectedFolderPaths.sorted())
        logger.info("Effective excluded paths:")
        logListOrNone(summary.excludedPaths.sorted())
        logger.info("Storage folders not selected as candidates:")
        logListOrNone(nonCandidateStorageFolderPaths)
        logger.info("Untracked candidate folders selected for cleanup:")
        logListOrNone(summary.candidateFolderPaths)
        logger.info("Deleted folders:")
        logListOrNone(summary.deletedFolderPaths)
        logger.info("============================================================")
        logBlankLines(3)
    }

    private fun logBlankLines(count: Int) {
        repeat(count) { logger.info("") }
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

    private fun logListOrNone(values: List<String>) {
        if (values.isEmpty()) {
            logger.info("- none")
        } else {
            values.take(MAX_LOG_PATH_SAMPLE_SIZE).forEach { value ->
                logger.info("- $value")
            }

            if (values.size > MAX_LOG_PATH_SAMPLE_SIZE) {
                logger.info("- ... truncated ${values.size - MAX_LOG_PATH_SAMPLE_SIZE} additional path(s)")
            }
        }
    }

    private companion object {
        const val MAX_LOG_PATH_SAMPLE_SIZE = 100
    }
}
