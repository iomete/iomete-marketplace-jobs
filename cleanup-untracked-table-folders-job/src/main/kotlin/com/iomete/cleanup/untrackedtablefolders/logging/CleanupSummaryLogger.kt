package com.iomete.cleanup.untrackedtablefolders.logging

import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import jakarta.enterprise.context.ApplicationScoped
import org.jboss.logging.Logger

@ApplicationScoped
class CleanupSummaryLogger {

    private val logger = Logger.getLogger(CleanupSummaryLogger::class.java)

    fun logCleanupSummary(
        catalog: String,
        database: String,
        discoveredDatabaseLocation: String?,
        storageScanLocation: String,
        activeTableLocations: List<String>,
        storageFolderPaths: List<String>,
        excludedPaths: List<String>,
        candidateFolderPaths: List<String>,
        candidateSizeStats: StorageSizeStats?,
        deletedFolderPaths: List<String>,
        deletedSizeStats: StorageSizeStats?,
    ) {
        val candidateFolderSet = candidateFolderPaths.toSet()
        val protectedFolderPaths = activeTableLocations.toSet()
        val nonCandidateStorageFolderPaths =
            storageFolderPaths.filter { it !in candidateFolderSet }.sorted()

        logBlankLines(3)
        logger.info("========== Cleanup Untracked Table Folders Summary ==========")
        logger.info("Catalog: $catalog")
        logger.info("Configured database: $database")
        logger.info("Discovered database location: $discoveredDatabaseLocation")
        logger.info("Object storage scan root: $storageScanLocation")
        logger.info("Protected catalog active table location count: ${activeTableLocations.size}")
        logger.info("Immediate child storage folders scanned: ${storageFolderPaths.size}")
        logger.info("Untracked candidate folder count: ${candidateFolderPaths.size}")
        logger.info(
            if (candidateSizeStats != null) {
                "Estimated candidate size: ${formatBytes(candidateSizeStats.totalSizeBytes)} across ${candidateSizeStats.objectCount} object(s)"
            } else {
                "Estimated candidate size: skipped because collect_size_statistics=false"
            }
        )
        logger.info("Deleted untracked folder count: ${deletedFolderPaths.size}")
        logger.info(
            if (deletedSizeStats != null) {
                "Deleted size: ${formatBytes(deletedSizeStats.totalSizeBytes)} across ${deletedSizeStats.objectCount} object(s)"
            } else {
                "Deleted size: skipped because collect_size_statistics=false"
            }
        )
        logger.info("Deletion performed: ${deletedFolderPaths.isNotEmpty()}")
        logger.info("Protected catalog active table locations:")
        logListOrNone(protectedFolderPaths.sorted())
        logger.info("Effective excluded paths:")
        logListOrNone(excludedPaths.sorted())
        logger.info("Storage folders not selected as candidates:")
        logListOrNone(nonCandidateStorageFolderPaths)
        logger.info("Untracked candidate folders selected for cleanup:")
        logListOrNone(candidateFolderPaths)
        logger.info("Deleted folders:")
        logListOrNone(deletedFolderPaths)
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
