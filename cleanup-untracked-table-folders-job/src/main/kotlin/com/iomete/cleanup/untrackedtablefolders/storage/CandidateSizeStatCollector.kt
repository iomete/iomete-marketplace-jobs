package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

@ApplicationScoped
class CandidateSizeStatCollector {

    private val logger = Logger.getLogger(CandidateSizeStatCollector::class.java)

    @Inject lateinit var config: ApplicationConfig
    @Inject lateinit var objectStorageDiscoveryService: ObjectStorageDiscoveryService

    fun collectPerFolder(candidateFolderPaths: List<String>): Map<String, StorageSizeStats> {
        if (candidateFolderPaths.isEmpty()) {
            return emptyMap()
        }

        if (!config.collectSizeStatistics) {
            logger.info(
                "Skipping size statistics because collect_size_statistics=false. Candidate and deleted size audit fields will be NULL."
            )
            return emptyMap()
        }

        logger.info(
            "Collecting size statistics for ${candidateFolderPaths.size} candidate folder(s). This may take time for folders with many objects. To skip this step, set collect_size_statistics=false."
        )

        val result = mutableMapOf<String, StorageSizeStats>()
        var failedFolderCount = 0

        candidateFolderPaths.forEach { candidateFolderPath ->
            try {
                result[candidateFolderPath] =
                    objectStorageDiscoveryService.collectSizeStats(listOf(candidateFolderPath))
            } catch (th: Throwable) {
                failedFolderCount += 1
                logger.warn(
                    "Failed to collect size statistics for candidate folder; recording unknown size and continuing without aborting cleanup: path=$candidateFolderPath",
                    th,
                )
            }
        }

        if (failedFolderCount > 0) {
            logger.warn(
                "Size statistics collection failed for $failedFolderCount of ${candidateFolderPaths.size} candidate folder(s). Candidate and deleted size audit fields exclude the failed folders."
            )
        }

        return result
    }

    fun sum(stats: Iterable<StorageSizeStats>): StorageSizeStats =
        stats.fold(StorageSizeStats.ZERO) { total, current ->
            StorageSizeStats(
                objectCount = total.objectCount + current.objectCount,
                totalSizeBytes = total.totalSizeBytes + current.totalSizeBytes,
            )
        }
}
