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

    fun collectPerFolder(
        catalog: String,
        candidateFolderPaths: List<String>,
    ): CandidateFolderStatistics {
        if (candidateFolderPaths.isEmpty()) {
            return CandidateFolderStatistics.EMPTY
        }

        logger.info(
            "Collecting size and age statistics for ${candidateFolderPaths.size} candidate folder(s). This may take time for folders with many objects."
        )

        val batch = objectStorageDiscoveryService.collectSizeStatsPerFolder(catalog, candidateFolderPaths)

        batch.failures.forEach { (candidateFolderPath, th) ->
            logger.warn("Failed to collect size statistics for candidate folder: path=$candidateFolderPath", th)
        }

        if (batch.absentFolderPaths.isNotEmpty()) {
            logger.info(
                "${batch.absentFolderPaths.size} of ${candidateFolderPaths.size} candidate folder(s) no longer exist and are not cleanup candidates."
            )
        }

        // Confirmed absence is not an access failure, so only folders that still exist count toward the all-failed check.
        val existingFolderCount = candidateFolderPaths.size - batch.absentFolderPaths.size

        if (batch.failures.isNotEmpty() && batch.failures.size == existingFolderCount) {
            throw IllegalStateException(
                "Failed to collect size statistics for all $existingFolderCount candidate folder(s) that still exist for catalog=$catalog. " +
                    "Treating this as a storage access failure rather than unknown sizes.",
                batch.failures.values.first(),
            )
        }

        if (batch.failures.isNotEmpty()) {
            logger.warn(
                "Size statistics collection failed for ${batch.failures.size} of $existingFolderCount existing candidate folder(s). " +
                    "Continuing: these folders are skipped as statistics_unavailable and excluded from the candidate and deleted size audit fields."
            )
        }

        return CandidateFolderStatistics(batch.statsByFolder, batch.absentFolderPaths)
    }

    fun sum(stats: Iterable<StorageSizeStats>): StorageSizeStats =
        stats.fold(StorageSizeStats.ZERO) { total, current ->
            StorageSizeStats(
                objectCount = total.objectCount + current.objectCount,
                totalSizeBytes = total.totalSizeBytes + current.totalSizeBytes,
            )
        }
}
