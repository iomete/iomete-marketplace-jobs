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

    /**
     * Size statistics are reporting only; they never gate deletion, so a candidate folder that
     * cannot be read is recorded as an unknown size and the run continues.
     *
     * Two failures are not tolerated, because both mean the job cannot reach the catalog's
     * storage at all and a run that reported success would be misleading:
     *  - [CatalogStorageConfigurationException], which is a configuration defect, and
     *  - every candidate folder failing, which is the signature of a wrong endpoint or
     *    wrong credentials rather than one bad folder.
     */
    fun collectPerFolder(
        catalog: String,
        candidateFolderPaths: List<String>,
    ): Map<String, StorageSizeStats> {
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

        val batch = objectStorageDiscoveryService.collectSizeStatsPerFolder(catalog, candidateFolderPaths)

        batch.failures.forEach { (candidateFolderPath, th) ->
            logger.warn(
                "Failed to collect size statistics for candidate folder; recording unknown size and continuing without aborting cleanup: path=$candidateFolderPath",
                th,
            )
        }

        if (batch.failures.size == candidateFolderPaths.size) {
            throw IllegalStateException(
                "Failed to collect size statistics for all ${candidateFolderPaths.size} candidate folder(s) of catalog=$catalog. " +
                    "Treating this as a storage access failure rather than unknown sizes.",
                batch.failures.values.first(),
            )
        }

        if (batch.failures.isNotEmpty()) {
            logger.warn(
                "Size statistics collection failed for ${batch.failures.size} of ${candidateFolderPaths.size} candidate folder(s). Candidate and deleted size audit fields exclude the failed folders."
            )
        }

        return batch.statsByFolder
    }

    fun sum(stats: Iterable<StorageSizeStats>): StorageSizeStats =
        stats.fold(StorageSizeStats.ZERO) { total, current ->
            StorageSizeStats(
                objectCount = total.objectCount + current.objectCount,
                totalSizeBytes = total.totalSizeBytes + current.totalSizeBytes,
            )
        }
}
