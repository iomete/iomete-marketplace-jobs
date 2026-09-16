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

    /** Collects optional size statistics without letting isolated folder failures abort the run. */
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

        val result = mutableMapOf<String, StorageSizeStats>()
        val failures = mutableListOf<Throwable>()

        candidateFolderPaths.forEach { candidateFolderPath ->
            try {
                result[candidateFolderPath] =
                    objectStorageDiscoveryService.collectSizeStats(catalog, listOf(candidateFolderPath))
            } catch (th: CatalogStorageConfigurationException) {
                throw th
            } catch (th: Throwable) {
                failures += th
                logger.warn(
                    "Failed to collect size statistics for candidate folder; recording unknown size and continuing without aborting cleanup: path=$candidateFolderPath",
                    th,
                )
            }
        }

        if (failures.size == candidateFolderPaths.size) {
            throw IllegalStateException(
                "Failed to collect size statistics for all ${candidateFolderPaths.size} candidate folder(s) of catalog=$catalog. " +
                    "Treating this as a storage access failure rather than unknown sizes.",
                failures.first(),
            )
        }

        if (failures.isNotEmpty()) {
            logger.warn(
                "Size statistics collection failed for ${failures.size} of ${candidateFolderPaths.size} candidate folder(s). Candidate and deleted size audit fields exclude the failed folders."
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
