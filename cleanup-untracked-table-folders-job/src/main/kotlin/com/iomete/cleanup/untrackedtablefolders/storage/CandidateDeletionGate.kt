package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

@ApplicationScoped
class CandidateDeletionGate {

    private val logger = Logger.getLogger(CandidateDeletionGate::class.java)

    @Inject lateinit var config: ApplicationConfig
    @Inject lateinit var catalogDiscoveryService: CatalogDiscoveryService
    @Inject lateinit var objectStorageDeletionService: ObjectStorageDeletionService

    /**
     * Returns the sorted list of folder paths that were actually deleted.
     *
     * Behavior:
     *  - `dry_run=true` → returns emptyList, no catalog or storage I/O.
     *  - `dry_run=false` AND `delete_enabled=false` → throws IllegalStateException.
     *  - `dry_run=false` AND `delete_enabled=true` → re-queries the catalog for the
     *    current active table locations and skips any candidate that now contains
     *    (or is) an active table location. Surviving candidates are deleted
     *    recursively via [ObjectStorageDeletionService]. The recheck is the
     *    TOCTOU guard between detection and deletion.
     */
    fun deleteCandidates(
        catalog: String,
        database: String,
        candidateFolders: List<StorageFolder>,
    ): List<String> {
        if (config.dryRun) {
            return emptyList()
        }

        check(config.deleteEnabled) {
            "delete_enabled must be true before deleting candidate folders"
        }

        if (candidateFolders.isEmpty()) {
            return emptyList()
        }

        val currentActiveTableLocations = currentActiveTableLocations(catalog, database)

        return candidateFolders
            .mapNotNull { candidateFolder ->
                deleteIfNotClaimedByActiveTable(candidateFolder, currentActiveTableLocations)
            }
            .sorted()
    }

    private fun currentActiveTableLocations(catalog: String, database: String): List<String> =
        catalogDiscoveryService
            .discoverDatabase(catalog = catalog, database = database)
            .tables
            .mapNotNull { it.location }
            .map { StoragePathUtils.normalizeLocation(it) }

    private fun deleteIfNotClaimedByActiveTable(
        candidateFolder: StorageFolder,
        currentActiveTableLocations: List<String>,
    ): String? {
        val normalizedCandidatePath = StoragePathUtils.normalizeLocation(candidateFolder.path)

        val claimedByActiveTable =
            currentActiveTableLocations.any { activeLocation ->
                StoragePathUtils.isSameOrChildLocation(
                    candidateLocation = activeLocation,
                    rootLocation = normalizedCandidatePath,
                )
            }

        if (claimedByActiveTable) {
            logger.warn(
                "Skipping deletion because candidate folder is or contains an active table location: path=${candidateFolder.path}"
            )
            return null
        }

        return objectStorageDeletionService
            .deleteFolderRecursively(candidateFolder.path)
            .takeIf { it.deleted }
            ?.path
    }
}
