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

        val deletableFolders =
            candidateFolders.filterNot { candidateFolder ->
                claimedByActiveTable(candidateFolder, currentActiveTableLocations)
            }

        return objectStorageDeletionService
            .deleteFoldersRecursively(catalog, deletableFolders.map { it.path }.sorted())
            .filter { it.deleted }
            .map { it.path }
            .sorted()
    }

    private fun currentActiveTableLocations(catalog: String, database: String): List<String> {
        val discoveredDatabase = catalogDiscoveryService.discoverDatabase(catalog = catalog, database = database)

        // A table that became unresolved since discovery leaves an unknown location behind, so no
        // candidate in this database can still be proven untracked.
        check(discoveredDatabase.unresolvedTables.isEmpty()) {
            "Refusing to delete: ${discoveredDatabase.unresolvedTables.size} catalog table(s) could not be resolved to a " +
                "storage location during the pre-delete recheck. catalog=$catalog, database=$database"
        }

        return discoveredDatabase
            .tables
            .mapNotNull { it.location }
            .map { StoragePathUtils.normalizeLocation(it) }
    }

    private fun claimedByActiveTable(
        candidateFolder: StorageFolder,
        currentActiveTableLocations: List<String>,
    ): Boolean {
        val normalizedCandidatePath = StoragePathUtils.normalizeLocation(candidateFolder.path)

        val claimed =
            currentActiveTableLocations.any { activeLocation ->
                StoragePathUtils.isSameOrChildLocation(
                    candidateLocation = activeLocation,
                    rootLocation = normalizedCandidatePath,
                )
            }

        if (claimed) {
            logger.warn(
                "Skipping deletion because candidate folder is or contains an active table location: path=${candidateFolder.path}"
            )
        }

        return claimed
    }
}
