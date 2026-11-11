package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.candidate.StorageFolderReconciliation
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

data class CandidateDeletionResult(
    val deletedPaths: List<String> = emptyList(),
    val notFoundPaths: List<String> = emptyList(),
)

@ApplicationScoped
class CandidateDeletionGate {

    private val logger = Logger.getLogger(CandidateDeletionGate::class.java)

    @Inject lateinit var config: ApplicationConfig
    @Inject lateinit var catalogDiscoveryService: CatalogDiscoveryService
    @Inject lateinit var objectStorageDeletionService: ObjectStorageDeletionService

    fun deleteCandidates(
        catalog: String,
        database: String,
        reconciliation: StorageFolderReconciliation.DeletionEligible,
        excludedPaths: List<String>,
    ): CandidateDeletionResult {
        if (config.dryRun) {
            return CandidateDeletionResult()
        }

        check(config.deleteEnabled) {
            "delete_enabled must be true before deleting candidate folders"
        }

        if (reconciliation.folders.isEmpty()) {
            return CandidateDeletionResult()
        }

        val currentActiveTableLocations = currentActiveTableLocations(catalog, database)
        val normalizedExcludedPaths = excludedPaths.map { StoragePathUtils.normalizeLocation(it) }

        val deletableFolders =
            reconciliation.folders.filterNot { candidateFolder ->
                claimedByActiveTable(candidateFolder, currentActiveTableLocations) ||
                    overlapsExcludedPath(candidateFolder, normalizedExcludedPaths)
            }

        val (deleted, notFound) =
            objectStorageDeletionService
                .deleteFoldersRecursively(catalog, deletableFolders.map { it.path }.sorted())
                .partition { it.deleted }

        return CandidateDeletionResult(
            deletedPaths = deleted.map { it.path }.sorted(),
            notFoundPaths = notFound.map { it.path }.sorted(),
        )
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
            .flatMap { listOfNotNull(it.location) + it.writeLocations }
            .map { StoragePathUtils.normalizeLocation(it) }
    }

    private fun claimedByActiveTable(
        candidateFolder: StorageFolder,
        currentActiveTableLocations: List<String>,
    ): Boolean {
        val normalizedCandidatePath = StoragePathUtils.normalizeLocation(candidateFolder.path)

        val claimed =
            currentActiveTableLocations.any { activeLocation ->
                StoragePathUtils.overlaps(normalizedCandidatePath, activeLocation)
            }

        if (claimed) {
            logger.warn(
                "Skipping deletion because candidate folder overlaps an active table location: path=${candidateFolder.path}"
            )
        }

        return claimed
    }

    private fun overlapsExcludedPath(
        candidateFolder: StorageFolder,
        normalizedExcludedPaths: List<String>,
    ): Boolean {
        val normalizedCandidatePath = StoragePathUtils.normalizeLocation(candidateFolder.path)

        val excludedPath =
            normalizedExcludedPaths.firstOrNull { excludedPath ->
                StoragePathUtils.overlaps(normalizedCandidatePath, excludedPath)
            } ?: return false

        logger.warn(
            "Skipping deletion because candidate folder overlaps an excluded path: path=${candidateFolder.path}, excludedPath=$excludedPath"
        )

        return true
    }
}
