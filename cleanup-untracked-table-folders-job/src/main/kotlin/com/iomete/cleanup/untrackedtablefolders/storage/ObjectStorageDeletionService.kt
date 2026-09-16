package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.hadoop.fs.Path
import org.jboss.logging.Logger

data class DeletedStorageFolder(
    val path: String,
    val deleted: Boolean,
)

@ApplicationScoped
class ObjectStorageDeletionService {
    private val logger = Logger.getLogger(ObjectStorageDeletionService::class.java)

    @Inject
    lateinit var catalogFileSystemProvider: CatalogFileSystemProvider

    /** Opens one filesystem for the whole batch and stops at the first failure. */
    fun deleteFoldersRecursively(
        catalog: String,
        locations: List<String>,
    ): List<DeletedStorageFolder> {
        if (locations.isEmpty()) {
            return emptyList()
        }

        return catalogFileSystemProvider.withFileSystem(
            catalog = catalog,
            operation = "delete storage folder recursively",
            locations = locations,
        ) { fileSystem ->
            locations.map { location ->
                logger.warn("Deleting storage folder recursively: catalog=$catalog, location=$location")

                val deleted =
                    catalogFileSystemProvider.runOperation(catalog, "delete storage folder recursively", location) {
                        val path = Path(location)
                        if (!fileSystem.exists(path)) {
                            logger.warn("Storage folder does not exist, skipping delete: location=$location")
                            false
                        } else {
                            fileSystem.delete(path, true)
                        }
                    }

                if (!deleted) {
                    logger.warn("Storage folder was not deleted: location=$location")
                } else {
                    logger.warn("Storage folder deleted successfully: location=$location")
                }

                DeletedStorageFolder(path = location, deleted = deleted)
            }
        }
    }
}
