package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
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

    fun deleteFolderRecursively(
        catalog: String,
        location: String,
    ): DeletedStorageFolder {
        logger.warn("Deleting storage folder recursively: catalog=$catalog, location=$location")

        val deleted =
            try {
                catalogFileSystemProvider.withFileSystem(catalog, location) { fileSystem, path ->
                    if (!fileSystem.exists(path)) {
                        logger.warn("Storage folder does not exist, skipping delete: location=$location")
                        false
                    } else {
                        fileSystem.delete(path, true)
                    }
                }
            } catch (th: CatalogStorageConfigurationException) {
                throw th
            } catch (th: Throwable) {
                throw IllegalStateException(
                    "Failed to delete storage folder recursively: catalog=$catalog, location=$location",
                    th,
                )
            }

        if (!deleted) {
            logger.warn("Storage folder was not deleted: location=$location")
        } else {
            logger.warn("Storage folder deleted successfully: location=$location")
        }

        return DeletedStorageFolder(
            path = location,
            deleted = deleted,
        )
    }
}
