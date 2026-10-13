package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.jboss.logging.Logger

data class DeletedStorageFolder(
    val path: String,
    val deleted: Boolean,
)

class PartialDeletionException(
    val deletedPaths: List<String>,
    val failedPath: String?,
    cause: Throwable,
) : IllegalStateException(
    "Folder deletion stopped after ${deletedPaths.size} confirmed deletion(s)" +
        (failedPath?.let { "; the state of $it is unknown" } ?: "") +
        ": ${cause.message}",
    cause,
)

private class FolderDeletionFailure(
    val path: String?,
    val cause: Throwable,
)

@ApplicationScoped
class ObjectStorageDeletionService {
    private val logger = Logger.getLogger(ObjectStorageDeletionService::class.java)

    @Inject
    lateinit var catalogFileSystemProvider: CatalogFileSystemProvider

    fun deleteFoldersRecursively(
        catalog: String,
        locations: List<String>,
    ): List<DeletedStorageFolder> {
        if (locations.isEmpty()) {
            return emptyList()
        }

        // Keep partial deletion state outside withFileSystem because operation failures are wrapped.
        val results = mutableListOf<DeletedStorageFolder>()
        var failure: FolderDeletionFailure? = null

        try {
            catalogFileSystemProvider.withFileSystem(
                catalog = catalog,
                operation = "delete storage folder recursively",
                locations = locations,
            ) { fileSystem ->
                for (location in locations) {
                    val folderFailure = deleteFolder(catalog, fileSystem, location, results)
                    if (folderFailure != null) {
                        failure = folderFailure
                        break
                    }
                }
            }
        } catch (th: Throwable) {
            if (results.isEmpty() && failure == null) {
                throw th
            }
            failure = failure ?: FolderDeletionFailure(path = null, cause = th)
        }

        failure?.let {
            throw PartialDeletionException(
                deletedPaths = results.filter { result -> result.deleted }.map { result -> result.path },
                failedPath = it.path,
                cause = it.cause,
            )
        }

        return results
    }

    private fun deleteFolder(
        catalog: String,
        fileSystem: FileSystem,
        location: String,
        results: MutableList<DeletedStorageFolder>,
    ): FolderDeletionFailure? {
        logger.warn("Deleting storage folder recursively: catalog=$catalog, location=$location")

        val deleted =
            try {
                catalogFileSystemProvider.runOperation(catalog, "delete storage folder recursively", location) {
                    val path = Path(location)
                    if (!fileSystem.exists(path)) {
                        logger.warn("Storage folder does not exist, skipping delete: location=$location")
                        null
                    } else {
                        fileSystem.delete(path, true)
                    }
                }
            } catch (th: CatalogStorageOperationException) {
                return FolderDeletionFailure(path = location, cause = th)
            }

        return when (deleted) {
            null -> {
                logger.warn("Storage folder was not deleted: location=$location")
                results += DeletedStorageFolder(path = location, deleted = false)
                null
            }

            true -> {
                logger.warn("Storage folder deleted successfully: location=$location")
                results += DeletedStorageFolder(path = location, deleted = true)
                null
            }

            false ->
                FolderDeletionFailure(
                    path = location,
                    cause = IllegalStateException("Recursive delete returned false for an existing folder; its state is unknown: location=$location"),
                )
        }
    }
}
