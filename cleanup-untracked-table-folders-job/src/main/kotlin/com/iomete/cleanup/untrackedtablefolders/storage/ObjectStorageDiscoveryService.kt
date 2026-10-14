package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.jboss.logging.Logger

data class StorageFolder(
    val path: String,
)

data class StorageSizeStats(
    val objectCount: Long,
    val totalSizeBytes: Long,
    val newestObjectModificationTimeMillis: Long? = null,
) {
    companion object {
        val ZERO = StorageSizeStats(objectCount = 0, totalSizeBytes = 0)
    }
}

data class SizeStatsBatch(
    val statsByFolder: Map<String, StorageSizeStats>,
    val failures: Map<String, Throwable>,
)

@ApplicationScoped
class ObjectStorageDiscoveryService {
    private val logger = Logger.getLogger(ObjectStorageDiscoveryService::class.java)

    @Inject
    lateinit var catalogFileSystemProvider: CatalogFileSystemProvider

    fun listImmediateChildFolders(
        catalog: String,
        location: String,
    ): List<StorageFolder> {
        logger.info("Listing immediate child folders for catalog=$catalog under location=$location")

        return catalogFileSystemProvider.withFileSystem(
            catalog = catalog,
            operation = "list immediate child folders",
            locations = listOf(location),
        ) { fileSystem ->
            childFolders(fileSystem, Path(location))
        }
    }

    fun listImmediateChildFoldersIfPresent(
        catalog: String,
        location: String,
    ): List<StorageFolder>? {
        logger.info("Listing immediate child folders for catalog=$catalog under location=$location")

        return catalogFileSystemProvider.withFileSystem(
            catalog = catalog,
            operation = "list immediate child folders",
            locations = listOf(location),
        ) { fileSystem ->
            val path = Path(location)
            if (fileSystem.exists(path)) childFolders(fileSystem, path) else null
        }
    }

    private fun childFolders(
        fileSystem: FileSystem,
        path: Path,
    ): List<StorageFolder> =
        fileSystem
            .listStatus(path)
            .filter { it.isDirectory }
            .map { StorageFolder(path = it.path.toString()) }
            .sortedBy { it.path }

    fun collectSizeStatsPerFolder(
        catalog: String,
        folderPaths: List<String>,
    ): SizeStatsBatch {
        if (folderPaths.isEmpty()) {
            return SizeStatsBatch(emptyMap(), emptyMap())
        }

        val sortedFolderPaths = folderPaths.sorted()

        return catalogFileSystemProvider.withFileSystem(
            catalog = catalog,
            operation = "collect size statistics",
            locations = sortedFolderPaths,
        ) { fileSystem ->
            val statsByFolder = linkedMapOf<String, StorageSizeStats>()
            val failures = linkedMapOf<String, Throwable>()

            sortedFolderPaths.forEach { folderPath ->
                logger.info("Collecting size statistics for candidate folder: $folderPath")

                try {
                    val folderStats =
                        catalogFileSystemProvider.runOperation(catalog, "collect size statistics", folderPath) {
                            sizeOf(fileSystem, Path(folderPath))
                        }

                    logger.info(
                        "Collected size statistics for candidate folder=$folderPath: objectCount=${folderStats.objectCount}, totalSizeBytes=${folderStats.totalSizeBytes}"
                    )
                    statsByFolder[folderPath] = folderStats
                } catch (th: CatalogStorageOperationException) {
                    failures[folderPath] = th
                }
            }

            SizeStatsBatch(statsByFolder, failures)
        }
    }

    fun collectSizeStats(
        catalog: String,
        folderPaths: List<String>,
    ): StorageSizeStats {
        val batch = collectSizeStatsPerFolder(catalog, folderPaths)

        batch.failures.values.firstOrNull()?.let { throw it }

        return batch.statsByFolder.values.fold(StorageSizeStats.ZERO) { total, current ->
            StorageSizeStats(
                objectCount = total.objectCount + current.objectCount,
                totalSizeBytes = total.totalSizeBytes + current.totalSizeBytes,
            )
        }
    }

    private fun sizeOf(
        fileSystem: FileSystem,
        path: Path,
    ): StorageSizeStats {
        val files = fileSystem.listFiles(path, true)

        var objectCount = 0L
        var totalSizeBytes = 0L
        var newestModificationTimeMillis = Long.MIN_VALUE
        var hasInvalidModificationTime = false

        while (files.hasNext()) {
            val fileStatus = files.next()
            objectCount += 1
            totalSizeBytes += fileStatus.len
            newestModificationTimeMillis = maxOf(newestModificationTimeMillis, fileStatus.modificationTime)
            if (fileStatus.modificationTime <= 0) {
                hasInvalidModificationTime = true
            }
        }

        return StorageSizeStats(
            objectCount = objectCount,
            totalSizeBytes = totalSizeBytes,
            // One object of unknown age makes the folder's age unknown, whatever the other objects report.
            newestObjectModificationTimeMillis =
                newestModificationTimeMillis.takeIf { objectCount > 0 && !hasInvalidModificationTime },
        )
    }
}
