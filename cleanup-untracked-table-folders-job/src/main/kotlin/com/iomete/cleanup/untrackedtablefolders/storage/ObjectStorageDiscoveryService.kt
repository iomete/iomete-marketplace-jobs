package com.iomete.cleanup.untrackedtablefolders.storage

import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

data class StorageFolder(
    val path: String,
    val modificationTimeMillis: Long,
)

data class StorageSizeStats(
    val objectCount: Long,
    val totalSizeBytes: Long,
) {
    companion object {
        val ZERO = StorageSizeStats(objectCount = 0, totalSizeBytes = 0)
    }
}

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

        return try {
            catalogFileSystemProvider.withFileSystem(catalog, location) { fileSystem, path ->
                fileSystem
                    .listStatus(path)
                    .filter { it.isDirectory }
                    .map {
                        StorageFolder(
                            path = it.path.toString(),
                            modificationTimeMillis = it.modificationTime,
                        )
                    }
                    .sortedBy { it.path }
            }
        } catch (th: CatalogStorageConfigurationException) {
            throw th
        } catch (th: Throwable) {
            throw IllegalStateException(
                "Failed to list immediate child folders for catalog=$catalog under location=$location",
                th,
            )
        }
    }

    fun collectSizeStats(
        catalog: String,
        folderPaths: List<String>,
    ): StorageSizeStats {
        if (folderPaths.isEmpty()) {
            return StorageSizeStats.ZERO
        }

        var objectCount = 0L
        var totalSizeBytes = 0L

        folderPaths.sorted().forEach { folderPath ->
            logger.info("Collecting size statistics for candidate folder: $folderPath")

            val folderStats =
                try {
                    catalogFileSystemProvider.withFileSystem(catalog, folderPath) { fileSystem, path ->
                        val files = fileSystem.listFiles(path, true)

                        var folderObjectCount = 0L
                        var folderSizeBytes = 0L

                        while (files.hasNext()) {
                            val fileStatus = files.next()
                            folderObjectCount += 1
                            folderSizeBytes += fileStatus.len
                        }

                        StorageSizeStats(
                            objectCount = folderObjectCount,
                            totalSizeBytes = folderSizeBytes,
                        )
                    }
                } catch (th: CatalogStorageConfigurationException) {
                    throw th
                } catch (th: Throwable) {
                    throw IllegalStateException(
                        "Failed to collect size statistics for catalog=$catalog, candidate folder=$folderPath",
                        th,
                    )
                }

            logger.info(
                "Collected size statistics for candidate folder=$folderPath: objectCount=${folderStats.objectCount}, totalSizeBytes=${folderStats.totalSizeBytes}"
            )

            objectCount += folderStats.objectCount
            totalSizeBytes += folderStats.totalSizeBytes
        }

        return StorageSizeStats(
            objectCount = objectCount,
            totalSizeBytes = totalSizeBytes,
        )
    }
}
