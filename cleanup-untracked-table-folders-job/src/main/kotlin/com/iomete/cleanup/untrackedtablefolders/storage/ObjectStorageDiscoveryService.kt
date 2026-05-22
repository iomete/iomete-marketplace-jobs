package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.hadoop.fs.Path
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
    lateinit var sparkSessionProvider: SparkSessionProvider

    fun listImmediateChildFolders(location: String): List<StorageFolder> {
        logger.info("Listing immediate child folders under location=$location")

        val spark = sparkSessionProvider.getOrCreate()
        val path = Path(location)

        return try {
            val fileSystem = path.getFileSystem(spark.sparkContext().hadoopConfiguration())

            fileSystem.listStatus(path)
                .filter { it.isDirectory }
                .map {
                    StorageFolder(
                        path = it.path.toString(),
                        modificationTimeMillis = it.modificationTime,
                    )
                }
                .sortedBy { it.path }
        } catch (th: Throwable) {
            throw IllegalStateException(
                "Failed to list immediate child folders under location=$location",
                th,
            )
        }
    }

    fun collectSizeStats(folderPaths: List<String>): StorageSizeStats {
        if (folderPaths.isEmpty()) {
            return StorageSizeStats.ZERO
        }

        val spark = sparkSessionProvider.getOrCreate()
        var objectCount = 0L
        var totalSizeBytes = 0L

        folderPaths.sorted().forEach { folderPath ->
            logger.info("Collecting size statistics for candidate folder: $folderPath")

            try {
                val path = Path(folderPath)
                val fileSystem = path.getFileSystem(spark.sparkContext().hadoopConfiguration())
                val files = fileSystem.listFiles(path, true)

                var folderObjectCount = 0L
                var folderSizeBytes = 0L

                while (files.hasNext()) {
                    val fileStatus = files.next()
                    folderObjectCount += 1
                    folderSizeBytes += fileStatus.len
                }

                logger.info(
                    "Collected size statistics for candidate folder=$folderPath: objectCount=$folderObjectCount, totalSizeBytes=$folderSizeBytes"
                )

                objectCount += folderObjectCount
                totalSizeBytes += folderSizeBytes
            } catch (th: Throwable) {
                throw IllegalStateException(
                    "Failed to collect size statistics for candidate folder=$folderPath",
                    th,
                )
            }
        }

        return StorageSizeStats(
            objectCount = objectCount,
            totalSizeBytes = totalSizeBytes,
        )
    }
}
