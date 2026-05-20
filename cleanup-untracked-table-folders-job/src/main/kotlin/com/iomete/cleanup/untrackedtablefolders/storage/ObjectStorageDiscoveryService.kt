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
}
