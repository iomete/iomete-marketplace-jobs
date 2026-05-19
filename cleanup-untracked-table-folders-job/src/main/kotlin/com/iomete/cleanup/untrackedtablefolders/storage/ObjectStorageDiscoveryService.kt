package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.hadoop.fs.Path
import org.jboss.logging.Logger

@ApplicationScoped
class ObjectStorageDiscoveryService {
    private val logger = Logger.getLogger(ObjectStorageDiscoveryService::class.java)

    @Inject
    lateinit var sparkSessionProvider: SparkSessionProvider

    fun listImmediateChildFolders(location: String): List<String> {
        logger.info("Listing immediate child folders under location=$location")

        val spark = sparkSessionProvider.getOrCreate()
        val path = Path(location)
        val fileSystem = path.getFileSystem(spark.sparkContext().hadoopConfiguration())

        return try {
            fileSystem.listStatus(path)
                .filter { it.isDirectory }
                .map { it.path.toString() }
                .sorted()
        } catch (th: Throwable) {
            logger.warn("Failed to list immediate child folders under location=$location", th)
            emptyList()
        }
    }
}
