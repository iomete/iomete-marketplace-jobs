package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
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
    lateinit var sparkSessionProvider: SparkSessionProvider

    fun deleteFolderRecursively(location: String): DeletedStorageFolder {
        logger.warn("Deleting storage folder recursively: location=$location")

        val spark = sparkSessionProvider.getOrCreate()
        val path = Path(location)

        val deleted =
            try {
                val fileSystem = path.getFileSystem(spark.sparkContext().hadoopConfiguration())

                if (!fileSystem.exists(path)) {
                    logger.warn("Storage folder does not exist, skipping delete: location=$location")
                    false
                } else {
                    fileSystem.delete(path, true)
                }
            } catch (th: Throwable) {
                throw IllegalStateException("Failed to delete storage folder recursively: location=$location", th)
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
