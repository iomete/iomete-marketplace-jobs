package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.jboss.logging.Logger

/** Provides isolated filesystem access using catalog-specific storage configuration. */
@ApplicationScoped
class CatalogFileSystemProvider {
    private val logger = Logger.getLogger(CatalogFileSystemProvider::class.java)

    @Inject
    lateinit var sparkSessionProvider: SparkSessionProvider

    @Inject
    lateinit var catalogStorageConfigResolver: CatalogStorageConfigResolver

    fun <T> withFileSystem(
        catalog: String,
        location: String,
        block: (FileSystem, Path) -> T,
    ): T {
        val path = Path(location)
        val configuration = buildConfiguration(catalog, path)

        return FileSystem.newInstance(path.toUri(), configuration).use { fileSystem ->
            block(fileSystem, path)
        }
    }

    internal fun buildConfiguration(
        catalog: String,
        path: Path,
    ): Configuration = applyCatalogStorageConfig(catalog, path, sparkHadoopConfiguration())

    /** Applies catalog storage settings to a copy of the base configuration. */
    internal fun applyCatalogStorageConfig(
        catalog: String,
        path: Path,
        base: Configuration,
    ): Configuration {
        val configuration = Configuration(base)
        val scheme = path.toUri().scheme

        if (!CatalogStorageProperties.isS3Scheme(scheme)) {
            logger.info(
                "Location scheme '$scheme' is not S3-compatible; using Spark's Hadoop configuration as-is for catalog=$catalog, location=$path"
            )
            return configuration
        }

        val catalogStorageConfig = catalogStorageConfigResolver.resolve(catalog)

        catalogStorageConfig.removedKeys.forEach { configuration.unset(it) }
        catalogStorageConfig.overrides.forEach { (key, value) -> configuration.set(key, value) }

        return configuration
    }

    private fun sparkHadoopConfiguration(): Configuration =
        sparkSessionProvider.getOrCreate().sparkContext().hadoopConfiguration()
}
