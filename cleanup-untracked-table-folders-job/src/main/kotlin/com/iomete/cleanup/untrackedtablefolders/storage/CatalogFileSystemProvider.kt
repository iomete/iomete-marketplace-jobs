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
        operation: String,
        locations: List<String>,
        block: (FileSystem) -> T,
    ): T {
        val target = requireSingleFileSystemTarget(catalog, operation, locations)
        val configuration = buildConfiguration(catalog, target)

        return runOperation(catalog, operation, locations.first()) {
            FileSystem.newInstance(target.toUri(), configuration).use { fileSystem ->
                block(fileSystem)
            }
        }
    }

    fun <T> runOperation(
        catalog: String,
        operation: String,
        location: String,
        block: () -> T,
    ): T =
        try {
            block()
        } catch (th: CatalogStorageConfigurationException) {
            throw th
        } catch (th: CatalogStorageOperationException) {
            throw th
        } catch (th: Throwable) {
            throw CatalogStorageOperationException(
                "Failed to $operation for catalog=$catalog, location=$location",
                th,
            )
        }

    /**
     * One filesystem instance serves one scheme and authority, so a batch that spans more than one
     * must not be pushed through a single instance.
     */
    private fun requireSingleFileSystemTarget(
        catalog: String,
        operation: String,
        locations: List<String>,
    ): Path {
        if (locations.isEmpty()) {
            throw CatalogStorageConfigurationException(
                catalog = catalog,
                message = "Cannot $operation for catalog=$catalog without a location.",
            )
        }

        val paths = locations.map { Path(it) }
        val targets = paths.map { it.toUri().let { uri -> "${uri.scheme}://${uri.authority}" } }.distinct()

        if (targets.size > 1) {
            throw CatalogStorageConfigurationException(
                catalog = catalog,
                message =
                    "Cannot $operation for catalog=$catalog across more than one storage target: ${targets.sorted()}.",
            )
        }

        return paths.first()
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
