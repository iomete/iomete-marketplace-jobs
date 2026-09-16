package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.util.concurrent.ConcurrentHashMap
import org.jboss.logging.Logger

/** Resolves catalog-specific storage configuration from the Spark session. */
@ApplicationScoped
class CatalogStorageConfigResolver {
    private val logger = Logger.getLogger(CatalogStorageConfigResolver::class.java)

    @Inject
    lateinit var sparkSessionProvider: SparkSessionProvider

    private val cache = ConcurrentHashMap<String, CatalogStorageConfig>()

    fun resolve(catalog: String): CatalogStorageConfig =
        cache.computeIfAbsent(catalog) { name ->
            val sparkConf = sparkSessionProvider.getOrCreate().sparkContext().getConf()

            if (!sparkConf.contains(CatalogStorageProperties.catalogRegistrationKey(name))) {
                throw CatalogStorageConfigurationException(
                    catalog = name,
                    message =
                        "Catalog '$name' is not registered in this Spark session, so its object-storage " +
                            "configuration cannot be resolved. Expected sparkConf key " +
                            "'${CatalogStorageProperties.catalogRegistrationKey(name)}'. Check that the catalog exists " +
                            "and that this job's domain has permission to use it.",
                )
            }

            val catalogProperties =
                sparkConf
                    .getAllWithPrefix(CatalogStorageProperties.catalogPropertyPrefix(name))
                    .associate { it._1() to it._2() }

            val hadoopConfiguration = sparkSessionProvider.getOrCreate().sparkContext().hadoopConfiguration()

            CatalogHadoopConfigBuilder
                .build(
                    catalog = name,
                    catalogProperties = catalogProperties,
                    hadoopFallback = { key -> hadoopConfiguration.get(key) },
                ).also {
                    logger.info("Resolved catalog storage configuration: ${CatalogHadoopConfigBuilder.describe(it)}")
                }
        }
}
