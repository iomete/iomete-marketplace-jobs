package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CatalogClient
import com.iomete.catalogsync.CoreClient
import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.ExcludedItemException
import com.iomete.catalogsync.config.enforceCatalogExclusionRules
import com.iomete.catalogsync.config.enforceSchemaExclusionRules
import com.iomete.catalogsync.config.ignoreExcluded
import jakarta.inject.Singleton
import org.apache.spark.sql.SparkSession
import org.eclipse.microprofile.rest.client.inject.RestClient
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

private val logger = LoggerFactory.getLogger(MetadataScraper::class.java)

@Singleton
class MetadataScraper(
    private val sparkSessionProvider: SparkSessionProvider,
    private val applicationConfig: ApplicationConfig,
    private val sparkMetadataReader: SparkMetadataReader,
    private val tableMetadataExtractor: TableMetadataExtractor,
    @param:RestClient val coreServiceClient: CoreClient,
    @param:RestClient val catalogServiceClient: CatalogClient,
) {
    private val exclusionRules = applicationConfig.exclusionRules

    fun run() {
        logger.info("Running process with application config: {}", applicationConfig)

        val catalogs =
            coreServiceClient
                .catalogs()
                .mapNotNull { catalog ->
                    ignoreExcluded {
                        exclusionRules.enforceCatalogExclusionRules(catalog)
                        catalog
                    }
                }

        val catalogExecutor = Executors.newFixedThreadPool(catalogs.size.coerceIn(1, 8))
        try {
            val futures =
                catalogs.map { catalog ->
                    catalogExecutor.submit {
                        processCatalog(catalog)
                    }
                }
            futures.forEach { it.get() }
        } finally {
            catalogExecutor.shutdown()
        }
    }

    private fun processCatalog(catalog: CatalogDetails) {
        val spark = sparkSessionProvider.getSession(catalog)
        val schemas = sparkMetadataReader.getSchemas(spark, catalog.name)

        val schemaExecutor = Executors.newFixedThreadPool(schemas.size.coerceIn(1, 8))
        try {
            val futures =
                schemas.map { schema ->
                    schemaExecutor.submit<SchemaMetadata?> {
                        ignoreExcluded {
                            processSchema(spark = spark, catalog = catalog, schema = schema)
                        }
                    }
                }

            val processedSchemas =
                futures.mapNotNull { it.get() }.onEach {
                    it.log()
                    catalogServiceClient.indexSchema(it)
                }

            CatalogMetadata
                .build(catalog, processedSchemas, sparkApplicationId = spark.sparkContext().applicationId())
                .also { it.log() }
                .also { catalogServiceClient.indexCatalog(it) }
        } finally {
            schemaExecutor.shutdown()
        }
    }

    private fun processSchema(
        spark: SparkSession,
        catalog: CatalogDetails,
        schema: String,
    ): SchemaMetadata {
        logger.info("Processing schema: {}.{}", catalog, schema)

        exclusionRules.enforceSchemaExclusionRules(
            schema = schema,
            props = sparkMetadataReader.getSchemaProperties(spark, catalog.name, schema),
        )

        val failures = AtomicInteger(0)

        val tables =
            sparkMetadataReader
                .getTables(spark, catalog, schema)
                .parallelStream()
                .map { t ->
                    try {
                        tableMetadataExtractor
                            .scrapeTable(spark, catalog.name, schema, t.name, t.isTemp)
                            .also { it.log() }
                            .also {
                                logger.info(
                                    "Indexing table {}.{}.{} with spark session ID={}",
                                    it.catalog,
                                    schema,
                                    t.name,
                                    it.sparkApplicationId,
                                )
                                catalogServiceClient.indexTable(it)
                            }
                    } catch (_: ExcludedItemException) {
                        null // skipped
                    } catch (th: Throwable) {
                        failures.incrementAndGet()
                        logger.error("Failed to process table {}.{}.{}: {}", catalog.name, schema, t.name, th.localizedMessage)
                        null
                    }
                }.toList()
                .mapNotNull { it }

        return SchemaMetadata.build(
            catalog = catalog.name,
            schema = schema,
            tables = tables,
            failuresSize = failures.get(),
            sparkApplicationId = spark.sparkContext().applicationId(),
        )
    }
}
