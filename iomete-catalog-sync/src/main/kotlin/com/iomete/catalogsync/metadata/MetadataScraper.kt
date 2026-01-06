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
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.log

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

        coreServiceClient
            .catalogs()
            .mapNotNull { catalog ->
                ignoreExcluded {
                    exclusionRules.enforceCatalogExclusionRules(catalog)
                    catalog // keep it if not excluded
                }
            }.onEach { catalog ->
                val spark = sparkSessionProvider.getSession(catalog)

                val processedSchemas: List<SchemaMetadata> =
                    sparkMetadataReader
                        .getSchemas(spark, catalog.name)
                        .asSequence()
                        .mapNotNull {
                            ignoreExcluded {
                                processSchema(
                                    spark = spark,
                                    catalog = catalog,
                                    schema = it,
                                )
                            }
                        }.onEach {
                            it.log()
                            catalogServiceClient.indexSchema(it)
                        }.toList()

                CatalogMetadata
                    .build(catalog, processedSchemas, sparkApplicationId = spark.sparkContext().applicationId())
                    .also { it.log() }
                    .also { catalogServiceClient.indexCatalog(it) }
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
