package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CatalogClient
import com.iomete.catalogsync.CoreClient
import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.ExcludedItemException
import com.iomete.catalogsync.config.enforceCatalogExclusionRules
import com.iomete.catalogsync.config.includes
import com.iomete.catalogsync.config.enforceSchemaExclusionRules
import com.iomete.catalogsync.config.ignoreExcluded
import jakarta.annotation.PreDestroy
import jakarta.inject.Singleton
import org.apache.spark.sql.SparkSession
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.eclipse.microprofile.rest.client.inject.RestClient
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

private val logger = LoggerFactory.getLogger(MetadataScraper::class.java)

private data class SchemaResult(
    val schemaMetadata: SchemaMetadata,
    val tableMetadataList: List<TableMetadata>,
)

@Singleton
class MetadataScraper(
    private val sparkSessionProvider: SparkSessionProvider,
    private val applicationConfig: ApplicationConfig,
    private val sparkMetadataReader: SparkMetadataReader,
    private val tableMetadataExtractor: TableMetadataExtractor,
    @param:RestClient val coreServiceClient: CoreClient,
    @param:RestClient val catalogServiceClient: CatalogClient,
    @param:ConfigProperty(name = "iomete.domain") private val domain: String,
) {
    private val inclusionRules = applicationConfig.inclusionRules
    private val exclusionRules = applicationConfig.exclusionRules
    private val schemaParallelism = System.getenv("SCHEMA_PARALLELISM")?.toIntOrNull()
        ?: Runtime.getRuntime().availableProcessors()
    private val httpPool: ExecutorService = Executors.newFixedThreadPool(
        System.getenv("HTTP_PARALLELISM")?.toIntOrNull() ?: 16
    )

    @PreDestroy
    fun shutdown() {
        logger.info("Shutting down httpPool")
        httpPool.shutdown()
        if (!httpPool.awaitTermination(60, TimeUnit.SECONDS)) {
            logger.warn("httpPool did not terminate in time, forcing shutdown")
            httpPool.shutdownNow()
        }
    }

    fun run() {
        logger.info("Running process with application config: {}", applicationConfig)

        val (catalogs, isPrivate) = catalogsToSync()
        checkIncludedCatalogsExist(catalogs, isPrivate)

        // Inclusion narrows the list first; every exclusion rule then still applies, so exclusion wins.
        val (included, notIncluded) = catalogs.partition { inclusionRules.includes(it) }
        if (notIncluded.isNotEmpty()) {
            logger.info("Skipping {} catalogs that are not in the inclusion list", notIncluded.size)
            logger.debug("Catalogs not in the inclusion list: {}", notIncluded.map { it.name })
        }

        included
            .mapNotNull { catalog -> excludeOrKeep(catalog) }
            .forEach { catalog ->
                try {
                    val spark = sparkSessionProvider.getSession(catalog)

                    val schemas = sparkMetadataReader.getSchemas(spark, catalog.name)
                    logger.info("Processing {} schemas with parallelism={}", schemas.size, schemaParallelism)
                    val pool = ForkJoinPool(schemaParallelism)
                    val schemaResults: List<SchemaResult> = try {
                        pool.submit<List<SchemaResult>> {
                            schemas.parallelStream()
                                .map { schema ->
                                    try {
                                        ignoreExcluded {
                                            processSchema(
                                                spark = spark,
                                                catalog = catalog,
                                                schema = schema,
                                            )
                                        }
                                    } catch (th: Throwable) {
                                        logger.error("Failed to process schema {}.{}: {}", catalog.name, schema, th.message, th)
                                        null
                                    }
                                }
                                .toList()
                                .filterNotNull()
                        }.get()
                    } finally {
                        pool.shutdown()
                    }

                    val allTableMetadata = schemaResults.flatMap { it.tableMetadataList }
                    val allSchemaMetadata = schemaResults.map { it.schemaMetadata }

                    logger.info(
                        "Spark processing complete for catalog {}. Firing {} indexTable and {} indexSchema HTTP calls concurrently.",
                        catalog.name, allTableMetadata.size, allSchemaMetadata.size
                    )

                    val tableFutures = allTableMetadata.map { table ->
                        CompletableFuture.runAsync({
                            logger.debug(
                                "Indexing table {}.{}.{} with spark session ID={}",
                                table.catalog, table.schema, table.name, table.sparkApplicationId
                            )
                            catalogServiceClient.indexTable(table)
                        }, httpPool).exceptionally { th ->
                            logger.error(
                                "Failed to index table {}.{}.{}",
                                table.catalog, table.schema, table.name, th.cause ?: th
                            )
                            null
                        }
                    }

                    val schemaFutures = allSchemaMetadata.map { schema ->
                        CompletableFuture.runAsync({
                            schema.log()
                            catalogServiceClient.indexSchema(schema)
                        }, httpPool).exceptionally { th ->
                            logger.error(
                                "Failed to index schema {}.{}",
                                catalog.name, schema, th.cause ?: th
                            )
                            null
                        }
                    }

                    CompletableFuture.allOf(*(tableFutures + schemaFutures).toTypedArray()).join()

                    try {
                        CatalogMetadata
                            .build(catalog, allSchemaMetadata, sparkApplicationId = spark.sparkContext().applicationId())
                            .also { it.log() }
                            .also { catalogServiceClient.indexCatalog(it) }
                    } catch (th: Throwable) {
                        logger.error("Failed to index catalog {}: {}", catalog.name, th.cause?.message ?: th.message, th)
                    }
                } catch (th: Throwable) {
                    logger.error("Failed to process catalog {}: {}", catalog.name, th.message, th)
                }
            }
    }

    /**
     * A private domain's job syncs only the catalogs the domain owns, reached with its launch settings.
     * Any other domain syncs the install-wide list. A failed lookup stops the run.
     */
    private fun catalogsToSync(): SyncScope {
        val isPrivate =
            checkNotNull(coreServiceClient.domain(domain).isPrivate) { "iom-core did not say whether domain $domain is private" }
        if (!isPrivate) return SyncScope(coreServiceClient.catalogs(), isPrivate = false)

        logger.info("Domain {} is private, syncing only its own catalogs", domain)
        val catalogs = coreServiceClient.ownedCatalogs(domain).catalogs.filter { catalog ->
            sparkSessionProvider.isLaunchCatalog(catalog.name).also { launched ->
                if (!launched) logger.warn("Skipping catalog {}: the job was not launched with its settings", catalog.name)
            }
        }
        return SyncScope(catalogs, isPrivate = true)
    }

    private data class SyncScope(val catalogs: List<CatalogDetails>, val isPrivate: Boolean)

    // An included catalog that an exclusion rule drops would otherwise sync nothing on every run, logged only at INFO.
    private fun excludeOrKeep(catalog: CatalogDetails): CatalogDetails? =
        try {
            exclusionRules.enforceCatalogExclusionRules(catalog)
            catalog
        } catch (e: ExcludedItemException) {
            if (catalog.name in inclusionRules.catalogs.names) {
                logger.warn("Catalog {} is in the inclusion list but is not synced: {}", catalog.name, e.message)
            }
            null
        }

    // A misspelt name in the inclusion list would otherwise sync nothing without saying why.
    // Some unsyncable names are only a warning; when none of them can be synced, the run fails.
    // In a private domain a name can exist and still be unsyncable (not owned, or not launched with), so the
    // messages say "cannot sync" rather than "does not exist".
    private fun checkIncludedCatalogsExist(catalogs: List<CatalogDetails>, isPrivate: Boolean) {
        val included = inclusionRules.catalogs.names
        if (included.isEmpty()) return

        val syncable = catalogs.map { it.name }.toSet()
        val unsyncable = included.filterNot { it in syncable }
        val reason = if (isPrivate) " (a private domain syncs only the catalogs it owns that the job was launched with)" else ""
        if (unsyncable.size == included.size) {
            throw IllegalStateException(
                "None of the included catalogs can be synced by this job: $unsyncable. Catalogs this job can sync: $syncable$reason",
            )
        }
        if (unsyncable.isNotEmpty()) {
            logger.warn(
                "Included catalogs this job cannot sync, skipping them: {}. Catalogs this job can sync: {}{}",
                unsyncable, syncable, reason,
            )
        }
    }

    private fun processSchema(
        spark: SparkSession,
        catalog: CatalogDetails,
        schema: String,
    ): SchemaResult {
        logger.info("Processing schema: {}.{}", catalog.name, schema)

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
                            .scrapeTable(
                                spark = spark,
                                catalog = catalog.name,
                                schema = schema,
                                tableName = t.name,
                                isTemp = t.isTemp,
                                useIcebergFastPath = catalog.isIcebergCatalog() && !t.isView,
                            ).also { it.log() }
                    } catch (_: ExcludedItemException) {
                        null // skipped
                    } catch (th: Throwable) {
                        failures.incrementAndGet()
                        logger.error("Failed to process table {}.{}.{}: {}", catalog.name, schema, t.name, th.localizedMessage, th)
                        null
                    }
                }.toList()
                .filterNotNull()

        val schemaMetadata = SchemaMetadata.build(
            catalog = catalog.name,
            schema = schema,
            tables = tables,
            failuresSize = failures.get(),
            sparkApplicationId = spark.sparkContext().applicationId(),
        )

        return SchemaResult(
            schemaMetadata = schemaMetadata,
            tableMetadataList = tables,
        )
    }

    private fun CatalogDetails.isIcebergCatalog(): Boolean =
        type.any { it.equals("iceberg", ignoreCase = true) }
}
