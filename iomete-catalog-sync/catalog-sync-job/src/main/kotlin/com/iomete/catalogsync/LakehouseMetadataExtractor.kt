package com.iomete.catalogsync

import com.iomete.catalogsync.extract.*
import io.micrometer.core.instrument.Meter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.apache.spark.sql.Row
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.function.Supplier
import jakarta.inject.Singleton
import org.eclipse.microprofile.rest.client.inject.RestClient
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ForkJoinPool
import kotlin.collections.set
import org.eclipse.microprofile.config.ConfigProvider

const val METRIC_NAME_TABLE_PROCESS = "1.table_process"
const val METRIC_NAME_EXTRACT_TABLE_STATISTICS = "2.extract_table_statistics"
const val METRIC_NAME_EXTRACT_COLUMNS = "3.extract_columns"
const val METRIC_NAME_EXTRACT_COLUMNS_STATISTICS = "4.extract_columns_statistics"
const val METRIC_NAME_EXTRACT_TAGS = "5.extract_tags"
const val METRIC_NAME_DATA_SYNC = "6.data_sync"
const val METRIC_NAME_TABLE_PROCESS_FAILURES = "table_process_failures"
const val METRIC_NAME_DATA_SYNC_FAILURES = "data_sync_failures"

val METRIC_NAMES = setOf(
    METRIC_NAME_TABLE_PROCESS,
    METRIC_NAME_EXTRACT_TABLE_STATISTICS,
    METRIC_NAME_EXTRACT_COLUMNS_STATISTICS,
    METRIC_NAME_EXTRACT_COLUMNS,
    METRIC_NAME_EXTRACT_TAGS,
    METRIC_NAME_DATA_SYNC
)

@Singleton
class LakehouseMetadataExtractor(
    private val tableExtractorFactory: TableExtractorFactory,
    private val dataSync: DataSync,
    sparkSessionProvider: SparkSessionProvider,
    applicationConfig: ApplicationConfig,
    private val registry: MeterRegistry,
    @RestClient val coreServiceClient: CoreServiceClient
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val spark = sparkSessionProvider.sparkSession
    private val excludeSchemas: Set<String> = applicationConfig.excludeSchemas().orElse(setOf())
    
    private val catalogViewSupport = ConcurrentHashMap<String, Boolean>()
    private val viewSupportedCatalogTypes = setOf("iceberg", "glue", "rest")

    private data class SchemaBatch(
        val catalog: CoreServiceClient.CatalogDetails,
        val schema: String,
        val tables: List<Row>,
        val discoveryFailed: Boolean = false
    )

    private data class TableWorkItem(
        val catalog: CoreServiceClient.CatalogDetails,
        val schema: String,
        val tableRow: Row
    )

    private data class SyncResult(
        val catalogName: String,
        val schema: String,
        val tableName: String,
        val failed: Boolean
    )

    private data class TableResult(
        val catalogName: String,
        val schema: String,
        val tableName: String,
        val metadata: TableMetadata?,
        val error: String?
    )

    fun scrape(appConfig: AppConfig) {
        val catalogs = getCatalog(appConfig)
        logger.info("Catalogs: {}", catalogs)

        val config = ConfigProvider.getConfig()
        val parallelism = config
            .getOptionalValue("HTTP_PARALLELISM", Int::class.java)
            .orElse(Runtime.getRuntime().availableProcessors().coerceAtLeast(4))
        val tableProcessTimeout = config
            .getOptionalValue("TABLE_PROCESS_TIMEOUT_SECONDS", Long::class.java)
            .orElse(60L)
        val syncTimeout = config
            .getOptionalValue("SYNC_TIMEOUT_SECONDS", Long::class.java)
            .orElse(60L)

        val pool = ForkJoinPool(parallelism)
        try {
            val timeoutExecutor = java.util.concurrent.Executors.newFixedThreadPool(parallelism)
            try {
                logger.info("Using ForkJoinPool with parallelism={}, tableProcessTimeout={}s", parallelism, tableProcessTimeout)
            // Phase 1: Discover all schemas and tables (parallel, flat — no nesting)
            val catalogDiscoveryFailed = mutableSetOf<String>()
            val allSchemaEntries = catalogs.flatMap { catalog ->
                logger.info("Fetching schemas for catalog: {}", catalog.name)
                try {
                    val schemas = getSchemas(catalog.name)
                    logger.info("Catalog {} has {} schemas", catalog.name, schemas.size)
                    schemas.map { schema -> catalog to schema }
                } catch (th: Throwable) {
                    logger.error("Failed to discover schemas in catalog {}: {}", catalog.name, th.message, th)
                    catalogDiscoveryFailed.add(catalog.name)
                    emptyList()
                }
            }

            val schemaBatches = Collections.synchronizedList(mutableListOf<SchemaBatch>())
            pool.submit {
                allSchemaEntries.parallelStream().forEach { (catalog, schema) ->
                    try {
                        logger.info("Discovering tables in {}.{}", catalog.name, schema)
                        val tables = getTables(catalog.name, schema, catalog.type)
                        schemaBatches.add(SchemaBatch(catalog, schema, tables))
                    } catch (th: Throwable) {
                        logger.error("Failed to discover tables in {}.{}: {}", catalog.name, schema, th.message, th)
                        schemaBatches.add(SchemaBatch(catalog, schema, emptyList(), discoveryFailed = true))
                    }
                }
            }.get()

            logger.info("Discovery complete: {} schemas, {} tables total",
                schemaBatches.size,
                schemaBatches.sumOf { it.tables.size }
            )

            // Phase 2: Process all tables (parallel, flat — no nesting)
            val allWorkItems = schemaBatches.flatMap { batch ->
                batch.tables.map { tableRow -> TableWorkItem(batch.catalog, batch.schema, tableRow) }
            }

            val results = Collections.synchronizedList(mutableListOf<TableResult>())
            pool.submit {
                allWorkItems.parallelStream().forEach { work ->
                    val catalogName = work.catalog.name
                    var tableName = "unknown"
                    try {
                        tableName = work.tableRow.getString(1)
                        val isTemp = work.tableRow.getBoolean(2)

                        logger.info("Processing table: {}.{}.{}", catalogName, work.schema, tableName)

                        val tableProcessMetric = getTimer(
                            name = METRIC_NAME_TABLE_PROCESS, catalog = catalogName, schema = work.schema, tableName = tableName
                        )
                        val scrapedData = tableProcessMetric.recordCallable {
                            val future = CompletableFuture.supplyAsync({
                                scrapeTable(catalog = catalogName, schema = work.schema, tableName = tableName, isTemp = isTemp)
                            }, timeoutExecutor)
                            try {
                                future.get(tableProcessTimeout, TimeUnit.SECONDS)
                            } catch (te: TimeoutException) {
                                future.cancel(true)
                                throw te
                            }
                        }

                        logger.info(
                            "Processing finished in {} ms for schema: {}.{}, table: {}",
                            tableProcessMetric.totalTime(TimeUnit.MILLISECONDS),
                            catalogName,
                            work.schema,
                            tableName,
                        )

                        results.add(TableResult(catalogName, work.schema, tableName, scrapedData, null))
                    } catch (th: Throwable) {
                        logger.error("Failed to process table {}.{}.{}: {}", catalogName, work.schema, tableName, th.message, th)
                        registry.counter(METRIC_NAME_TABLE_PROCESS_FAILURES, "catalog", catalogName, "schema", work.schema, "table", tableName).increment()
                        results.add(TableResult(catalogName, work.schema, tableName, null, th.message ?: "Unknown error"))
                    }
                }
            }.get()

            // Phase 3: Sync table data (parallel, flat — no nesting)
            val finalResults = ArrayList(results)
            val successfulResults = finalResults.filter { it.metadata != null }
            val syncResults = Collections.synchronizedList(mutableListOf<SyncResult>())
            pool.submit {
                successfulResults.parallelStream().forEach { result ->
                    try {
                        val dataSyncMetric = getTimer(
                            name = METRIC_NAME_DATA_SYNC, catalog = result.catalogName, schema = result.schema, tableName = result.tableName
                        )
                        dataSyncMetric.record<Unit> {
                            val future = CompletableFuture.supplyAsync({
                                dataSync.syncTableData(result.metadata!!)
                            }, timeoutExecutor)
                            try {
                                future.get(syncTimeout, TimeUnit.SECONDS)
                            } catch (te: TimeoutException) {
                                future.cancel(true)
                                throw te
                            }
                        }
                        syncResults.add(SyncResult(result.catalogName, result.schema, result.tableName, failed = false))
                    } catch (th: Throwable) {
                        logger.error("Failed to sync table {}.{}.{}: {}", result.catalogName, result.schema, result.tableName, th.message, th)
                        registry.counter(METRIC_NAME_DATA_SYNC_FAILURES, "catalog", result.catalogName, "schema", result.schema, "table", result.tableName).increment()
                        syncResults.add(SyncResult(result.catalogName, result.schema, result.tableName, failed = true))
                    }
                }
            }.get()

            // Phase 4: Aggregate and sync schema/catalog metadata (sequential, fast)
            val finalSyncResults = ArrayList(syncResults)
            val resultsBySchema = finalResults.groupBy { "${it.catalogName}/${it.schema}" }
            val syncResultsBySchema = finalSyncResults.groupBy { "${it.catalogName}/${it.schema}" }
            val schemasByCatalog = schemaBatches.groupBy { it.catalog.name }

            catalogs.forEach { catalog ->
                try {
                    val catalogSchemas = schemasByCatalog[catalog.name] ?: emptyList()
                    var totalTableCount = 0
                    var totalSizeInBytes = 0L
                    var totalFiles = 0L

                    catalogSchemas.forEach { batch ->
                        val key = "${catalog.name}/${batch.schema}"
                        val schemaResults = resultsBySchema[key] ?: emptyList()
                        val schemaSyncResults = syncResultsBySchema[key] ?: emptyList()
                        val successful = schemaResults.mapNotNull { it.metadata }
                        val failedCount = schemaResults.count { it.error != null }
                        val syncFailedCount = schemaSyncResults.count { it.failed }

                        if (failedCount > 0) {
                            logger.warn("Failed to process {} tables in schema {}.{}", failedCount, catalog.name, batch.schema)
                            schemaResults.filter { it.error != null }.forEach {
                                logger.warn("Table {}.{}.{} failed: {}", catalog.name, batch.schema, it.tableName, it.error)
                            }
                        }

                        val schemaTableCount = batch.tables.size
                        val viewCount = successful.count { it.isView }
                        val schemaSizeInBytes = successful.sumOf { it.sizeInBytes ?: 0L }
                        val schemaDbSizeInBytes = successful.sumOf { it.totalTableSizeInBytes ?: 0L }
                        val schemaFiles = successful.sumOf { it.numFiles ?: 0L }

                        val schemaMetadata = SchemaMetadata(
                            catalog = catalog.name,
                            schema = batch.schema,
                            totalTableCount = schemaTableCount,
                            totalViewCount = viewCount,
                            totalSizeInBytes = schemaSizeInBytes,
                            totalDbSizeInBytes = schemaDbSizeInBytes,
                            totalFiles = schemaFiles,
                            failedTableCount = failedCount,
                            syncFailedCount = syncFailedCount,
                            discoveryFailed = batch.discoveryFailed
                        )
                        dataSync.syncSchemaData(schemaMetadata)

                        totalTableCount += schemaTableCount
                        totalSizeInBytes += schemaSizeInBytes
                        totalFiles += schemaFiles

                        logger.info(
                            "Processing schema: {} finished! Total Tables: {}, Views: {}, Total Size: {} bytes, Total Files: {}, Failed Tables: {}",
                            batch.schema, schemaTableCount, viewCount, schemaSizeInBytes, schemaFiles, failedCount
                        )
                    }

                    val catalogMetadata = CatalogMetadata(
                        catalog = catalog.name,
                        type = catalog.type.toSet(),
                        location = catalog.location,
                        storageEndpoint = catalog.storageEndpoint,
                        totalSchemaCount = catalogSchemas.size,
                        totalTableCount = totalTableCount,
                        totalSizeInBytes = totalSizeInBytes,
                        totalFiles = totalFiles,
                        domainsAllowed = catalog.domainsAllowed.toSet(),
                        discoveryFailed = catalog.name in catalogDiscoveryFailed
                    )
                    dataSync.syncCatalogData(catalogMetadata)

                    logger.info(
                        "Processing catalog: {} finished! Total Schemas: {}, Total Tables: {}, Total Size: {} bytes, Total Files: {}",
                        catalog, catalogSchemas.size, totalTableCount, totalSizeInBytes, totalFiles
                    )
                } catch (th: Throwable) {
                    logger.error("Failed to sync metadata for catalog {}: {}", catalog.name, th.message, th)
                }
            }

            // Summary log
            val totalDiscovered = allWorkItems.size
            val totalProcessed = finalResults.count { it.metadata != null }
            val totalProcessFailed = finalResults.count { it.error != null }
            val totalSyncFailed = finalSyncResults.count { it.failed }
            val totalSynced = totalProcessed - totalSyncFailed
            val totalDiscoveryFailed = schemaBatches.count { it.discoveryFailed }

            logger.info(
                "Catalog sync summary: schemasDiscovered={} tablesDiscovered={} tablesProcessed={} tablesSynced={} processFailures={} syncFailures={} discoveryFailures={} catalogDiscoveryFailures={}",
                schemaBatches.size, totalDiscovered, totalProcessed, totalSynced,
                totalProcessFailed, totalSyncFailed, totalDiscoveryFailed, catalogDiscoveryFailed.size
            )
            } finally {
                timeoutExecutor.shutdown()
                timeoutExecutor.awaitTermination(30, TimeUnit.SECONDS)
            }
        } finally {
            pool.shutdown()
            pool.awaitTermination(30, TimeUnit.SECONDS)
        }

        printMetrics()
    }

    private fun scrapeTable(catalog: String, schema: String, tableName: String, isTemp: Boolean): TableMetadata? {
        val table = describeTable(catalog, schema, tableName)

        var tableType = table.metadata.getOrDefault("Type", "UNKNOWN")
        val isView = tableType.equals("view", ignoreCase = true)

        val tableProvider = table.metadata.getOrDefault("Provider", "UNKNOWN")
        if (tableType == "UNKNOWN" && tableProvider == "iceberg") {
            tableType = "MANAGED"
        }

        val currentSnapshotId = extractCurrentSnapshotId(table.metadata)

        val tableExtractor = tableExtractorFactory.extractorFor(
            provider = tableProvider,
            isView = isView,
            catalog = catalog,
            schema = schema,
            table = tableName,
            currentSnapshotId = currentSnapshotId
        )

        val extractTableStatisticsMetric = getTimer(
            name = METRIC_NAME_EXTRACT_TABLE_STATISTICS, catalog = catalog, schema = schema, tableName = tableName
        )

        val extractColumnsStatisticsMetric = getTimer(
            name = METRIC_NAME_EXTRACT_COLUMNS_STATISTICS, catalog = catalog, schema = schema, tableName = tableName
        )

        val extractTagsMetric = getTimer(
            name = METRIC_NAME_EXTRACT_TAGS, catalog = catalog, schema = schema, tableName = tableName
        )

        fun <T : Any?> recordNullable(timer: Timer, supplier: Supplier<T>): T? {
            return timer.record(supplier)
        }

        fun <T : Any> record(timer: Timer, supplier: Supplier<T>): T {
            @Suppress("NULLABILITY_MISMATCH_BASED_ON_JAVA_ANNOTATIONS")
            return timer.record(supplier)
        }

        val datasetStatistics: TableStatistics? = recordNullable(extractTableStatisticsMetric) {
            when (tableExtractor) {
                is SupportTableStatistics -> tableExtractor.extractTableStatistics()
                else -> null
            }
        }

        val columnMetadataList: List<ColumnMetadata> = table.columns

        if (tableExtractor is SupportColumnTags) {
            val columnTags = record(extractTagsMetric) {
                tableExtractor.extractColumnTags(columns = columnMetadataList.map { it.name })
            }

            columnMetadataList.forEach { columnMetadata ->
                columnMetadata.tags = columnTags[columnMetadata.name] ?: listOf()
            }
        }

        if (tableExtractor is SupportColumnStatistics) {
            val columnStatistics = record(extractColumnsStatisticsMetric) {
                tableExtractor.extractColumnStatistics(columns = columnMetadataList.map { it.name })
            }
            columnMetadataList.forEach { columnMetadata ->
                columnMetadata.stats = columnStatistics[columnMetadata.name] ?: listOf()
            }
        }

        val tableTags = columnMetadataList.flatMap { it.tags }
            .filter { it.contains("PII") || it.contains("PCI") }.distinct().toList()


        var creationTime: Long? = null
        try {
            val tableCreationTime = table.metadata.getOrDefault("Created Time", null)
            if (tableCreationTime != null) {
                creationTime = LocalDateTime.parse(
                    tableCreationTime,
                    DateTimeFormatter.ofPattern("E MMM dd HH:mm:ss zzz yyyy")
                ).toEpochSecond(ZoneOffset.UTC)
            }
        } catch (ex: DateTimeParseException) {
            logger.warn(
                "error parsing table creation time for {}.{}.{} time={}",
                catalog,
                schema,
                tableName,
                table.metadata.getOrDefault("Created Time", "")
            )
        }

        return TableMetadata(
            catalog = catalog,
            schema = schema,
            name = tableName,
            description = table.metadata.getOrDefault("Comment", ""),
            tableType = tableType,
            isView = isView,
            isTemporary = isTemp,

            owner = table.metadata.getOrDefault("Owner", ""),
            provider = tableProvider,
            viewText = table.metadata["View Text"],
            createdAt = creationTime,

            lastModified = datasetStatistics?.lastModified,
            numFiles = datasetStatistics?.numFiles,
            totalTableNumFiles = datasetStatistics?.totalTableNumFiles,
            sizeInBytes = datasetStatistics?.sizeInBytes,
            totalTableSizeInBytes = datasetStatistics?.totalTableSizeInBytes,
            totalRecords = datasetStatistics?.totalRecords,

            columns = columnMetadataList,

            tags = tableTags,

            syncTime = Instant.now().toEpochMilli()
        )
    }

    private fun getCatalog(appConfig: AppConfig): List<CoreServiceClient.CatalogDetails> {
        logger.info("Fetching catalogs...")

        val allCatalogs = coreServiceClient.catalogs().toList()
        val include = appConfig.catalog.include
        val exclude = appConfig.catalog.exclude.toSet()

        return if (include.isNotEmpty()) {
            allCatalogs.filter { it.name in include }
        } else {
            allCatalogs.filterNot { it.name in exclude }
        }
    }

    private fun getSchemas(catalog: String): List<String> {
        logger.info("Fetching schemas in catalog: {}... excludeSchemas: {}", catalog, excludeSchemas)

        return spark.sql("show databases in `$catalog`")
            .collectAsList().map { it.getString(0) }
            .filter { schemaName -> schemaName.isNotBlank() }
            .filter { schemaName -> !excludeSchemas.contains(schemaName) }
    }

    fun getTables(catalog: String, schema: String, catalogType: List<String>): List<Row> {
        val tables = fetchTables(catalog, schema)
        val views = fetchViews(catalog, schema, catalogType)

        return (tables + views).distinctBy { it.getString(1) }
    }

    private fun fetchTables(catalog: String, schema: String): List<Row> {
        return try {
            spark.sql("show tables from `$catalog`.`$schema`").collectAsList()
        } catch (th: Throwable) {
            logger.warn("Failed to fetch tables for catalog {} & schema {}", catalog, schema, th)
            emptyList()
        }
    }

    private fun fetchViews(catalog: String, schema: String, catalogType: List<String>): List<Row> {
        return try {
            if (!shouldFetchViews(catalog, catalogType)) {
                return emptyList()
            }
            spark.sql("show views from `$catalog`.`$schema`").collectAsList()
        } catch (th: Throwable) {
            logger.warn("Failed to fetch views for catalog {} & schema {}", catalog, schema, th)
            emptyList()
        }
    }

    private fun shouldFetchViews(catalog: String, catalogType: List<String>): Boolean {
        return catalogViewSupport.computeIfAbsent(catalog) {
            checkViewSupport(catalog, catalogType)
        }
    }

    private fun checkViewSupport(catalog: String, catalogType: List<String>): Boolean {
        val hasViewSupport = catalogType.any { type ->
            viewSupportedCatalogTypes.contains(type.lowercase())
        }
        if (!hasViewSupport) {
            logger.info("Skipping views for catalog '{}' with unsupported types: {}", catalog, catalogType)
        }

        return hasViewSupport
    }

    /**
     * Extracts the current-snapshot-id from DESCRIBE EXTENDED metadata.
     * Iceberg stores this inside the "Table Properties" value as a bracketed key-value list,
     * e.g. "[current-snapshot-id=1234567890, format-version=2, ...]".
     * Returns "none" if the table has no snapshot, null if the key is not found.
     */
    private fun extractCurrentSnapshotId(metadata: Map<String, String>): String? {
        // Try direct key first (in case Spark version exposes it directly)
        metadata["Current-Snapshot-Id"]?.let { return it }

        // Parse from Table Properties: "[key1=val1, key2=val2, ...]"
        val tableProperties = metadata["Table Properties"] ?: return null
        val match = SNAPSHOT_ID_REGEX.find(tableProperties)
        return match?.groupValues?.get(1) ?: "none"
    }

    private fun describeTable(catalog: String, schema: String, tableName: String): TableDescription {
        logger.info("describeTable for {}", tableName)

        var rawColumns: List<Row> = listOf()
        try {
            rawColumns = spark.sql("describe extended `$catalog`.`$schema`.`$tableName`").collectAsList()
        } catch (th: Throwable) {
            logger.warn("Couldn't describeTable for {}.{}.{}", catalog, schema, tableName, th)
        }

        return processTableColumns(rawColumns);
    }

    private fun printMetrics() {
        val logMetrics = registry.meters
            .asSequence()
            .filter { it.id.type == Meter.Type.TIMER && METRIC_NAMES.contains(it.id.name) }
            .map { meter ->
                LogMetric(
                    name = meter.id.name,
                    tag = meter.id.tags.joinToString(".") { tag -> tag.value },
                    totalTime = meter.measure().firstOrNull { it.statistic.name == "TOTAL_TIME" }?.value
                )
            }
            .groupBy { it.tag }
            .toList()
            .sortedBy { (_, value) -> value.maxOf { it.totalTime ?: 0.0 } }
            .toList().toMap()
            .toMap()


        val report = StringBuilder()
        logMetrics.forEach { logMetricGroup ->
            logMetricGroup.value.sortedWith(compareBy { it.name }).forEach {
                val formattedValue = "%.2f".format(it.totalTime)
                report.append("Timer: $formattedValue sec, ${it.tag}, ${it.name}\n")
            }
            report.append("\n")
        }

        logger.info("Report: {}", report)
    }

    private fun getTimer(name: String, catalog: String, schema: String, tableName: String): Timer {
        return registry.timer(name, "catalog", catalog, "schema", schema, "table", tableName)
    }

    fun processTableColumns(rawColumns: List<Row>): TableDescription {
        var sortOrder = 0
        var currentSection: TableColumnSection = TableColumnSection.COLUMNS
        val sectionHeaders = mapOf(
            "# Partition Information" to TableColumnSection.PARTITIONS,
            "# Partitioning" to TableColumnSection.PARTITIONS,
            "# Metadata Columns" to TableColumnSection.METADATA,
            "# Detailed Table Information" to TableColumnSection.TABLE_INFO,
            "# Detailed View Information" to TableColumnSection.VIEW_INFO
        )

        val columnsMap = mutableMapOf<String, ColumnMetadata>()
        val metadataMap = mutableMapOf<String, String>()
        for (row in rawColumns) {
            val columnName = row.getString(0).orEmpty()
            val dataType = row.getString(1).orEmpty()
            val comment = row.getString(2)

            if (columnName.startsWith("#") || columnName.isBlank()) {
                val matchedSection = sectionHeaders.entries.find { entry ->
                    columnName.contains(entry.key, ignoreCase = true)
                }?.value

                if (matchedSection != null) {
                    currentSection = matchedSection
                    
                    if (currentSection == TableColumnSection.VIEW_INFO) {
                        metadataMap["Type"] = "view"
                    }
                }

                continue
            }

            when (currentSection) {
                TableColumnSection.COLUMNS -> {
                    val columnMetadata = ColumnMetadata(
                        name = columnName,
                        description = comment,
                        dataType = dataType,
                        sortOrder = sortOrder,
                        isPartitionKey = false,
                    )
                    columnsMap[columnName] = columnMetadata
                    sortOrder += 1
                }
                TableColumnSection.PARTITIONS -> {
                    // sometimes partition name is in columnName, but sometimes it in dataType (iceberg, delta)
                    val partitionColName =
                        if (columnName.contains("Part "))
                            dataType
                        else
                            columnName
                    columnsMap[partitionColName]?.isPartitionKey = true
                }
                TableColumnSection.TABLE_INFO, TableColumnSection.VIEW_INFO -> {
                    metadataMap[columnName] = dataType
                }
                TableColumnSection.METADATA -> {
                    // Not processing as of now
                }
            }
        }

        return TableDescription(
            columns = columnsMap.values.toList(),
            metadata = metadataMap
        )
    }

    data class TableDescription(val columns: List<ColumnMetadata>, val metadata: Map<String, String>)

    data class LogMetric(val name: String, val tag: String, val totalTime: Double?)

    companion object {
        private val SNAPSHOT_ID_REGEX = Regex("""current-snapshot-id\s*=\s*([^,\]\s]+)""")
    }
}
