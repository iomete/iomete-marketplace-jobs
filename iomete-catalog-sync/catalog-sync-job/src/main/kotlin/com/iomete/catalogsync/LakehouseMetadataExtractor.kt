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
import java.util.concurrent.TimeUnit
import java.util.function.Supplier
import jakarta.inject.Singleton
import org.eclipse.microprofile.rest.client.inject.RestClient
import java.util.concurrent.ConcurrentHashMap
import kotlin.collections.set

const val METRIC_NAME_TABLE_PROCESS = "1.table_process"
const val METRIC_NAME_EXTRACT_TABLE_STATISTICS = "2.extract_table_statistics"
const val METRIC_NAME_EXTRACT_COLUMNS = "3.extract_columns"
const val METRIC_NAME_EXTRACT_COLUMNS_STATISTICS = "4.extract_columns_statistics"
const val METRIC_NAME_EXTRACT_TAGS = "5.extract_tags"
const val METRIC_NAME_DATA_SYNC = "6.data_sync"

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

    fun scrape(appConfig: AppConfig) {
        val catalogs = getCatalog(appConfig)
        logger.info("Catalogs: {}", catalogs)

        catalogs.forEach { catalog ->
            logger.info("Processing catalog: {}", catalog)

            val schemas = getSchemas(catalog.name)
            logger.info("Schemas: {}", schemas)

            val totalSchemaCount = schemas.size
            var totalTableCount = 0
            var totalSizeInBytes = 0L
            var totalFiles = 0L

            schemas.forEach { schema ->
                val schemaMetrics = processSchema(catalog = catalog.name, schema = schema)
                totalTableCount += schemaMetrics.totalTableCount
                totalSizeInBytes += schemaMetrics.totalSizeInBytes
                totalFiles += schemaMetrics.totalFiles
            }

            val catalogMetadata = CatalogMetadata(
                catalog = catalog.name,
                type = catalog.type.toSet(),
                location = catalog.location,
                storageEndpoint = catalog.storageEndpoint,
                totalSchemaCount = totalSchemaCount,
                totalTableCount = totalTableCount,
                totalSizeInBytes = totalSizeInBytes,
                totalFiles = totalFiles,
                domainsAllowed = catalog.domainsAllowed.toSet()
            )
            dataSync.syncCatalogData(catalogMetadata)

            logger.info(
                "Processing catalog: {} finished! Total Schemas: {}, Total Tables: {}, Total Size: {} bytes, Total Files: {}",
                catalog, totalSchemaCount, totalTableCount, totalSizeInBytes, totalFiles
            )
        }

        printMetrics()
    }

    private fun processSchema(catalog: String, schema: String): SchemaMetadata {
        logger.info("Processing schema: {}.{}", catalog, schema)
        val tables = getTables(catalog, schema)
        val totalTableCount = tables.size
        var totalViewCount = 0
        var totalSizeInBytes = 0L
        var totalFiles = 0L
        var failedTableCount = 0

        // Create a thread-safe collection to track failed tables
        val failedTables = ConcurrentHashMap<String, String>()

        tables.parallelStream().forEach { tableRow ->
            val tableName = tableRow.getString(1)
            val isTemp = tableRow.getBoolean(2)

            logger.info("Processing table: {}.{}.{}", catalog, schema, tableName)
            var scrapedData: TableMetadata? = null

            try {
                val tableProcessMetric = getTimer(name = METRIC_NAME_TABLE_PROCESS, catalog = catalog, schema = schema, tableName = tableName)
                scrapedData =
                    tableProcessMetric.recordCallable { scrapeTable(catalog = catalog, schema = schema, tableName = tableName, isTemp = isTemp) }

                logger.info(
                    "Processing finished in {} ms for schema: {}.{}, table: {}",
                    tableProcessMetric.totalTime(TimeUnit.MILLISECONDS),
                    catalog,
                    schema,
                    tableName,
                )

                val dataSyncMetric = getTimer(name = METRIC_NAME_DATA_SYNC, catalog = catalog, schema = schema, tableName = tableName)
                scrapedData?.let {
                    dataSyncMetric.record<Unit> { dataSync.syncTableData(it) }
                    synchronized(this) {
                        if (it.isView) totalViewCount++
                        totalSizeInBytes += it.sizeInBytes ?: 0L
                        totalFiles += it.numFiles ?: 0L
                    }
                }
            } catch (th: Throwable) {
                // Record the failure but continue processing other tables
                failedTables[tableName] = th.message ?: "Unknown error"
                logger.error("Failed to process table {}.{}.{}: {}", catalog, schema, tableName, th.message, th)
                synchronized(this) {
                    failedTableCount++
                }
            }
        }

        // Log information about failed tables
        if (failedTableCount > 0) {
            logger.warn("Failed to process {} tables in schema {}.{}", failedTableCount, catalog, schema)
            failedTables.forEach { (tableName, errorMessage) ->
                logger.warn("Table {}.{}.{} failed: {}", catalog, schema, tableName, errorMessage)
            }
        }

        val schemaMetadata = SchemaMetadata(
            catalog = catalog,
            schema = schema,
            totalTableCount = totalTableCount,
            totalViewCount = totalViewCount,
            totalSizeInBytes = totalSizeInBytes,
            totalFiles = totalFiles,
            failedTableCount = failedTableCount
        )
        dataSync.syncSchemaData(schemaMetadata)

        logger.info(
            "Processing schema: {} finished! Total Tables: {}, Views: {}, Total Size: {} bytes, Total Files: {}, Failed Tables: {}",
            schema, totalTableCount, totalViewCount, totalSizeInBytes, totalFiles, failedTableCount
        )
        return schemaMetadata
    }

    private fun scrapeTable(catalog: String, schema: String, tableName: String, isTemp: Boolean): TableMetadata? {
        val table = describeTable(catalog, schema, tableName)

        var tableType = table.metadata.getOrDefault("Type", "UNKNOWN")
        val isView = tableType.equals("view", ignoreCase = true)

        val tableProvider = table.metadata.getOrDefault("Provider", "UNKNOWN")
        if (tableType == "UNKNOWN" && tableProvider == "iceberg") {
            tableType = "MANAGED"
        }

        val tableExtractor = tableExtractorFactory.extractorFor(
            provider = tableProvider,
            isView = isView,
            catalog = catalog,
            schema = schema,
            table = tableName
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
            sizeInBytes = datasetStatistics?.sizeInBytes,
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

        try {
            return spark.sql("show databases in `$catalog`")
                .collectAsList().map { it.getString(0) }
                .filter { schemaName -> schemaName.isNotBlank() } // filter out empty schema names
                .filter { schemaName -> !excludeSchemas.contains(schemaName) }

        } catch (th: Throwable) {
            logger.warn("Couldn't fetch schemas in catalog: {}", catalog, th)
            return emptyList()
        }
    }

    private fun getTables(catalog: String, schema: String): List<Row> {
        try {
            return spark.sql("show tables from `$catalog`.`$schema`").collectAsList()
        } catch (th: Throwable) {
            logger.warn("Couldn't fetch tables for catalog {} & schema {}", catalog, schema, th)
            return emptyList()
        }
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
            "# Detailed View Information" to TableColumnSection.TABLE_INFO
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
                TableColumnSection.TABLE_INFO -> {
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
}
