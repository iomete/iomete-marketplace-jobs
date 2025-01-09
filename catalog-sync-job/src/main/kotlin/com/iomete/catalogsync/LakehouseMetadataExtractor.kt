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
import java.util.Optional
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
    @RestClient val sqlClient: SqlClient
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    private val spark = sparkSessionProvider.sparkSession
    private val excludeSchemas: Set<String> = applicationConfig.excludeSchemas().orElse(setOf())

    fun scrape() {
        val catalogs = getCatalog()
        logger.info("Catalogs: {}", catalogs)

        catalogs.forEach { catalog ->
            logger.info("Processing catalog: {}", catalog)

            val schemas = getSchemas(catalog)
            logger.info("Schemas: {}", schemas)

            schemas.forEach { schema ->
                processSchema(catalog = catalog, schema = schema)
            }
        }

        printMetrics()
    }

    private fun processSchema(catalog: String, schema: String) {
        logger.info("Processing schema: {}.{}", catalog, schema)

        getTables(catalog, schema).parallelStream().forEach { tableRow ->
            val tableName = tableRow.getString(1)
            val isTemp = tableRow.getBoolean(2)

            logger.info("Processing table: {}.{}.{}", catalog, schema, tableName)
            var scrapedData: TableMetadata? = null

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
            scrapedData?.let { dataSyncMetric.record<Unit> { dataSync.syncData(it) } }
        }
        logger.info("Processing schema: {} finished!", schema)
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

    private fun getCatalog(): List<String> {
        logger.info("Fetching catalogs...")
        // return spark.sql("show catalogs").collectAsList().map { it.getString(0) }
        val domain = Optional.ofNullable(System.getenv("IOMETE_DOMAIN")).orElse("default")
        return sqlClient.catalogs(domain).toList()
    }

    private fun getSchemas(catalog: String): List<String> {
        logger.info("Fetching schemas in catalog: {}... excludeSchemas: {}", catalog, excludeSchemas)

        try {
            return spark.sql("show databases in $catalog")
                .collectAsList().map { it.getString(0) }
                .filter { schemaName -> schemaName.isNotBlank() } // filter out empty schema names
                .filter { schemaName -> !excludeSchemas.contains(schemaName) }

        } catch (ex: Exception) {
            logger.warn("Couldn't fetch schemas in catalog: {}", catalog, ex)
            return emptyList()
        }
    }

    private fun getTables(catalog: String, schema: String): List<Row> {
        return spark.sql("show tables from $catalog.$schema").collectAsList()
    }

    private fun describeTable(catalog: String, schema: String, tableName: String): TableDescription {
        logger.info("describeTable for {}", tableName)

        var rawColumns: List<Row> = listOf()
        try {
            rawColumns = spark.sql("describe extended $catalog.$schema.`$tableName`").collectAsList()
        } catch (ex: Exception) {
            logger.warn("Couldn't describeTable for {}.{}.{}", catalog, schema, tableName, ex)
        }
        var sortOrder = 0
        var columnFlag = true
        var partitionFlag = false
        var metadataFlag = false

        val columnsMap = mutableMapOf<String, ColumnMetadata>()
        val metadataMap = mutableMapOf<String, String>()
        for (row in rawColumns) {
            val columnName = row.getString("col_name").orEmpty()
            val dataType = row.getString("data_type").orEmpty()
            val comment = row.getString("comment")

            when {
                columnName == "" -> columnFlag = false
                columnName.contains("# Partitioning") -> partitionFlag = true
                columnName.contains("# Detailed Table Information") -> {
                    metadataFlag = true
                    partitionFlag = false
                }
            }

            when {
                columnFlag -> {
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

                partitionFlag -> {
                    // sometimes partition name is in columnName, but sometimes it in dataType (iceberg, delta)
                    val partitionColName =
                        if (columnName.contains("Part "))
                            dataType
                        else
                            columnName
                    columnsMap[partitionColName]?.isPartitionKey = true
                }

                metadataFlag -> {
                    metadataMap[columnName] = dataType
                }
            }
        }

        return TableDescription(
            columns = columnsMap.values.toList(),
            metadata = metadataMap
        )
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

    data class TableDescription(val columns: List<ColumnMetadata>, val metadata: Map<String, String>)

    data class LogMetric(val name: String, val tag: String, val totalTime: Double?)
}
