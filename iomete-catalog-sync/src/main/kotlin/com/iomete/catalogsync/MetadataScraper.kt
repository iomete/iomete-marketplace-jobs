package com.iomete.catalogsync

import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.extract.ColumnStat
import com.iomete.catalogsync.extract.SupportColumnStatistics
import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.SupportTableStatistics
import com.iomete.catalogsync.extract.TableExtractorFactory
import com.iomete.catalogsync.extract.TableStatistics
import com.iomete.catalogsync.presidio.PIIDetectionService
import com.iomete.catalogsync.utils.ExcludedItemException
import com.iomete.catalogsync.utils.enforceCatalogExclusionRules
import com.iomete.catalogsync.utils.enforceSchemaExclusionRules
import com.iomete.catalogsync.utils.enforceTableExclusionRules
import com.iomete.catalogsync.utils.ignoreExcluded
import com.iomete.catalogsync.utils.log
import jakarta.inject.Singleton
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.eclipse.microprofile.rest.client.inject.RestClient
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException
import java.util.Objects
import java.util.concurrent.atomic.AtomicInteger
import kotlin.collections.map
import kotlin.collections.set

private val logger = LoggerFactory.getLogger(MetadataScraper::class.java)

@Singleton
class MetadataScraper(
    private val sparkSessionProvider: SparkSessionProvider,
    private val applicationConfig: ApplicationConfig,
    private val tableExtractorFactory: TableExtractorFactory,
    private val piiDetectionService: PIIDetectionService,
    @param:RestClient val coreServiceClient: CoreClient,
    @param:RestClient val catalogServiceClient: CatalogClient,
) {
    private val viewSupportedCatalogTypes = setOf("iceberg", "glue", "rest")
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
                    getSchemas(spark, catalog.name)
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
                    .build(catalog, processedSchemas)
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
            props = getSchemaProperties(spark, catalog.name, schema),
        )

        val failures = AtomicInteger(0)

        val tables =
            getTables(spark, catalog, schema)
                .parallelStream()
                .map { t ->
                    try {
                        scrapeTable(spark, catalog.name, schema, t.name, t.isTemp)
                            .also { it.log() }
                            .also { catalogServiceClient.indexTable(it) }
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
        )
    }

    private fun scrapeTable(
        spark: SparkSession,
        catalog: String,
        schema: String,
        tableName: String,
        isTemp: Boolean,
    ): TableMetadata {
        val table = describeTable(spark, catalog, schema, tableName)

        exclusionRules.enforceTableExclusionRules(
            table = tableName,
            props = parseIcebergPropertiesSafe(table.metadata["Table Properties"]),
        )

        var tableType = table.metadata.getOrDefault("Type", "UNKNOWN")
        val isView = tableType.equals("view", ignoreCase = true)

        val tableProvider = table.metadata.getOrDefault("Provider", "UNKNOWN")
        if (tableType == "UNKNOWN" && tableProvider == "iceberg") {
            tableType = "MANAGED"
        }

        val tableExtractor =
            tableExtractorFactory.extractorFor(
                spark = spark,
                provider = tableProvider,
                isView = isView,
                catalog = catalog,
                schema = schema,
                table = tableName,
            )

        val datasetStatistics: TableStatistics? =
            when (tableExtractor) {
                is SupportTableStatistics -> tableExtractor.extractTableStatistics()
                else -> null
            }

        val columnMetadataList: List<ColumnMetadata> = table.columns

        if (tableExtractor is SupportColumnTags) {
            val columnTags =
                piiDetectionService.extract(
                    spark = spark,
                    catalog = catalog,
                    fullTableName = "`$catalog`.`$schema`.`$tableName`",
                    columns = columnMetadataList.map { it.name },
                )

            columnMetadataList.forEach { columnMetadata ->
                columnMetadata.tags = columnTags[columnMetadata.name] ?: listOf()
            }
        }

        if (tableExtractor is SupportColumnStatistics) {
            val columnStatistics =
                tableExtractor.extractColumnStatistics(columns = columnMetadataList.map { it.name })
            columnMetadataList.forEach { columnMetadata ->
                columnMetadata.stats = columnStatistics[columnMetadata.name] ?: listOf()
            }
        }

        val tableTags =
            columnMetadataList
                .flatMap { it.tags }
                .filter { it.contains("PII") || it.contains("PCI") }
                .distinct()
                .toList()

        var creationTime: Long? = null
        try {
            val tableCreationTime = table.metadata.getOrDefault("Created Time", null)
            if (tableCreationTime != null) {
                creationTime =
                    LocalDateTime
                        .parse(
                            tableCreationTime,
                            DateTimeFormatter.ofPattern("E MMM dd HH:mm:ss zzz yyyy"),
                        ).toEpochSecond(ZoneOffset.UTC)
            }
        } catch (ex: DateTimeParseException) {
            logger.warn(
                "error parsing table creation time for {}.{}.{} time={}, error={}",
                catalog,
                schema,
                tableName,
                table.metadata.getOrDefault("Created Time", ""),
                ex.localizedMessage,
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
            syncTime = Instant.now().toEpochMilli(),
        )
    }

    private fun getSchemas(
        spark: SparkSession,
        catalog: String,
    ): List<String> {
        logger.info("Fetching schemas in catalog {}", catalog)

        try {
            return spark
                .sql("show databases in `$catalog`")
                .collectAsList()
                .map { it.getString(0) }
        } catch (th: Throwable) {
            logger.warn("Couldn't fetch schemas in catalog: {}", catalog, th)
            return emptyList()
        }
    }

    fun getSchemaProperties(
        spark: SparkSession,
        catalog: String,
        schemaName: String,
    ): Map<String, String> {
        val dbExtended =
            try {
                spark
                    .sql("DESC DATABASE EXTENDED `$catalog`.`$schemaName`")
                    .collectAsList()
                    .associate { it.getString(0).lowercase() to it.getString(1) }
            } catch (th: Throwable) {
                logger.warn("Couldn't fetch schema properties in catalog: {}", catalog, th)
                return emptyMap()
            }

        val properties = dbExtended["properties"]

        if (properties.isNullOrBlank()) return emptyMap()

        return properties
            .trim()
            .removePrefix("(")
            .removeSuffix(")")
            .split("),")
            .mapNotNull { pair ->
                val cleaned =
                    pair
                        .removePrefix("(")
                        .removeSuffix(")")
                        .trim()

                if (cleaned.isBlank() || !cleaned.contains(",")) return@mapNotNull null

                val parts = cleaned.split(",", limit = 2)
                val key = parts.getOrNull(0)?.trim()
                val value = parts.getOrNull(1)?.trim()

                if (key.isNullOrEmpty() || value.isNullOrEmpty()) {
                    null
                } else {
                    key to value
                }
            }.toMap()
    }

    private fun parseIcebergPropertiesSafe(input: String?): Map<String, String> {
        if (input.isNullOrBlank()) return emptyMap()

        val trimmed = input.trim()
        if (trimmed == "[]" || !trimmed.startsWith("[") || !trimmed.endsWith("]")) {
            return emptyMap()
        }

        return trimmed
            .removeSurrounding("[", "]")
            .takeIf { it.isNotBlank() }
            ?.split(",")
            ?.mapNotNull { entry ->
                val idx = entry.indexOf("=")
                if (idx <= 0 || idx == entry.lastIndex) return@mapNotNull null

                val key = entry.take(idx).trim()
                val value = entry.substring(idx + 1).trim()

                if (key.isEmpty() || value.isEmpty()) null else key to value
            }?.toMap()
            ?: emptyMap()
    }

    fun getTables(
        spark: SparkSession,
        catalog: CatalogDetails,
        schema: String,
    ): List<ShowTablesRow> {
        val rows = fetchTables(spark, catalog.name, schema) + fetchViews(spark, catalog, schema)
        return rows
            .distinctBy { it.getString(1) }
            .map {
                ShowTablesRow(name = it.getString(1), isTemp = it.getBoolean(2))
            }
    }

    private fun fetchTables(
        spark: SparkSession,
        catalog: String,
        schema: String,
    ): List<Row> =
        try {
            spark.sql("show tables from `$catalog`.`$schema`").collectAsList()
        } catch (th: Throwable) {
            logger.warn("Failed to fetch tables for catalog {} & schema {}", catalog, schema, th)
            emptyList()
        }

    private fun fetchViews(
        spark: SparkSession,
        catalog: CatalogDetails,
        schema: String,
    ): List<Row> {
        val hasViewSupport = catalog.type.any { viewSupportedCatalogTypes.contains(it.lowercase()) }
        if (!hasViewSupport) return emptyList()

        return try {
            spark.sql("show views from `${catalog.name}`.`$schema`").collectAsList()
        } catch (th: Throwable) {
            logger.warn("Failed to fetch views for catalog {} & schema {}", catalog, schema, th)
            emptyList()
        }
    }

    private fun describeTable(
        spark: SparkSession,
        catalog: String,
        schema: String,
        tableName: String,
    ): TableDescription {
        logger.info("describeTable for {}", tableName)

        var rawColumns: List<Row> = listOf()
        try {
            rawColumns = spark.sql("describe extended `$catalog`.`$schema`.`$tableName`").collectAsList()
        } catch (th: Throwable) {
            logger.warn("Couldn't describeTable for {}.{}.{}", catalog, schema, tableName, th)
        }

        return processTableColumns(rawColumns)
    }

    fun processTableColumns(rawColumns: List<Row>): TableDescription {
        var sortOrder = 0
        var currentSection: TableColumnSection = TableColumnSection.COLUMNS
        val sectionHeaders =
            mapOf(
                "# Partition Information" to TableColumnSection.PARTITIONS,
                "# Partitioning" to TableColumnSection.PARTITIONS,
                "# Metadata Columns" to TableColumnSection.METADATA,
                "# Detailed Table Information" to TableColumnSection.TABLE_INFO,
                "# Detailed View Information" to TableColumnSection.VIEW_INFO,
            )

        val columnsMap = mutableMapOf<String, ColumnMetadata>()
        val metadataMap = mutableMapOf<String, String>()
        for (row in rawColumns) {
            val columnName = row.getString(0).orEmpty()
            val dataType = row.getString(1).orEmpty()
            val comment = row.getString(2)

            if (columnName.startsWith("#") || columnName.isBlank()) {
                val matchedSection =
                    sectionHeaders.entries
                        .find { entry ->
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
                    val columnMetadata =
                        ColumnMetadata(
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
                        if (columnName.contains("Part ")) {
                            dataType
                        } else {
                            columnName
                        }
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
            metadata = metadataMap,
        )
    }

    data class ShowTablesRow(
        val name: String,
        val isTemp: Boolean,
    )

    data class TableProcessResult(
        val table: String,
        val metadata: TableMetadata? = null,
        val error: Throwable? = null,
    )

    data class TableDescription(
        val columns: List<ColumnMetadata>,
        val metadata: Map<String, String>,
    )

    data class LogMetric(
        val name: String,
        val tag: String,
        val totalTime: Double?,
    )

    data class CatalogMetadata(
        val catalog: String,
        val type: Set<String>, // ex. ["INTERNAL", "ICEBERG"] or ["EXTERNAL", "JDBC"]
        val location: String?,
        val storageEndpoint: String?,
        val totalSchemaCount: Int,
        val totalTableCount: Int,
        val totalSizeInBytes: Long,
        val totalFiles: Long,
    ) {
        companion object {
            fun build(
                catalog: CatalogDetails,
                schemas: List<SchemaMetadata>,
            ) = CatalogMetadata(
                catalog = catalog.name,
                type = catalog.type.toSet(),
                location = catalog.location,
                storageEndpoint = catalog.storageEndpoint,
                totalSchemaCount = schemas.size,
                totalTableCount = schemas.sumOf { it.totalTableCount },
                totalSizeInBytes = schemas.sumOf { it.totalSizeInBytes },
                totalFiles = schemas.sumOf { it.totalFiles },
            )
        }
    }

    data class SchemaMetadata(
        val catalog: String,
        val schema: String,
        val totalTableCount: Int,
        val totalViewCount: Int,
        val totalSizeInBytes: Long,
        val totalDbSizeInBytes: Long,
        val totalFiles: Long,
        val failedTableCount: Int,
    ) {
        companion object {
            fun build(
                catalog: String,
                schema: String,
                tables: List<TableMetadata>,
                failuresSize: Int,
            ) = SchemaMetadata(
                catalog = catalog,
                schema = schema,
                totalTableCount = tables.filterNot { it.isView }.size,
                totalViewCount = tables.filter { it.isView }.size,
                totalSizeInBytes = tables.sumOf { it.sizeInBytes ?: 0L },
                totalDbSizeInBytes = tables.sumOf { it.totalTableSizeInBytes ?: 0L },
                totalFiles = tables.sumOf { it.numFiles ?: 0L },
                failedTableCount = failuresSize,
            )
        }
    }

    data class TableMetadata(
        val catalog: String,
        val schema: String,
        val name: String,
        val description: String?,
        val tableType: String, // MANAGED, EXTERNAL, VIEW
        val isView: Boolean,
        val isTemporary: Boolean,
        val owner: String,
        // Managed providers: hive,delta
        // External providers: org.apache.spark.sql.json,com.databricks.spark.csv,...
        val provider: String?,
        val viewText: String?,
        val createdAt: Long? = null,
        val lastModified: Long? = null,
        val numFiles: Long? = null,
        val totalTableNumFiles: Long? = null,
        val sizeInBytes: Long? = null,
        val totalTableSizeInBytes: Long? = null,
        val totalRecords: Long? = null,
        val columns: List<ColumnMetadata>,
        var tags: List<String> = listOf(),
        val syncTime: Long,
    )

    data class ColumnMetadata(
        val name: String,
        val dataType: String,
        val description: String?,
        val sortOrder: Int,
        var isPartitionKey: Boolean,
        var stats: List<ColumnStat> = listOf(),
        var tags: List<String> = listOf(),
    )

    enum class TableColumnSection {
        COLUMNS,
        PARTITIONS,
        METADATA,
        TABLE_INFO,
        VIEW_INFO,
    }
}
