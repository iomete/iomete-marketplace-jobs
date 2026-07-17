package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.enforceTableExclusionRules
import com.iomete.catalogsync.extract.SupportColumnStatistics
import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.SupportTableStatistics
import com.iomete.catalogsync.extract.TableExtractorFactory
import com.iomete.catalogsync.extract.TableStatistics
import com.iomete.catalogsync.presidio.PIIDetectionService
import jakarta.inject.Singleton
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeParseException

private val logger = LoggerFactory.getLogger(TableMetadataExtractor::class.java)

@Singleton
class TableMetadataExtractor(
    private val tableExtractorFactory: TableExtractorFactory,
    private val piiDetectionService: PIIDetectionService,
    private val applicationConfig: ApplicationConfig,
    private val sparkMetadataReader: SparkMetadataReader,
    private val icebergMetadataReader: IcebergMetadataReader,
) {
    private val exclusionRules = applicationConfig.exclusionRules

    fun scrapeTable(
        spark: SparkSession,
        catalog: String,
        schema: String,
        tableName: String,
        isTemp: Boolean,
        useIcebergFastPath: Boolean = false,
    ): TableMetadata {
        val icebergMetadata =
            if (useIcebergFastPath) {
                loadIcebergMetadataSafe(spark, catalog, schema, tableName)
            } else {
                null
            }
        val table = icebergMetadata?.tableDescription ?: sparkMetadataReader.describeTable(spark, catalog, schema, tableName)

        val tableProperties = icebergMetadata?.tableProperties ?: parseIcebergPropertiesSafe(table.metadata["Table Properties"])

        exclusionRules.enforceTableExclusionRules(
            table = tableName,
            props = tableProperties,
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
                tableProperties = tableProperties,
            )

        val datasetStatistics: TableStatistics? =
            if (icebergMetadata != null) {
                icebergMetadata.statistics
            } else {
                when (tableExtractor) {
                    is SupportTableStatistics -> tableExtractor.extractTableStatistics()
                    else -> null
                }
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
            sparkApplicationId = spark.sparkContext().applicationId(),
        )
    }

    private fun loadIcebergMetadataSafe(
        spark: SparkSession,
        catalog: String,
        schema: String,
        tableName: String,
    ): IcebergLoadedTableMetadata? =
        try {
            icebergMetadataReader.loadTableMetadata(spark, catalog, schema, tableName)
        } catch (th: Throwable) {
            logger.warn(
                "Iceberg metadata fast path failed for {}.{}.{}, falling back to Spark DESCRIBE EXTENDED",
                catalog,
                schema,
                tableName,
                th,
            )
            null
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
}
