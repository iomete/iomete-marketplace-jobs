package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.extract.ColumnStat
import jakarta.inject.Singleton
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger(SparkMetadataReader::class.java)

@Singleton
class SparkMetadataReader {
    private val viewSupportedCatalogTypes = setOf("iceberg", "glue", "rest")

    fun getSchemas(
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

    fun describeTable(
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

    private fun processTableColumns(rawColumns: List<Row>): TableDescription {
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
}

data class ShowTablesRow(
    val name: String,
    val isTemp: Boolean,
)

data class TableDescription(
    val columns: List<ColumnMetadata>,
    val metadata: Map<String, String>,
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
