package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.extract.TableStatistics
import jakarta.inject.Singleton
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Snapshot
import org.apache.iceberg.Table
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import org.apache.spark.sql.SparkSession

private const val ICEBERG_PROVIDER = "iceberg"
private const val TABLE_COMMENT_PROPERTY = "comment"
private const val TABLE_OWNER_PROPERTY = "owner"
private const val PARTITION_SPEC_METADATA = "Partition Spec"

private const val TOTAL_DATA_FILES = "total-data-files"
private const val TOTAL_FILES_SIZE = "total-files-size"
private const val TOTAL_RECORDS = "total-records"
private const val ADDED_DATA_FILES = "added-data-files"
private const val ADDED_FILES_SIZE = "added-files-size"

data class IcebergLoadedTableMetadata(
    val tableDescription: TableDescription,
    val tableProperties: Map<String, String>,
    val statistics: TableStatistics?,
)

@Singleton
class IcebergMetadataReader {
    private val tableLoader: (
        spark: SparkSession,
        catalog: String,
        schemaName: String,
        tableName: String,
    ) -> Table

    constructor() : this(::loadIcebergTable)

    internal constructor(
        tableLoader: (
            spark: SparkSession,
            catalog: String,
            schemaName: String,
            tableName: String,
        ) -> Table,
    ) {
        this.tableLoader = tableLoader
    }

    fun loadTableMetadata(
        spark: SparkSession,
        catalog: String,
        schemaName: String,
        tableName: String,
    ): IcebergLoadedTableMetadata {
        val table = tableLoader(spark, catalog, schemaName, tableName)
        val icebergSchema = table.schema()
        val partitionSpec = table.spec()
        val properties = table.properties().orEmpty()
        val partitionSourceIds = partitionSpec?.fields().orEmpty().map { it.sourceId() }.toSet()

        return IcebergLoadedTableMetadata(
            tableDescription = TableDescription(
                columns = icebergSchema.columns().mapIndexed { index, field ->
                    ColumnMetadata(
                        name = field.name(),
                        dataType = toSparkType(field.type()),
                        description = field.doc(),
                        sortOrder = index,
                        isPartitionKey = field.fieldId() in partitionSourceIds,
                    )
                },
                metadata = buildMetadata(properties, partitionSpecString(icebergSchema, partitionSpec)),
            ),
            tableProperties = properties,
            statistics = extractStatistics(table),
        )
    }

    private fun buildMetadata(
        properties: Map<String, String>,
        partitionSpec: String,
    ): Map<String, String> =
        buildMap {
            put("Provider", ICEBERG_PROVIDER)
            properties[TABLE_COMMENT_PROPERTY]?.let { put("Comment", it) }
            properties[TABLE_OWNER_PROPERTY]?.let { put("Owner", it) }
            put("Table Properties", formatTableProperties(properties))
            put(PARTITION_SPEC_METADATA, partitionSpec)
        }

    private fun extractStatistics(table: Table): TableStatistics? {
        val currentSnapshot = table.currentSnapshot() ?: return null
        val snapshots = table.snapshots().toList().ifEmpty { listOf(currentSnapshot) }
        val firstSnapshot = snapshots.minByOrNull { it.timestampMillis() } ?: currentSnapshot
        val currentSummary = currentSnapshot.summary().orEmpty()

        return TableStatistics(
            lastModified = currentSnapshot.timestampMillis(),
            numFiles = currentSummary.longValue(TOTAL_DATA_FILES),
            totalTableNumFiles = historicalTotalFromAvailableSummaries(
                snapshots = snapshots,
                firstSnapshot = firstSnapshot,
                totalKey = TOTAL_DATA_FILES,
                addedKey = ADDED_DATA_FILES,
            ),
            sizeInBytes = currentSummary.longValue(TOTAL_FILES_SIZE),
            totalTableSizeInBytes = historicalTotalFromAvailableSummaries(
                snapshots = snapshots,
                firstSnapshot = firstSnapshot,
                totalKey = TOTAL_FILES_SIZE,
                addedKey = ADDED_FILES_SIZE,
            ),
            totalRecords = currentSummary.longValue(TOTAL_RECORDS),
        )
    }

    private fun historicalTotalFromAvailableSummaries(
        snapshots: List<Snapshot>,
        firstSnapshot: Snapshot,
        totalKey: String,
        addedKey: String,
    ): Long {
        // Matches the legacy `$table.snapshots` SQL aggregation, which COALESCEd missing
        // summary values to 0: baseline is the earliest retained snapshot's total, plus
        // added deltas from every later snapshot. Iceberg omits `added-*` keys for
        // snapshots that add no files (e.g. delete-only commits), so missing means 0.
        val firstSummary = firstSnapshot.summary().orEmpty()
        val firstTotal = firstSummary.longValue(totalKey) ?: 0L
        val firstAdded = firstSummary.longValue(addedKey) ?: 0L
        val addedSum = snapshots.sumOf { it.summary().orEmpty().longValue(addedKey) ?: 0L }
        return firstTotal + (addedSum - firstAdded)
    }

    private fun Map<String, String>.longValue(key: String): Long? = this[key]?.toLongOrNull()
}

private fun toSparkType(type: Type): String =
    when (type.typeId()) {
        Type.TypeID.TIME -> throw UnsupportedOperationException("Spark does not support Iceberg time fields")
        Type.TypeID.TIMESTAMP -> {
            val timestamp = type.asPrimitiveType() as Types.TimestampType
            sparkTimestampType(timestamp.shouldAdjustToUTC())
        }
        Type.TypeID.TIMESTAMP_NANO -> {
            val timestamp = type.asPrimitiveType() as Types.TimestampNanoType
            sparkTimestampType(timestamp.shouldAdjustToUTC())
        }
        else -> SparkSchemaUtil.convert(type).catalogString()
    }

private fun sparkTimestampType(shouldAdjustToUTC: Boolean): String =
    if (shouldAdjustToUTC) {
        "timestamp"
    } else {
        "timestamp_ntz"
    }

private fun partitionSpecString(
    icebergSchema: Schema,
    spec: PartitionSpec?,
): String {
    val fields = spec?.fields().orEmpty()
    if (fields.isEmpty()) return "[]"

    return fields.joinToString(", ") { field ->
        val sourceName = icebergSchema.findColumnName(field.sourceId()) ?: field.sourceId().toString()
        if (field.transform().isIdentity) {
            sourceName
        } else {
            "${field.transform()}($sourceName)"
        }
    }
}

private fun loadIcebergTable(
    spark: SparkSession,
    catalog: String,
    schemaName: String,
    tableName: String,
): Table = Spark3Util.loadIcebergTable(spark, quotedFullName(catalog, schemaName, tableName))

private fun quotedFullName(vararg parts: String): String =
    parts.joinToString(".") { part -> "`${part.replace("`", "``")}`" }

private fun formatTableProperties(properties: Map<String, String>): String =
    if (properties.isEmpty()) {
        "[]"
    } else {
        properties
            .toSortedMap()
            .entries
            .joinToString(prefix = "[", postfix = "]") { (key, value) -> "$key=$value" }
    }
