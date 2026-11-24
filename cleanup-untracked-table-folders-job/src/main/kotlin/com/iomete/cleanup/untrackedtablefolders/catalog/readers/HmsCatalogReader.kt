package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import org.apache.iceberg.TableMetadataParser
import org.apache.iceberg.io.FileIO
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog
import scala.Option
import scala.collection.JavaConverters

/**
 * Reads the Hive metastore side of spark_catalog through Spark's V1 [ExternalCatalog], never through
 * SparkSessionCatalog, which hides an HMS table behind a REST table of the same name.
 *
 * [icebergIo] reads the metadata file of Iceberg tables registered in HMS. Without it they are gaps.
 */
class HmsCatalogReader(
    private val catalogName: String,
    private val externalCatalog: ExternalCatalog,
    private val icebergIo: FileIO?,
    private val viewPathParser: ViewPathParser = ViewPathParser(),
    private val limits: ReaderLimits = ReaderLimits(),
) {
    fun read(): CatalogReadResult {
        val databases = try {
            externalCatalog.listDatabases().asList()
        } catch (e: Exception) {
            return CatalogReadResult.failed(catalogName, gapFor(emptyList(), "list_databases_failed", e))
        }
        val result = ReadAccumulator(catalogName)
        databases.forEach { readDatabase(it, result) }
        return result.result()
    }

    private fun readDatabase(database: String, result: ReadAccumulator) {
        try {
            result.location(listOf(database), LocationRole.NAMESPACE, externalCatalog.getDatabase(database).locationUri()?.toString())
        } catch (e: Exception) {
            result.gap(listOf(database), "load_database_failed", e)
        }
        val tables = try {
            externalCatalog.listTables(database).asList()
        } catch (e: Exception) {
            result.gap(listOf(database), "list_tables_failed", e)
            return
        }
        tables.chunked(limits.tablesByNameBatch).forEach { readBatch(database, it, result) }
    }

    // getTablesByName fails the whole batch for one unreadable table and silently leaves out names it can't
    // find, so every name it didn't return is loaded on its own.
    private fun readBatch(database: String, names: List<String>, result: ReadAccumulator) {
        val loaded = try {
            externalCatalog.getTablesByName(database, JavaConverters.asScalaBuffer(names)).asList()
        } catch (_: Exception) {
            emptyList()
        }
        val byName = loaded.associateBy { it.identifier().table() }
        names.forEach { tableName ->
            val table = byName[tableName] ?: try {
                externalCatalog.getTable(database, tableName)
            } catch (e: Exception) {
                result.gap(listOf(database, tableName), "load_table_failed", e)
                null
            }
            if (table != null) readTable(listOf(database, tableName), table, result)
        }
    }

    private fun readTable(name: List<String>, table: CatalogTable, result: ReadAccumulator) {
        val location = table.storage().locationUri().orNull()?.toString()
        if (table.tableType().name() == "VIEW") {
            result.location(name, LocationRole.VIEW, location)
            viewPathParser.pathUse(name, table.viewText().orNull())?.let { result.externalPathUse += it }
            return
        }
        when {
            // JDBC tables keep their data in another database, as the SQL discovery also assumes.
            location == null && table.provider().orNull()?.lowercase() in JDBC_PROVIDERS -> Unit
            location.isNullOrBlank() -> result.gaps += ReadGap(name, ReadGapKind.UNVERIFIED, "table_location_missing")
            else -> result.location(name, LocationRole.TABLE, location)
        }
        if (table.storage().inputFormat().orNull() == SYMLINK_INPUT_FORMAT) {
            result.externalPathUse += ExternalPathUse(name, emptyList(), "symlink_manifest_table")
        }
        // Typed fields only, except these two keys: an HMS-registered Iceberg table shows provider hive.
        if (table.properties().get("table_type").orNull().equals("ICEBERG", ignoreCase = true)) {
            readRegisteredIceberg(name, table.properties().get("metadata_location").orNull(), result)
        }
        if (!table.partitionColumnNames().isEmpty) readPartitions(name, location, result)
    }

    private fun readRegisteredIceberg(name: List<String>, metadataLocation: String?, result: ReadAccumulator) {
        if (metadataLocation.isNullOrBlank() || icebergIo == null) {
            result.gaps += ReadGap(name, ReadGapKind.FAILED, "iceberg_metadata_not_readable")
            return
        }
        try {
            result.icebergTable(name, TableMetadataParser.read(icebergIo, metadataLocation))
        } catch (e: Exception) {
            result.gap(name, "iceberg_metadata_not_readable", e)
        }
    }

    private fun readPartitions(name: List<String>, tableLocation: String?, result: ReadAccumulator) {
        val (database, table) = name
        try {
            val count = externalCatalog.listPartitionNames(database, table, Option.empty()).size()
            if (count > limits.maxPartitionsPerTable) {
                result.gaps += ReadGap(name, ReadGapKind.LIMIT_EXCEEDED, "too_many_partitions")
                return
            }
            externalCatalog.listPartitions(database, table, Option.empty()).asList().forEach { partition ->
                val location = partition.storage().locationUri().orNull()?.toString()
                when {
                    location.isNullOrBlank() -> result.gaps += ReadGap(name, ReadGapKind.UNVERIFIED, "partition_location_missing")
                    !isInside(location, tableLocation) -> result.location(name, LocationRole.PARTITION, location)
                }
            }
        } catch (e: Exception) {
            result.gap(name, "list_partitions_failed", e)
        }
    }

    private fun isInside(location: String, tableLocation: String?): Boolean =
        tableLocation != null && runCatching { StoragePathUtils.isSameOrChildLocation(location, tableLocation) }.getOrDefault(false)

    private companion object {
        const val SYMLINK_INPUT_FORMAT = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat"
        val JDBC_PROVIDERS = setOf("jdbc", "org.apache.spark.sql.jdbc", "org.apache.spark.sql.jdbc.defaultsource")
    }
}

internal fun <T> Option<T>.orNull(): T? = if (isDefined) get() else null
