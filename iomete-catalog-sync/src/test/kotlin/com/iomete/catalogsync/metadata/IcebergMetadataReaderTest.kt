package com.iomete.catalogsync.metadata

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.Snapshot
import org.apache.iceberg.Table
import org.apache.iceberg.types.Types
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class IcebergMetadataReaderTest {
    private val spark = mockk<SparkSession>(relaxed = true)

    @Test
    fun `loads columns comments partition flags spec properties and summary metrics from iceberg table`() {
        val schema = Schema(
            Types.NestedField.required(1, "id", Types.LongType.get(), "record id"),
            Types.NestedField.optional(2, "region", Types.StringType.get(), "sales region"),
            Types.NestedField.optional(3, "amount", Types.DecimalType.of(10, 2), "order amount"),
        )
        val partitionSpec = PartitionSpec.builderFor(schema).identity("region").build()
        val firstSnapshot = snapshot(
            timestampMillis = 1_700_000_000_000,
            summary = mapOf(
                "total-data-files" to "3",
                "total-files-size" to "500",
                "total-records" to "100",
                "added-data-files" to "3",
                "added-files-size" to "500",
            ),
        )
        val currentSnapshot = snapshot(
            timestampMillis = 1_700_000_500_000,
            summary = mapOf(
                "total-data-files" to "5",
                "total-files-size" to "1024",
                "total-records" to "250",
                "added-data-files" to "7",
                "added-files-size" to "900",
            ),
        )
        val table = table(
            schema = schema,
            spec = partitionSpec,
            properties = mapOf(
                "comment" to "orders table",
                "owner" to "analytics",
                "hidden" to "true",
            ),
            currentSnapshot = currentSnapshot,
            snapshots = listOf(currentSnapshot, firstSnapshot),
        )

        val metadata = readerReturning(table).loadTableMetadata(spark, "cat", "sales", "orders")

        assertEquals(
            mapOf("comment" to "orders table", "owner" to "analytics", "hidden" to "true"),
            metadata.tableProperties,
        )
        assertEquals("MANAGED", metadata.tableDescription.metadata["Type"])
        assertEquals("iceberg", metadata.tableDescription.metadata["Provider"])
        assertEquals("orders table", metadata.tableDescription.metadata["Comment"])
        assertEquals("analytics", metadata.tableDescription.metadata["Owner"])
        assertEquals("region", metadata.tableDescription.metadata["Partition Spec"])
        assertEquals(
            "[comment=orders table, hidden=true, owner=analytics]",
            metadata.tableDescription.metadata["Table Properties"],
        )

        val columns = metadata.tableDescription.columns
        assertEquals(listOf("id", "region", "amount"), columns.map { it.name })
        assertEquals(listOf("bigint", "string", "decimal(10,2)"), columns.map { it.dataType })
        assertEquals(listOf("record id", "sales region", "order amount"), columns.map { it.description })
        assertEquals(listOf(0, 1, 2), columns.map { it.sortOrder })
        assertFalse(columns.single { it.name == "id" }.isPartitionKey)
        assertTrue(columns.single { it.name == "region" }.isPartitionKey)
        assertFalse(columns.single { it.name == "amount" }.isPartitionKey)

        assertNotNull(metadata.statistics)
        assertEquals(currentSnapshot.timestampMillis(), metadata.statistics!!.lastModified)
        assertEquals(5L, metadata.statistics.numFiles)
        assertEquals(10L, metadata.statistics.totalTableNumFiles)
        assertEquals(1024L, metadata.statistics.sizeInBytes)
        assertEquals(1400L, metadata.statistics.totalTableSizeInBytes)
        assertEquals(250L, metadata.statistics.totalRecords)
    }

    @Test
    fun `formats primitive and nested iceberg types using spark compatible type strings`() {
        val schema = Schema(
            Types.NestedField.required(1, "bool_col", Types.BooleanType.get()),
            Types.NestedField.required(2, "int_col", Types.IntegerType.get()),
            Types.NestedField.required(3, "long_col", Types.LongType.get()),
            Types.NestedField.required(4, "float_col", Types.FloatType.get()),
            Types.NestedField.required(5, "double_col", Types.DoubleType.get()),
            Types.NestedField.required(6, "date_col", Types.DateType.get()),
            Types.NestedField.required(7, "ts_col", Types.TimestampType.withoutZone()),
            Types.NestedField.required(8, "ts_with_zone_col", Types.TimestampType.withZone()),
            Types.NestedField.required(9, "string_col", Types.StringType.get()),
            Types.NestedField.required(10, "binary_col", Types.BinaryType.get()),
            Types.NestedField.required(11, "decimal_col", Types.DecimalType.of(12, 4)),
            Types.NestedField.required(12, "list_col", Types.ListType.ofOptional(13, Types.StringType.get())),
            Types.NestedField.required(
                14,
                "map_col",
                Types.MapType.ofOptional(15, 16, Types.StringType.get(), Types.LongType.get()),
            ),
            Types.NestedField.required(
                17,
                "struct_col",
                Types.StructType.of(
                    Types.NestedField.required(18, "nested_id", Types.IntegerType.get()),
                    Types.NestedField.optional(19, "nested_name", Types.StringType.get()),
                ),
            ),
        )
        val table = table(
            schema = schema,
            spec = PartitionSpec.unpartitioned(),
            currentSnapshot = null,
            snapshots = emptyList(),
        )

        val columns = readerReturning(table).loadTableMetadata(spark, "cat", "sch", "tbl").tableDescription.columns

        assertEquals(
            listOf(
                "boolean",
                "int",
                "bigint",
                "float",
                "double",
                "date",
                "timestamp_ntz",
                "timestamp",
                "string",
                "binary",
                "decimal(12,4)",
                "array<string>",
                "map<string,bigint>",
                "struct<nested_id:int,nested_name:string>",
            ),
            columns.map { it.dataType },
        )
    }

    @Test
    fun `throws for unsupported iceberg time type`() {
        val schema = Schema(
            Types.NestedField.required(1, "time_col", Types.TimeType.get()),
        )
        val table = table(
            schema = schema,
            spec = PartitionSpec.unpartitioned(),
            currentSnapshot = null,
            snapshots = emptyList(),
        )

        val exception = assertThrows(UnsupportedOperationException::class.java) {
            readerReturning(table).loadTableMetadata(spark, "cat", "sch", "tbl")
        }

        assertEquals("Spark does not support Iceberg time fields", exception.message)
    }

    @Test
    fun `returns null statistics for table with no current snapshot`() {
        val schema = Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()))
        val table = table(
            schema = schema,
            spec = PartitionSpec.unpartitioned(),
            currentSnapshot = null,
            snapshots = emptyList(),
        )

        val metadata = readerReturning(table).loadTableMetadata(spark, "cat", "sch", "empty_table")

        assertNull(metadata.statistics)
        assertEquals(listOf("id"), metadata.tableDescription.columns.map { it.name })
        assertEquals("[]", metadata.tableDescription.metadata["Partition Spec"])
    }

    @Test
    fun `handles missing summary values without failing`() {
        val schema = Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()))
        val firstSnapshot = snapshot(
            timestampMillis = 10,
            summary = mapOf("total-data-files" to "2", "added-data-files" to "2"),
        )
        val currentSnapshot = snapshot(
            timestampMillis = 20,
            summary = mapOf("total-data-files" to "4", "added-data-files" to "2"),
        )
        val table = table(
            schema = schema,
            spec = PartitionSpec.unpartitioned(),
            currentSnapshot = currentSnapshot,
            snapshots = listOf(firstSnapshot, currentSnapshot),
        )

        val stats = readerReturning(table).loadTableMetadata(spark, "cat", "sch", "tbl").statistics

        assertNotNull(stats)
        assertEquals(4L, stats!!.numFiles)
        assertEquals(4L, stats.totalTableNumFiles)
        assertNull(stats.sizeInBytes)
        assertNull(stats.totalTableSizeInBytes)
        assertNull(stats.totalRecords)
    }

    @Test
    fun `returns null historical totals instead of drifting when added summary values are missing`() {
        val schema = Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()))
        val firstSnapshot = snapshot(
            timestampMillis = 10,
            summary = mapOf(
                "total-data-files" to "2",
                "total-files-size" to "100",
                "added-data-files" to "2",
                "added-files-size" to "100",
            ),
        )
        val currentSnapshot = snapshot(
            timestampMillis = 20,
            summary = mapOf(
                "total-data-files" to "4",
                "total-files-size" to "250",
                "added-files-size" to "150",
            ),
        )
        val table = table(
            schema = schema,
            spec = PartitionSpec.unpartitioned(),
            currentSnapshot = currentSnapshot,
            snapshots = listOf(firstSnapshot, currentSnapshot),
        )

        val stats = readerReturning(table).loadTableMetadata(spark, "cat", "sch", "tbl").statistics

        assertNotNull(stats)
        assertEquals(4L, stats!!.numFiles)
        assertNull(stats.totalTableNumFiles)
        assertEquals(250L, stats.sizeInBytes)
        assertEquals(250L, stats.totalTableSizeInBytes)
    }

    @Test
    fun `computes historical totals from available retained snapshot summaries`() {
        val schema = Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()))
        val retainedFirstSnapshot = snapshot(
            timestampMillis = 10,
            summary = mapOf(
                "total-data-files" to "100",
                "total-files-size" to "1000",
                "added-data-files" to "25",
                "added-files-size" to "250",
            ),
        )
        val currentSnapshot = snapshot(
            timestampMillis = 20,
            summary = mapOf(
                "total-data-files" to "90",
                "total-files-size" to "950",
                "added-data-files" to "10",
                "added-files-size" to "100",
            ),
        )
        val table = table(
            schema = schema,
            spec = PartitionSpec.unpartitioned(),
            currentSnapshot = currentSnapshot,
            snapshots = listOf(retainedFirstSnapshot, currentSnapshot),
        )

        val stats = readerReturning(table).loadTableMetadata(spark, "cat", "sch", "tbl").statistics

        assertNotNull(stats)
        assertEquals(90L, stats!!.numFiles)
        assertEquals(110L, stats.totalTableNumFiles)
        assertEquals(950L, stats.sizeInBytes)
        assertEquals(1100L, stats.totalTableSizeInBytes)
    }

    @Test
    fun `marks transformed partition source columns as partition keys and exposes transform spec`() {
        val schema = Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "event_time", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(3, "payload", Types.StringType.get()),
        )
        val spec =
            PartitionSpec
                .builderFor(schema)
                .bucket("id", 16)
                .day("event_time")
                .build()
        val table = table(
            schema = schema,
            spec = spec,
            currentSnapshot = null,
            snapshots = emptyList(),
        )

        val metadata = readerReturning(table).loadTableMetadata(spark, "cat", "sch", "tbl")
        val columns = metadata.tableDescription.columns

        assertEquals("bucket[16](id), day(event_time)", metadata.tableDescription.metadata["Partition Spec"])
        assertTrue(columns.single { it.name == "id" }.isPartitionKey)
        assertTrue(columns.single { it.name == "event_time" }.isPartitionKey)
        assertFalse(columns.single { it.name == "payload" }.isPartitionKey)
    }

    @Test
    fun `uses snapshot summaries only and does not read manifests`() {
        val schema = Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()))
        val currentSnapshot = snapshot(
            timestampMillis = 20,
            summary = mapOf(
                "total-data-files" to "1",
                "total-files-size" to "100",
                "total-records" to "10",
                "added-data-files" to "1",
                "added-files-size" to "100",
            ),
        )
        val table = table(
            schema = schema,
            spec = PartitionSpec.unpartitioned(),
            currentSnapshot = currentSnapshot,
            snapshots = listOf(currentSnapshot),
        )

        readerReturning(table).loadTableMetadata(spark, "cat", "sch", "tbl")

        verify(exactly = 0) { table.io() }
        verify(exactly = 0) { currentSnapshot.allManifests(any()) }
    }

    private fun readerReturning(table: Table) = IcebergMetadataReader { _, _, _, _ -> table }

    private fun table(
        schema: Schema,
        spec: PartitionSpec,
        properties: Map<String, String> = emptyMap(),
        currentSnapshot: Snapshot?,
        snapshots: List<Snapshot>,
    ): Table {
        val table = mockk<Table>(relaxed = true)
        every { table.schema() } returns schema
        every { table.spec() } returns spec
        every { table.properties() } returns properties
        every { table.currentSnapshot() } returns currentSnapshot
        every { table.snapshots() } returns snapshots
        return table
    }

    private fun snapshot(
        timestampMillis: Long,
        summary: Map<String, String>,
    ): Snapshot {
        val snapshot = mockk<Snapshot>(relaxed = true)
        every { snapshot.timestampMillis() } returns timestampMillis
        every { snapshot.summary() } returns summary
        return snapshot
    }
}
