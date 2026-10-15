package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

private const val TABLE = "`c`.`sales`.`events`"
private const val LOCATION = "s3a://b/lake/sales.db/events"

class TableOwnershipDiscoveryTest {

    private val spark = mockk<SparkSession>()

    private val discovery =
        CatalogDiscoveryService().apply {
            sparkSessionProvider = mockk<SparkSessionProvider>().also { every { it.getOrCreate() } returns spark }
        }

    @Test
    fun `an Iceberg table reports its declared write paths`() {
        listsOneTable(provider = "iceberg")
        rows("SHOW TBLPROPERTIES $TABLE", "write.data.path" to "s3a://b/lake/sales.db/events_data", "write.metadata.path" to " ", "format" to "iceberg/parquet")

        val table = discovery.discoverTables("c", "sales").single()

        assertEquals("s3a://b/lake/sales.db/events", table.location)
        assertEquals(listOf("s3a://b/lake/sales.db/events_data"), table.writeLocations)
    }

    @Test
    fun `a Hive table does not read Iceberg properties`() {
        listsOneTable(provider = "hive")

        val table = discovery.discoverTables("c", "sales").single()

        assertEquals(emptyList<String>(), table.writeLocations)
        verify(exactly = 0) { spark.sql("SHOW TBLPROPERTIES $TABLE") }
    }

    @Test
    fun `an Iceberg table with garbage collection off is unresolved`() {
        listsOneTable(provider = "iceberg")
        rows("SHOW TBLPROPERTIES $TABLE", "gc.enabled" to "false")

        val table = discovery.discoverTables("c", "sales").single()

        assertNull(table.location)
        assertEquals(true, table.isUnresolved)
    }

    @Test
    fun `an Iceberg table with a custom location provider is unresolved`() {
        listsOneTable(provider = "iceberg")
        rows("SHOW TBLPROPERTIES $TABLE", "write.location-provider.impl" to "com.example.Provider", "write.data.path" to "s3a://b/lake/sales.db/events_data")

        assertEquals(true, discovery.discoverTables("c", "sales").single().isUnresolved)
    }

    @Test
    fun `an Iceberg write path without a scheme or bucket is unresolved`() {
        listsOneTable(provider = "iceberg")
        rows("SHOW TBLPROPERTIES $TABLE", "write.data.path" to "/lake/sales.db/events_data")

        assertEquals(true, discovery.discoverTables("c", "sales").single().isUnresolved)
    }

    @Test
    fun `an Iceberg write path with a scheme but no bucket or host is unresolved`() {
        listsOneTable(provider = "iceberg")
        rows("SHOW TBLPROPERTIES $TABLE", "write.data.path" to "hdfs:/lake/sales.db/live_data")

        assertEquals(true, discovery.discoverTables("c", "sales").single().isUnresolved)
    }

    @Test
    fun `a table location without a scheme or bucket is unresolved`() {
        listsTables("events")
        describes(TABLE, details("Location" to "/lake/sales.db/events", "Provider" to "hive"))

        val table = discovery.discoverTables("c", "sales").single()

        assertNull(table.location)
        assertEquals(true, table.isUnresolved)
    }

    @Test
    fun `a column named provider is not read as the table provider`() {
        listsTables("events")
        describes(
            TABLE,
            listOf(column("id", "bigint"), column("provider"), header("# Partition Information"), RowFactory.create("# col_name", "data_type", "comment"), column("provider")) +
                details("Location" to LOCATION, "Provider" to "iceberg"),
        )
        rows("SHOW TBLPROPERTIES $TABLE", "write.data.path" to "s3a://b/lake/sales.db/events_data")

        assertEquals(listOf("s3a://b/lake/sales.db/events_data"), discovery.discoverTables("c", "sales").single().writeLocations)
    }

    @Test
    fun `a column named location is not read as the table location`() {
        listsTables("events")
        describes(
            TABLE,
            listOf(column("location")) + details("Location" to LOCATION, "Provider" to "parquet") +
                listOf(blank(), header("# Column Default Values"), RowFactory.create("location", "string", "'x'")),
        )

        val table = discovery.discoverTables("c", "sales").single()

        assertEquals(LOCATION, table.location)
        assertEquals(false, table.isUnresolved)
    }

    @Test
    fun `a column named like the details header is not read as the header`() {
        listsTables("events")
        describes(TABLE, listOf(column("# Detailed Table Information"), column("Location"), column("Provider")) + details("Location" to LOCATION, "Provider" to "iceberg"))
        rows("SHOW TBLPROPERTIES $TABLE", "write.data.path" to "s3a://b/lake/sales.db/events_data")

        val table = discovery.discoverTables("c", "sales").single()

        assertEquals(LOCATION, table.location)
        assertEquals(listOf("s3a://b/lake/sales.db/events_data"), table.writeLocations)
    }

    @Test
    fun `output without exactly one details section or with a repeated field is unresolved`() {
        listsTables("events", "orders", "refunds")
        describes(TABLE, listOf(column("id", "bigint")))
        describes("`c`.`sales`.`orders`", details("Location" to "s3a://b/other", "Provider" to "hive") + details("Location" to "s3a://b/lake/sales.db/orders", "Provider" to "hive"))
        describes("`c`.`sales`.`refunds`", details("Location" to "s3a://b/other", "Provider" to "hive", "Location" to "s3a://b/lake/sales.db/refunds"))

        assertEquals(listOf(true, true, true), discovery.discoverTables("c", "sales").map { it.isUnresolved })
    }

    @Test
    fun `a view has no location and stays resolved`() {
        listsTables("events")
        describes(TABLE, listOf(column("location", "int")) + details("Type" to "VIEW", "View Text" to "SELECT 1 AS location"))

        val table = discovery.discoverTables("c", "sales").single()

        assertNull(table.location)
        assertEquals(false, table.isUnresolved)
    }

    @Test
    fun `an Iceberg table whose properties cannot be read is unresolved`() {
        listsOneTable(provider = "iceberg")
        every { spark.sql("SHOW TBLPROPERTIES $TABLE") } throws IllegalStateException("denied")

        assertEquals(true, discovery.discoverTables("c", "sales").single().isUnresolved)
    }

    @Test
    fun `database locations map an unreadable database to null`() {
        rows("SHOW NAMESPACES IN `c`", "sales" to null, "broken" to null)
        rows("DESCRIBE DATABASE EXTENDED `c`.`sales`", "Location" to "s3a://b/lake/sales.db")
        every { spark.sql("DESCRIBE DATABASE EXTENDED `c`.`broken`") } throws IllegalStateException("denied")

        assertEquals(mapOf("sales" to "s3a://b/lake/sales.db", "broken" to null), discovery.visibleDatabaseLocations("c"))
    }

    @Test
    fun `a missing table is reported as absent and any other failure is thrown`() {
        every { spark.sql("DESCRIBE EXTENDED `c`.`default`.`sales`") } throws NoSuchTableException("default", "sales")
        every { spark.sql("DESCRIBE EXTENDED `c`.`default`.`events`") } throws
            IllegalStateException("lookup failed", NotFoundError())
        every { spark.sql("DESCRIBE EXTENDED `c`.`default`.`orders`") } throws IllegalStateException("access denied")
        every { spark.sql("DESCRIBE EXTENDED `c`.`default`.`table_or_view_not_found`") } throws
            IllegalStateException("Access denied to c.default.table_or_view_not_found")

        assertNull(discovery.tableLocationIfExists("c", "default", "sales"))
        assertNull(discovery.tableLocationIfExists("c", "default", "events"))
        assertThrows(IllegalStateException::class.java) { discovery.tableLocationIfExists("c", "default", "orders") }
        assertThrows(IllegalStateException::class.java) { discovery.tableLocationIfExists("c", "default", "table_or_view_not_found") }
    }

    private class NotFoundError : RuntimeException("not found"), SparkThrowable {
        override fun getErrorClass() = "TABLE_OR_VIEW_NOT_FOUND"
    }

    @Test
    fun `a table lookup reads the location from the details section and fails without one`() {
        describes("`c`.`default`.`sales`", listOf(column("location")) + details("Location" to "s3a://b/lake/sales", "Provider" to "hive"))
        describes("`c`.`default`.`orders`", listOf(column("location")))

        assertEquals("s3a://b/lake/sales", discovery.tableLocationIfExists("c", "default", "sales"))
        assertThrows(IllegalStateException::class.java) { discovery.tableLocationIfExists("c", "default", "orders") }
    }

    private fun listsOneTable(provider: String) {
        listsTables("events")
        describes(TABLE, listOf(column("id", "bigint")) + details("Location" to LOCATION, "Provider" to provider))
    }

    private fun listsTables(vararg tables: String) {
        every { spark.sql("SHOW TABLES FROM `c`.`sales`") } returns dataset(tables.map { RowFactory.create("sales", it, false) })
    }

    private fun describes(
        table: String,
        rows: List<Row>,
    ) {
        every { spark.sql("DESCRIBE EXTENDED $table") } returns dataset(rows)
    }

    private fun column(
        name: String,
        type: String = "string",
    ): Row = RowFactory.create(name, type, null)

    private fun header(name: String): Row = RowFactory.create(name, "", "")

    private fun blank(): Row = RowFactory.create("", "", "")

    // Spark 3.5 starts the table details with a blank row and a header that has no data type.
    private fun details(vararg fields: Pair<String, String>): List<Row> =
        listOf(blank(), header("# Detailed Table Information")) + fields.map { (name, value) -> RowFactory.create(name, value, "") }

    private fun rows(
        sql: String,
        vararg pairs: Pair<String, String?>,
    ) {
        every { spark.sql(sql) } returns dataset(pairs.map { (key, value) -> RowFactory.create(key, value) })
    }

    private fun dataset(rows: List<Row>): Dataset<Row> = mockk<Dataset<Row>>().also { every { it.collectAsList() } returns rows }
}
