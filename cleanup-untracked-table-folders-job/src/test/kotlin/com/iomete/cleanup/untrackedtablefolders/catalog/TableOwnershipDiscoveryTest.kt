package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

private const val TABLE = "`c`.`sales`.`events`"

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
    fun `a table location without a scheme or bucket is unresolved`() {
        every { spark.sql("SHOW TABLES FROM `c`.`sales`") } returns dataset(listOf(RowFactory.create("sales", "events", false)))
        rows("DESCRIBE EXTENDED $TABLE", "Location" to "/lake/sales.db/events", "Provider" to "hive")

        val table = discovery.discoverTables("c", "sales").single()

        assertNull(table.location)
        assertEquals(true, table.isUnresolved)
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
        every { spark.sql("DESCRIBE EXTENDED `c`.`default`.`sales`") } throws
            IllegalStateException("[TABLE_OR_VIEW_NOT_FOUND] The table or view cannot be found")
        every { spark.sql("DESCRIBE EXTENDED `c`.`default`.`orders`") } throws IllegalStateException("access denied")

        assertNull(discovery.tableLocationIfExists("c", "default", "sales"))
        assertThrows(IllegalStateException::class.java) { discovery.tableLocationIfExists("c", "default", "orders") }
    }

    private fun listsOneTable(provider: String) {
        every { spark.sql("SHOW TABLES FROM `c`.`sales`") } returns dataset(listOf(RowFactory.create("sales", "events", false)))
        rows("DESCRIBE EXTENDED $TABLE", "Location" to "s3a://b/lake/sales.db/events", "Provider" to provider)
    }

    private fun rows(
        sql: String,
        vararg pairs: Pair<String, String?>,
    ) {
        every { spark.sql(sql) } returns dataset(pairs.map { (key, value) -> RowFactory.create(key, value) })
    }

    private fun dataset(rows: List<Row>): Dataset<Row> = mockk<Dataset<Row>>().also { every { it.collectAsList() } returns rows }
}
