package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.candidate.StorageFolderReconciliation
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateDeletionGate
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDeletionService
import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
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
        rows("SHOW TBLPROPERTIES $TABLE", "write.data.path" to "s3a://b/lake/sales.db/events_data", "format" to "iceberg/parquet")

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
    fun `a table with a missing or blank location is unresolved`() {
        listsTables("events", "orders")
        describes(TABLE, details("Type" to "MANAGED", "Provider" to "hive"))
        describes("`c`.`sales`.`orders`", details("Type" to "EXTERNAL", "Location" to " ", "Provider" to "iceberg"))

        assertEquals(listOf(true, true), discovery.discoverTables("c", "sales").map { it.isUnresolved })
    }

    @Test
    fun `a table without a location blocks deletion at discovery and at the final check`() {
        rows("DESCRIBE DATABASE EXTENDED `c`.`sales`", "Location" to "s3a://b/lake/sales.db")
        listsTables("events")
        describes(TABLE, details("Type" to "MANAGED", "Provider" to "hive"))
        val deletion = mockk<ObjectStorageDeletionService>()
        val gate =
            CandidateDeletionGate().apply {
                config = ApplicationConfig(catalog = "c", databases = listOf("sales"), dryRun = false, deleteEnabled = true)
                catalogDiscoveryService = discovery
                objectStorageDeletionService = deletion
            }

        assertEquals(1, discovery.discoverDatabase("c", "sales").unresolvedTables.size)
        assertThrows(IllegalStateException::class.java) {
            gate.deleteCandidates("c", "sales", StorageFolderReconciliation.DeletionEligible(listOf(StorageFolder(LOCATION))), emptyList())
        }
        verify(exactly = 0) { deletion.deleteFoldersRecursively(any(), any()) }
    }

    @Test
    fun `Iceberg settings that are blank or not true are unresolved`() {
        listsTables("events", "orders", "refunds")
        listOf("events", "orders", "refunds").forEach { name ->
            describes("`c`.`sales`.`$name`", details("Location" to "s3a://b/lake/sales.db/$name", "Provider" to "iceberg"))
        }
        rows("SHOW TBLPROPERTIES $TABLE", "gc.enabled" to "")
        rows("SHOW TBLPROPERTIES `c`.`sales`.`orders`", "write.metadata.path" to " ")
        rows("SHOW TBLPROPERTIES `c`.`sales`.`refunds`", "write.location-provider.impl" to "")

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
    fun `a table that fails a check after its location was read keeps it as known, never as verified`() {
        listsTables("gc_off", "custom_provider", "bad_write_path", "unreadable")
        val names = listOf("gc_off", "custom_provider", "bad_write_path", "unreadable")
        names.forEach { name ->
            describes("`c`.`sales`.`$name`", details("Location" to "s3a://b/lake/sales.db/$name", "Provider" to "iceberg"))
        }
        rows("SHOW TBLPROPERTIES `c`.`sales`.`gc_off`", "gc.enabled" to "false")
        rows("SHOW TBLPROPERTIES `c`.`sales`.`custom_provider`", "write.location-provider.impl" to "com.example.Provider")
        rows("SHOW TBLPROPERTIES `c`.`sales`.`bad_write_path`", "write.data.path" to "/lake/sales.db/data")
        every { spark.sql("SHOW TBLPROPERTIES `c`.`sales`.`unreadable`") } throws IllegalStateException("denied")

        val tables = discovery.discoverTables("c", "sales")

        assertEquals(names.map { "s3a://b/lake/sales.db/$it" }, tables.map { it.knownLocation })
        assertEquals(listOf(null, null, null, null), tables.map { it.location })
        assertEquals(listOf(true, true, true, true), tables.map { it.isUnresolved })
    }

    @Test
    fun `a location that was never read or cannot be matched is not known`() {
        listsTables("no_location", "unqualified", "no_details", "missing_file", "missing_other_file")
        describes("`c`.`sales`.`no_location`", details("Type" to "MANAGED", "Provider" to "iceberg"))
        describes("`c`.`sales`.`unqualified`", details("Location" to "/lake/sales.db/unqualified", "Provider" to "iceberg"))
        describes("`c`.`sales`.`no_details`", listOf(column("id", "bigint")))
        every { spark.sql("DESCRIBE EXTENDED `c`.`sales`.`missing_file`") } throws
            java.io.FileNotFoundException("No such file or directory: s3a://b/lake/sales.db/missing_file/metadata/1.metadata.json")
        every { spark.sql("DESCRIBE EXTENDED `c`.`sales`.`missing_other_file`") } throws
            IllegalStateException("Location does not exist: s3a://b/lake/sales.db/missing_other_file/metadata/snap-1.avro")

        val tables = discovery.discoverTables("c", "sales")

        assertEquals(listOf(true, true, true, true, true), tables.map { it.isUnresolved })
        assertEquals(listOf(null, null, null, null, null), tables.map { it.knownLocation })
    }

    @Test
    fun `a table whose metadata file is missing keeps that file's table folder as known, never as verified`() {
        listsTables("missing_metadata")
        val message =
            "Server error: NotFoundException: Location does not exist: s3a://b/lake/sales/missing_metadata-abc/metadata/00003-x.metadata.json"
        every { spark.sql("DESCRIBE EXTENDED `c`.`sales`.`missing_metadata`") } throws IllegalStateException(message)

        val table = discovery.discoverTables("c", "sales").single()

        assertEquals(true, table.isUnresolved)
        assertNull(table.location)
        assertEquals("s3a://b/lake/sales/missing_metadata-abc", table.knownLocation)
        assertEquals(message, table.unresolvedReason)
    }

    @Test
    fun `a resolved table or a view cannot carry a known location`() {
        assertThrows(IllegalArgumentException::class.java) {
            DiscoveredTable("c", "sales", "events", isTemporary = false, location = LOCATION, knownLocation = LOCATION)
        }
        assertThrows(IllegalArgumentException::class.java) {
            DiscoveredTable("c", "sales", "events", isTemporary = false, location = null, knownLocation = LOCATION)
        }
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

    @Test
    fun `a table lookup fails for a table without a location but not for a view`() {
        describes("`c`.`default`.`sales`", details("Type" to "MANAGED", "Provider" to "hive"))
        describes("`c`.`default`.`orders`", details("Type" to "VIEW", "View Text" to "SELECT 1"))

        assertThrows(IllegalStateException::class.java) { discovery.tableLocationIfExists("c", "default", "sales") }
        assertNull(discovery.tableLocationIfExists("c", "default", "orders"))
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
