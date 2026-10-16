package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.catalog.RestNamespaceRoot
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import io.mockk.verify
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val SALES = "s3a://bucket/lake/sales.db"

class OwnershipProtectionFlowTest {

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `another database inside the cleanup root survives while an unrelated orphan is deleted`() {
        folders(SALES, "t1", "orphan")
        folders("$SALES/x_data", "x_table")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "x" to "$SALES/x_data"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$SALES/orphan"), record.deletedFolders)
        assertTrue(flow.objectExists("$SALES/x_data/x_table/part-0.parquet"))
    }

    @Test
    fun `a folder that holds another database's location survives`() {
        folders(SALES, "t1", "orphan", "area")
        folders("$SALES/area/x_data", "x_table")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("x" to "$SALES/area/x_data"))

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        assertTrue(flow.objectExists("$SALES/area/x_data/x_table/part-0.parquet"))
    }

    @Test
    fun `a database that shares the cleanup root protects every folder in it`() {
        folders(SALES, "t1", "maybe_theirs")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("twin" to SALES))

        flow.run()

        assertTrue(flow.auditRecords.single().deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/maybe_theirs/part-0.parquet"))
    }

    @Test
    fun `a database whose location holds the cleanup root does not stop an unrelated orphan`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("default" to WAREHOUSE))

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        verify(exactly = 1) { flow.catalogDiscoveryService.tableLocationIfExists(CATALOG, "default", "sales.db") }
    }

    @Test
    fun `that database's table on the cleanup root's path protects the root`() {
        val restRoot = "$WAREHOUSE/sales"
        folders(restRoot, "t_rest", "dt=2024-01-01")
        val flow = flow(tables = listOf(table("$restRoot/t_rest")), databases = mapOf("default" to WAREHOUSE))
        every { flow.catalogDiscoveryService.tableLocationIfExists(CATALOG, "default", "sales") } returns restRoot

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(restRoot, record.storageScanLocation)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$restRoot/dt=2024-01-01/part-0.parquet"))
    }

    @Test
    fun `an unreadable database location blocks deletion and says why`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "hidden_location" to null))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertEquals(listOf("$SALES/orphan"), record.candidateFolders)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertTrue("  Reason: other_database_storage_unknown" in flow.summaryLines())
    }

    @Test
    fun `a failed database listing blocks deletion`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } throws IllegalStateException("listing denied")

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `a failed lookup of the table on the cleanup root's path blocks deletion`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("default" to WAREHOUSE))
        every { flow.catalogDiscoveryService.tableLocationIfExists(CATALOG, "default", "sales.db") } throws IllegalStateException("denied")

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `an Iceberg write path inside the cleanup root survives while an unrelated orphan is deleted`() {
        folders(SALES, "t1", "t1_data", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1", writeLocations = listOf("$SALES/t1_data"))))

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        assertTrue(flow.objectExists("$SALES/t1_data/part-0.parquet"))
    }

    @Test
    fun `a write path that appears before the final check still protects its folder`() {
        folders(SALES, "t1", "late_data", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returnsMany
            listOf(
                database(listOf(table("$SALES/t1"))),
                database(listOf(table("$SALES/t1", writeLocations = listOf("$SALES/late_data")))),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertTrue("$SALES/late_data" in record.candidateFolders)
        assertEquals(listOf("$SALES/orphan"), record.deletedFolders)
        assertTrue(flow.objectExists("$SALES/late_data/part-0.parquet"))
    }

    @Test
    fun `a dry run does not list protected folders as candidates`() {
        folders(SALES, "t1", "t1_data", "orphan")
        folders("$SALES/x_data", "x_table")
        val flow =
            flow(
                tables = listOf(table("$SALES/t1", writeLocations = listOf("$SALES/t1_data"))),
                databases = mapOf("x" to "$SALES/x_data"),
                deleteEnabled = false,
            )

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().candidateFolders)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `the summary does not count write paths or other databases' storage as table folders`() {
        folders(SALES, "t1", "t1_data", "orphan")
        folders("$SALES/x_data", "x_table")
        val flow =
            flow(
                tables = listOf(table("$SALES/t1", writeLocations = listOf("$SALES/t1_data"))),
                databases = mapOf("x" to "$SALES/x_data"),
                deleteEnabled = false,
            )

        flow.run()
        val lines = flow.summaryLines()

        assertTrue("  Folders belonging to verified catalog tables     1" in lines)
        assertTrue("  Folders protected or skipped for other reasons   2" in lines)
        assertTrue("  Folders with no verified catalog owner           1" in lines)
        assertTrue(" Mode: dry run, nothing is deleted" in lines)
        assertTrue("  Dry run. No folder was deleted." in lines)
    }

    @Test
    fun `another database's REST namespace folder at the cleanup root keeps its Iceberg tables`() {
        val marketing = "$WAREHOUSE/marketing"
        folders(marketing, "v2_orders", "campaigns-6f1c")
        val flow =
            flow(
                tables = listOf(table("$marketing/v2_orders")),
                databases = mapOf("sales" to marketing, "marketing" to "$WAREHOUSE/marketing.db"),
                databaseLocation = marketing,
            )
        every { flow.catalogDiscoveryService.restNamespaceRoot(CATALOG, "marketing") } returns RestNamespaceRoot(marketing, "derived")

        flow.run()

        assertTrue(flow.auditRecords.single().deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$marketing/campaigns-6f1c/part-0.parquet"))
    }

    @Test
    fun `a cleanup root inside another database's REST namespace folder blocks deletion`() {
        val area = "$WAREHOUSE/finance/sales_area"
        folders(area, "t1", "orphan")
        val flow =
            flow(
                tables = listOf(table("$area/t1")),
                databases = mapOf("sales" to area, "finance" to "$WAREHOUSE/finance.db"),
                databaseLocation = area,
            )
        every { flow.catalogDiscoveryService.restNamespaceRoot(CATALOG, "finance") } returns RestNamespaceRoot("$WAREHOUSE/finance", "derived")

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$area/orphan/part-0.parquet"))
    }

    @Test
    fun `an unqualified database location blocks deletion`() {
        folders(SALES, "t1", "orphan", "other_db")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "other" to "/lake/sales.db/other_db"))

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$SALES/other_db/part-0.parquet"))
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `an unqualified location for the table on the cleanup root's path blocks deletion`() {
        val restRoot = "$WAREHOUSE/sales"
        folders(restRoot, "t_rest", "dt=2024-01-01")
        val flow = flow(tables = listOf(table("$restRoot/t_rest")), databases = mapOf("default" to WAREHOUSE))
        every { flow.catalogDiscoveryService.tableLocationIfExists(CATALOG, "default", "sales") } returns "/lake/sales"

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$restRoot/dt=2024-01-01/part-0.parquet"))
    }

    @Test
    fun `a database location with a scheme but no bucket or host blocks deletion`() {
        folders(SALES, "t1", "orphan", "other_db")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "other" to "hdfs:/lake/sales.db/other_db"))

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `a looked-up table location with a scheme but no bucket or host blocks deletion`() {
        val restRoot = "$WAREHOUSE/sales"
        folders(restRoot, "t_rest", "dt=2024-01-01")
        val flow = flow(tables = listOf(table("$restRoot/t_rest")), databases = mapOf("default" to WAREHOUSE))
        every { flow.catalogDiscoveryService.tableLocationIfExists(CATALOG, "default", "sales") } returns "hdfs:/lake/sales"

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$restRoot/dt=2024-01-01/part-0.parquet"))
    }

    @Test
    fun `a namespace that differs only in letter case is another database when the catalog lists both`() {
        folders(SALES, "t1", "orphan", "x_data")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "Sales" to "$SALES/x_data"))

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        assertTrue(flow.objectExists("$SALES/x_data/part-0.parquet"))
    }

    @Test
    fun `a catalog that lowercases names still recognizes the configured database`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES), database = "Sales")

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
    }

    @Test
    fun `two case variants of the configured name with no exact match block deletion`() {
        folders(SALES, "t1", "orphan")
        val flow =
            flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "Sales" to "$WAREHOUSE/Sales.db"), database = "SALES")

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `a table that becomes unresolved before the final check stops deletion`() {
        folders(SALES, "t1", "t2", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returnsMany
            listOf(
                database(listOf(table("$SALES/t1"))),
                database(
                    listOf(
                        table("$SALES/t1"),
                        DiscoveredTable(CATALOG, "sales", "t2", false, location = null, unresolvedReason = "The table location has no scheme or bucket"),
                    ),
                ),
            )

        flow.run()

        assertTrue(flow.auditRecords.single().deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertTrue(flow.objectExists("$SALES/t2/part-0.parquet"))
    }

    private fun folders(
        root: String,
        vararg children: String,
    ) {
        putFolder(root)
        children.forEach { putFolder("$root/$it", "part-0.parquet") }
    }

    private fun table(
        location: String,
        writeLocations: List<String> = emptyList(),
    ): DiscoveredTable =
        DiscoveredTable(CATALOG, "sales", location.substringAfterLast('/'), isTemporary = false, location = location, writeLocations = writeLocations)

    private fun database(tables: List<DiscoveredTable>): DiscoveredDatabase = DiscoveredDatabase(CATALOG, "sales", SALES, tables)

    private fun flow(
        tables: List<DiscoveredTable>,
        databases: Map<String, String?> = mapOf("sales" to SALES),
        deleteEnabled: Boolean = true,
        databaseLocation: String = SALES,
        database: String = "sales",
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(catalog = CATALOG, databases = listOf(database), dryRun = !deleteEnabled, deleteEnabled = deleteEnabled),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, database) } returns DiscoveredDatabase(CATALOG, database, databaseLocation, tables)
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns databases
        return flow
    }
}
