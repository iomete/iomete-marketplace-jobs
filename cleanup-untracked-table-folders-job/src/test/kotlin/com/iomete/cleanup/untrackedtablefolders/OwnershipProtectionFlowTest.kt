package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.catalog.RestNamespaceRoot
import com.iomete.cleanup.untrackedtablefolders.catalog.sessionConfigOf
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
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
        // Once in the first read and once at the final check before deletion.
        verify(exactly = 2) { flow.catalogDiscoveryService.tableLocationIfExists(CATALOG, "default", "sales.db") }
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
        assertTrue("default_folders_of_other_databases" in notChecked(record))
        assertTrue("other_database_tables" in notChecked(record))
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

        val record = flow.auditRecords.single()
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertTrue(flow.objectExists("$SALES/t2/part-0.parquet"))
        assertEquals("unexpected_error", record.statusReason)
        assertTrue("final_recheck_before_deletion" in checks(record))
    }

    @Test
    fun `another database's table and write path inside the cleanup root survive while an unrelated orphan is deleted`() {
        folders(SALES, "t1", "orders", "orders_data", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"))
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returns
            listOf(otherTable("archive", "$SALES/orders", writeLocations = listOf("$SALES/orders_data")))

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        assertTrue(flow.objectExists("$SALES/orders/part-0.parquet"))
        assertTrue(flow.objectExists("$SALES/orders_data/part-0.parquet"))
        verify(exactly = 0) { flow.catalogDiscoveryService.discoverTables(CATALOG, "sales") }
    }

    @Test
    fun `another database's table stored at the cleanup root protects every folder in it`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "legacy" to "$WAREHOUSE/legacy.db"))
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "legacy") } returns listOf(otherTable("legacy", SALES))

        flow.run()

        assertTrue(flow.auditRecords.single().deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `an unresolved table in another database blocks deletion in both modes and says why`() {
        listOf(true, false).forEach { deleteEnabled ->
            InMemoryS3FileSystem.reset()
            folders(SALES, "t1", "orphan")
            val flow =
                flow(
                    tables = listOf(table("$SALES/t1")),
                    databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"),
                    deleteEnabled = deleteEnabled,
                )
            every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returns
                listOf(DiscoveredTable(CATALOG, "archive", "broken", false, location = null, unresolvedReason = "metadata could not be read"))

            flow.run()

            val record = flow.auditRecords.single()
            assertEquals("BLOCKED", record.status)
            assertEquals("other_table_unresolved", record.statusReason)
            assertEquals(listOf("$SALES/orphan"), record.candidateFolders)
            assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
            assertTrue("  Reason: other_table_unresolved" in flow.summaryLines())
        }
    }

    @Test
    fun `tables of another database that cannot be read block deletion`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"))
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } throws IllegalStateException("describe denied")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertTrue("default_folders_of_visible_other_databases" in checks(record))
        assertTrue("other_database_tables" in notChecked(record))
        assertTrue("same_bucket_session_catalogs" in notChecked(record))
    }

    @Test
    fun `other tables are not read when every folder already has an owner`() {
        folders(SALES, "t1")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        verify(exactly = 0) { flow.catalogDiscoveryService.discoverTables(any(), any()) }
        verify(exactly = 0) { flow.catalogDiscoveryService.sessionCatalogWarehouses() }
        assertEquals(
            listOf(
                "visible_tables_of_this_database",
                "write_paths_of_visible_tables_of_this_database",
                "default_folders_of_visible_other_databases",
                "configured_exclusions",
            ),
            checks(record),
        )
        assertEquals(
            listOf(
                "other_database_tables",
                "same_bucket_session_catalogs",
                "final_recheck_before_deletion",
                "other_bucket_session_catalogs",
                "catalogs_outside_session",
                "hidden_tables",
                "nested_namespace_tables",
                "files_outside_table_folders",
            ),
            notChecked(record),
        )
    }

    @Test
    fun `the first read is shared by the run and each deleting database reads other storage again before deleting`() {
        val marketing = "$WAREHOUSE/marketing.db"
        folders(SALES, "t1", "orphan")
        folders(marketing, "m1", "orphan")
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(catalog = CATALOG, databases = listOf("sales", "marketing"), dryRun = false, deleteEnabled = true),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns database(listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "marketing") } returns
            DiscoveredDatabase(CATALOG, "marketing", marketing, listOf(otherTable("marketing", "$marketing/m1")))
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns
            mapOf("sales" to SALES, "marketing" to marketing, "archive" to "$WAREHOUSE/archive.db")

        flow.run()

        assertEquals(listOf(listOf("$SALES/orphan"), listOf("$marketing/orphan")), flow.auditRecords.map { it.deletedFolders })
        flow.auditRecords.forEach { record ->
            assertEquals(
                listOf(
                    "visible_tables_of_this_database",
                    "write_paths_of_visible_tables_of_this_database",
                    "default_folders_of_visible_other_databases",
                    "configured_exclusions",
                    "visible_tables_of_other_databases",
                    "visible_storage_of_same_bucket_session_catalogs",
                    "final_recheck_before_deletion",
                ),
                checks(record),
            )
            assertEquals(
                listOf(
                    "other_bucket_session_catalogs",
                    "catalogs_outside_session",
                    "hidden_tables",
                    "nested_namespace_tables",
                    "files_outside_table_folders",
                ),
                notChecked(record),
            )
        }
        // One shared first read, then one final read for each of the two databases that delete.
        verify(exactly = 3) { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) }
        verify(exactly = 3) { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") }
        verify(exactly = 2) { flow.catalogDiscoveryService.discoverTables(CATALOG, "marketing") }
        verify(exactly = 2) { flow.catalogDiscoveryService.discoverTables(CATALOG, "sales") }
    }

    @Test
    fun `a table that another database places in a candidate folder before the final check keeps that folder`() {
        folders(SALES, "t1", "orders", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"))
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returnsMany
            listOf(emptyList(), listOf(otherTable("archive", "$SALES/orders")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$SALES/orders", "$SALES/orphan"), record.candidateFolders)
        assertEquals(listOf("$SALES/orphan"), record.deletedFolders)
        assertEquals("1", record.diagnosticDetails["folder_kept_at_final_check_count"])
        assertEquals("$SALES/orders", record.diagnosticDetails["folder_kept_at_final_check_paths_sample"])
        assertTrue(flow.objectExists("$SALES/orders/part-0.parquet"))
        val lines = flow.summaryLines()
        assertTrue("  Folders kept by the final check                  1" in lines)
        assertTrue("- $SALES/orders" in lines)
    }

    @Test
    fun `when the final check keeps every candidate nothing is deleted and the row counts them as kept`() {
        folders(SALES, "t1", "orders", "events")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"))
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returnsMany
            listOf(emptyList(), listOf(otherTable("archive", "$SALES/orders"), otherTable("archive", "$SALES/events")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(2, record.candidateFolderCount)
        assertEquals(0, record.deletedFolderCount)
        assertEquals(0, record.deletedObjectCount)
        assertTrue(record.deletedFolders.isEmpty())
        assertEquals("2", record.diagnosticDetails["folder_kept_at_final_check_count"])
        assertTrue(flow.objectExists("$SALES/orders/part-0.parquet"))
        assertTrue(flow.objectExists("$SALES/events/part-0.parquet"))
        val lines = flow.summaryLines()
        assertTrue("  Folders deleted                                  0" in lines)
        assertTrue("  Folders kept by the final check                  2" in lines)
        assertTrue("- $SALES/events" in lines && "- $SALES/orders" in lines)
    }

    @Test
    fun `a database created in the cleanup root before the final check keeps its folder`() {
        folders(SALES, "t1", "late", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returnsMany
            listOf(mapOf("sales" to SALES), mapOf("sales" to SALES, "late" to "$SALES/late"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$SALES/orphan"), record.deletedFolders)
        assertEquals("$SALES/late", record.diagnosticDetails["folder_kept_at_final_check_paths_sample"])
        assertTrue(flow.objectExists("$SALES/late/part-0.parquet"))
    }

    @Test
    fun `other storage that cannot be read at the final check blocks deletion`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"))
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returns emptyList() andThenThrows
            IllegalStateException("catalog unreachable")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        val lines = flow.summaryLines()
        assertTrue("  Reason: other_database_storage_unknown" in lines)
        assertTrue("visible_tables_of_other_databases" in checks(record))
        assertTrue("final_recheck_before_deletion" in notChecked(record))
        val ownership = lines.joinToString("\n").substringAfter("OWNERSHIP CHECKS").substringBefore("CATALOG")
        assertTrue(ownership.substringAfter("Not checked").contains("a final read of other storage right before deleting"))
        assertFalse(ownership.substringBefore("Not checked").contains("final read"))
    }

    @Test
    fun `a table of another database that becomes unresolved before the final check blocks deletion`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")), databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"))
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returnsMany
            listOf(
                emptyList(),
                listOf(DiscoveredTable(CATALOG, "archive", "broken", false, location = null, unresolvedReason = "metadata could not be read")),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_table_unresolved", record.statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `a dry run does not read other storage a second time`() {
        folders(SALES, "t1", "orphan")
        val flow =
            flow(
                tables = listOf(table("$SALES/t1")),
                databases = mapOf("sales" to SALES, "archive" to "$WAREHOUSE/archive.db"),
                deleteEnabled = false,
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$SALES/orphan"), record.candidateFolders)
        assertEquals(null, record.diagnosticDetails["folder_kept_at_final_check_count"])
        verify(exactly = 1) { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) }
        verify(exactly = 1) { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") }
        assertTrue("visible_tables_of_other_databases" in checks(record))
        assertTrue("final_recheck_before_deletion" in notChecked(record))
    }

    @Test
    fun `a failed first read is tried again by the next database instead of blocking it`() {
        val marketing = "$WAREHOUSE/marketing.db"
        folders(SALES, "t1", "orphan")
        folders(marketing, "m1", "orphan")
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(catalog = CATALOG, databases = listOf("sales", "marketing"), dryRun = false, deleteEnabled = true),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns database(listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "marketing") } returns
            DiscoveredDatabase(CATALOG, "marketing", marketing, listOf(otherTable("marketing", "$marketing/m1")))
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } throws IllegalStateException("brief outage") andThen
            mapOf("sales" to SALES, "marketing" to marketing)

        flow.run()

        assertEquals(listOf("BLOCKED", "SUCCESS"), flow.auditRecords.map { it.status })
        assertEquals("other_database_storage_unknown", flow.auditRecords.first().statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertEquals(listOf("$marketing/orphan"), flow.auditRecords.last().deletedFolders)
    }

    @Test
    fun `a database location that could not be read is read again by the next database instead of blocking it`() {
        val marketing = "$WAREHOUSE/marketing.db"
        val flow = twoDatabaseFlow(marketing)
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns
            mapOf("sales" to SALES, "marketing" to marketing, "archive" to null) andThen
            mapOf("sales" to SALES, "marketing" to marketing, "archive" to "$WAREHOUSE/archive.db")

        flow.run()

        assertEquals(listOf("BLOCKED", "SUCCESS"), flow.auditRecords.map { it.status })
        assertEquals("other_database_storage_unknown", flow.auditRecords.first().statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertEquals(listOf("$marketing/orphan"), flow.auditRecords.last().deletedFolders)
    }

    @Test
    fun `an unresolved table of another database is read again by the next database instead of blocking it`() {
        val marketing = "$WAREHOUSE/marketing.db"
        val flow = twoDatabaseFlow(marketing)
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns
            mapOf("sales" to SALES, "marketing" to marketing, "archive" to "$WAREHOUSE/archive.db")
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returns
            listOf(DiscoveredTable(CATALOG, "archive", "t9", false, location = null, unresolvedReason = "metadata could not be read")) andThen
            listOf(otherTable("archive", "$WAREHOUSE/archive.db/t9"))

        flow.run()

        assertEquals(listOf("BLOCKED", "SUCCESS"), flow.auditRecords.map { it.status })
        assertEquals("other_table_unresolved", flow.auditRecords.first().statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertEquals(listOf("$marketing/orphan"), flow.auditRecords.last().deletedFolders)
    }

    @Test
    fun `a table of another catalog in the same bucket survives, and a catalog in another bucket is not read`() {
        folders(SALES, "t1", "shared_t", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns
            mapOf(CATALOG to listOf(WAREHOUSE), "views" to listOf("s3://bucket/views"), "far" to listOf("s3a://far-bucket/lake"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("views") } returns mapOf("reports" to "s3://bucket/views/reports.db")
        every { flow.catalogDiscoveryService.discoverTables("views", "reports") } returns
            listOf(DiscoveredTable("views", "reports", "shared_t", false, location = "$SALES/shared_t"))

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        assertTrue(flow.objectExists("$SALES/shared_t/part-0.parquet"))
        verify(exactly = 0) { flow.catalogDiscoveryService.visibleDatabaseLocations("far") }
    }

    @Test
    fun `spark_catalog is read when only its Hive warehouse shares the cleanup root's bucket`() {
        folders(SALES, "t1", "hive_t", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns
            mapOf("spark_catalog" to listOf("s3a://iceberg-bucket/lake", "s3a://bucket/hive"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("spark_catalog") } returns mapOf("legacy" to "s3a://bucket/hive/legacy.db")
        every { flow.catalogDiscoveryService.discoverTables("spark_catalog", "legacy") } returns
            listOf(DiscoveredTable("spark_catalog", "legacy", "hive_t", false, location = "$SALES/hive_t"))

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        assertTrue(flow.objectExists("$SALES/hive_t/part-0.parquet"))
    }

    @Test
    fun `a database of the same name in another catalog stored at the cleanup root protects every folder in it`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("views" to listOf(WAREHOUSE))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("views") } returns mapOf("sales" to SALES)

        flow.run()

        assertTrue(flow.auditRecords.single().deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `another catalog's warehouse inside the cleanup root is protected whole`() {
        folders(SALES, "t1", "nested_lake", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("nested" to listOf("$SALES/nested_lake"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("nested") } returns emptyMap()

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        assertTrue(flow.objectExists("$SALES/nested_lake/part-0.parquet"))
    }

    @Test
    fun `a catalog in the same bucket whose databases cannot be listed blocks deletion`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("views" to listOf("s3a://bucket/views"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("views") } throws IllegalStateException("catalog unreachable")

        flow.run()

        assertEquals("other_database_storage_unknown", flow.auditRecords.single().statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `a REST catalog with no warehouse setting is read, and its table in the cleanup root survives`() {
        folders(SALES, "t1", "remote_t", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        sessionCatalogs(flow, restCatalog("remote", "https://catalog.example.com/api/catalog"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("remote") } returns mapOf("reports" to "s3a://bucket/remote/reports")
        every { flow.catalogDiscoveryService.discoverTables("remote", "reports") } returns
            listOf(DiscoveredTable("remote", "reports", "remote_t", false, location = "$SALES/remote_t"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$SALES/orphan"), record.deletedFolders)
        assertTrue(flow.objectExists("$SALES/remote_t/part-0.parquet"))
        assertTrue("visible_storage_of_same_bucket_session_catalogs" in checks(record))
        assertTrue("final_recheck_before_deletion" in checks(record))
    }

    @Test
    fun `a table that a catalog with no warehouse setting places in a candidate folder before the final check keeps that folder`() {
        folders(SALES, "t1", "orders", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        sessionCatalogs(flow, restCatalog("remote", "https://catalog.example.com/api/catalog"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("remote") } returns mapOf("reports" to "s3a://bucket/remote/reports")
        every { flow.catalogDiscoveryService.discoverTables("remote", "reports") } returnsMany
            listOf(emptyList(), listOf(DiscoveredTable("remote", "reports", "orders", false, location = "$SALES/orders")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$SALES/orders", "$SALES/orphan"), record.candidateFolders)
        assertEquals(listOf("$SALES/orphan"), record.deletedFolders)
        assertEquals("$SALES/orders", record.diagnosticDetails["folder_kept_at_final_check_paths_sample"])
        assertTrue(flow.objectExists("$SALES/orders/part-0.parquet"))
    }

    @Test
    fun `a managed REST catalog with a blank warehouse blocks deletion because its database folders are unknown`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        sessionCatalogs(flow, restCatalog("remote", "http://iom-core/api/internal/catalogs/remote", warehouse = ""))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("remote") } returns mapOf("reports" to "s3a://bucket/remote/reports")
        every { flow.catalogDiscoveryService.discoverTables("remote", "reports") } returns emptyList()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertFalse("visible_storage_of_same_bucket_session_catalogs" in checks(record))
        assertTrue("same_bucket_session_catalogs" in notChecked(record))
    }

    @Test
    fun `a catalog with no warehouse setting whose databases cannot be listed blocks deletion`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        sessionCatalogs(flow, restCatalog("remote", "https://catalog.example.com/api/catalog"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("remote") } throws IllegalStateException("catalog unreachable")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertTrue("same_bucket_session_catalogs" in notChecked(record))
    }

    @Test
    fun `a JDBC catalog is not read and does not stop deletion`() {
        folders(SALES, "t1", "orphan")
        val flow = flow(tables = listOf(table("$SALES/t1")))
        sessionCatalogs(flow, mapOf("spark.sql.catalog.jdbc_source" to "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"))

        flow.run()

        assertEquals(listOf("$SALES/orphan"), flow.auditRecords.single().deletedFolders)
        verify(exactly = 0) { flow.catalogDiscoveryService.visibleDatabaseLocations("jdbc_source") }
    }

    // Lists the session's catalogs with the real CatalogDiscoveryService over these settings, so the flow sees which
    // catalogs it keeps or leaves out. Spark's own warehouse is in another bucket, so spark_catalog is not read.
    private fun sessionCatalogs(
        flow: CleanupFlowHarness,
        settings: Map<String, String>,
    ) {
        val runtimeConfig = sessionConfigOf(settings)
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://hive-bucket/warehouse"
        val discovery =
            CatalogDiscoveryService().apply {
                sparkSessionProvider =
                    mockk<SparkSessionProvider>().also { provider ->
                        val sparkSession = mockk<SparkSession>()
                        every { sparkSession.conf() } returns runtimeConfig
                        every { provider.getOrCreate() } returns sparkSession
                    }
            }
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } answers { discovery.sessionCatalogWarehouses() }
        every { flow.catalogDiscoveryService.restNamespaceRoot(any(), any()) } answers { discovery.restNamespaceRoot(firstArg(), secondArg()) }
    }

    private fun restCatalog(
        name: String,
        uri: String,
        warehouse: String? = null,
    ): Map<String, String> =
        mapOf(
            "spark.sql.catalog.$name" to "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.$name.type" to "rest",
            "spark.sql.catalog.$name.uri" to uri,
        ) + listOfNotNull(warehouse?.let { "spark.sql.catalog.$name.warehouse" to it })

    private fun checks(record: CleanupAuditRecord): List<String> = record.diagnosticDetails.getValue("ownership_checks").split(",")

    private fun notChecked(record: CleanupAuditRecord): List<String> =
        record.diagnosticDetails.getValue("ownership_not_checked").split(",")

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

    private fun otherTable(
        database: String,
        location: String,
        writeLocations: List<String> = emptyList(),
    ): DiscoveredTable =
        DiscoveredTable(CATALOG, database, location.substringAfterLast('/'), isTemporary = false, location = location, writeLocations = writeLocations)

    private fun database(tables: List<DiscoveredTable>): DiscoveredDatabase = DiscoveredDatabase(CATALOG, "sales", SALES, tables)

    private fun twoDatabaseFlow(marketing: String): CleanupFlowHarness {
        folders(SALES, "t1", "orphan")
        folders(marketing, "m1", "orphan")
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(catalog = CATALOG, databases = listOf("sales", "marketing"), dryRun = false, deleteEnabled = true),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns database(listOf(table("$SALES/t1")))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "marketing") } returns
            DiscoveredDatabase(CATALOG, "marketing", marketing, listOf(otherTable("marketing", "$marketing/m1")))
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        return flow
    }

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
