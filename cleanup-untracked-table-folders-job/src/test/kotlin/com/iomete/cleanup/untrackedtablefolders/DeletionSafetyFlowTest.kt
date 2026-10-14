package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"

class DeletionSafetyFlowTest {

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
    fun `orphan folder is deleted and the live table survives`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/orders", "part-0.parquet")
        putFolder("s3a://bucket/lake/sales.db/orphan", "part-0.parquet")
        val flow = flow(deleteEnabled = true)
        flow.catalogReturns(database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales.db/orders"))

        flow.run()

        assertFalse(flow.objectExists("s3a://bucket/lake/sales.db/orphan/part-0.parquet"))
        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/orders/part-0.parquet"))
        assertEquals("SUCCESS", flow.auditRecords.single().status)
        assertEquals(listOf("s3a://bucket/lake/sales.db/orphan"), flow.auditRecords.single().deletedFolders)
    }

    @Test
    fun `unresolved table blocks deletion`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/orders", "part-0.parquet")
        putFolder("s3a://bucket/lake/sales.db/orphan", "part-0.parquet")
        val flow = flow(deleteEnabled = true)
        flow.catalogReturns(
            database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales.db/orders")
                .let { it.copy(tables = it.tables + unresolvedTable("sales", "broken")) },
        )

        flow.run()

        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/orphan/part-0.parquet"))
        assertEquals("BLOCKED", flow.auditRecords.single().status)
    }

    @Test
    fun `dry run reports the orphan and deletes nothing`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/orders", "part-0.parquet")
        putFolder("s3a://bucket/lake/sales.db/orphan", "part-0.parquet")
        val flow = flow(deleteEnabled = false)
        flow.catalogReturns(database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales.db/orders"))

        flow.run()

        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/orphan/part-0.parquet"))
        assertEquals(listOf("s3a://bucket/lake/sales.db/orphan"), flow.auditRecords.single().candidateFolders)
    }

    @Test
    fun `files of a table located at the database root survive`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/data", "part-0.parquet")
        putFolder("s3a://bucket/lake/sales.db/metadata", "v1.metadata.json")
        putFolder("s3a://bucket/lake/sales.db/orders", "part-0.parquet")
        val flow = flow(deleteEnabled = true)
        flow.catalogReturns(
            database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales.db/orders"),
        )

        flow.run()

        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/data/part-0.parquet"))
        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/metadata/v1.metadata.json"))
    }

    @Test
    fun `excluded path inside an orphan folder survives`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/orders", "part-0.parquet")
        putFolder("s3a://bucket/lake/sales.db/legacy", "junk.parquet")
        putFolder("s3a://bucket/lake/sales.db/legacy/keep_me", "important.parquet")
        val flow = flow(deleteEnabled = true, excludePaths = listOf("s3a://bucket/lake/sales.db/legacy/keep_me"))
        flow.catalogReturns(database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales.db/orders"))

        flow.run()

        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/legacy/keep_me/important.parquet"))
        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/legacy/junk.parquet"))
    }

    @Test
    fun `orphan folder inside an excluded path survives`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/orders", "part-0.parquet")
        putFolder("s3a://bucket/lake/sales.db/orphan", "part-0.parquet")
        val flow = flow(deleteEnabled = true, excludePaths = listOf("s3a://bucket/lake/sales.db"))
        flow.catalogReturns(database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales.db/orders"))

        flow.run()

        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/orphan/part-0.parquet"))
    }

    @Test
    fun `database located at the warehouse root never exposes other databases`() {
        putFolder("s3a://bucket/lake")
        putFolder("s3a://bucket/lake/events", "part-0.parquet")
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/orders", "part-0.parquet")
        val flow = flow(deleteEnabled = true, database = "default")
        flow.catalogReturns(database("default", "s3a://bucket/lake", "s3a://bucket/lake/events"))

        flow.run()

        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/orders/part-0.parquet"))
        assertEquals("SKIPPED", flow.auditRecords.single().status)
    }

    @Test
    fun `bucket root database location is never a cleanup boundary`() {
        putFolder("s3a://bucket/team_x")
        putFolder("s3a://bucket/team_x/orders", "part-0.parquet")
        putFolder("s3a://bucket/team_x/other_team_data", "part-0.parquet")
        val flow = flow(deleteEnabled = true, database = "root_db")
        flow.catalogReturns(database("root_db", "s3a://bucket", "s3a://bucket/team_x/orders"))

        flow.run()

        assertTrue(flow.objectExists("s3a://bucket/team_x/other_team_data/part-0.parquet"))
        assertEquals("SKIPPED", flow.auditRecords.single().status)
    }

    @Test
    fun `table stored at a bucket root is handled without a URI failure`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/orphan", "part-0.parquet")
        val flow = flow(deleteEnabled = true)
        flow.catalogReturns(database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("cleanup_root_has_no_visible_table", record.statusReason)
        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/orphan/part-0.parquet"))
    }

    @Test
    fun `a fallback cleanup root with no visible table blocks deletion and the summary says why`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/hidden_hive_table", "part-0.parquet")
        putFolder("s3a://bucket/lake/sales/t_rest", "part-0.parquet")
        putFolder("s3a://bucket/custom/x/t_custom", "part-0.parquet")
        val flow = flow(deleteEnabled = true)
        flow.catalogReturns(
            database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales/t_rest", "s3a://bucket/custom/x/t_custom"),
        )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("s3a://bucket/lake/sales.db", record.storageScanLocation)
        assertEquals("BLOCKED", record.status)
        assertEquals("cleanup_root_has_no_visible_table", record.statusReason)
        assertEquals(listOf("s3a://bucket/lake/sales.db/hidden_hive_table"), record.candidateFolders)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/hidden_hive_table/part-0.parquet"))
        val summary = flow.summaryLines()
        assertTrue(summary.none { "eligible for cleanup" in it })
        assertTrue("  Reason: cleanup_root_has_no_visible_table" in summary)
        assertTrue("  No folder in this database was deleted." in summary)
    }

    @Test
    fun `a refused recursive delete stops further deletion`() {
        putFolder("s3a://bucket/lake/sales.db")
        putFolder("s3a://bucket/lake/sales.db/orders", "part-0.parquet")
        listOf("orphan_a", "orphan_b", "orphan_c").forEach {
            putFolder("s3a://bucket/lake/sales.db/$it", "part-0.parquet")
        }
        InMemoryS3FileSystem.refuseDeleteFor = "s3a://bucket/lake/sales.db/orphan_b"
        val flow = flow(deleteEnabled = true)
        flow.catalogReturns(database("sales", "s3a://bucket/lake/sales.db", "s3a://bucket/lake/sales.db/orders"))

        flow.run()

        assertTrue(flow.objectExists("s3a://bucket/lake/sales.db/orphan_c/part-0.parquet"))
        assertEquals("FAILED", flow.auditRecords.single().status)
    }

    private fun flow(
        deleteEnabled: Boolean,
        database: String = "sales",
        excludePaths: List<String> = emptyList(),
    ): CleanupFlowHarness =
        CleanupFlowHarness(
            ApplicationConfig(
                catalog = CATALOG,
                databases = listOf(database),
                excludePaths = excludePaths,
                dryRun = !deleteEnabled,
                deleteEnabled = deleteEnabled,
            ),
        )

    private fun CleanupFlowHarness.catalogReturns(database: DiscoveredDatabase) {
        every { catalogDiscoveryService.discoverDatabase(CATALOG, database.database) } returns database
        every { catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
    }

    private fun database(
        name: String,
        location: String,
        vararg tableLocations: String,
    ): DiscoveredDatabase =
        DiscoveredDatabase(
            catalog = CATALOG,
            database = name,
            location = location,
            tables =
                tableLocations.mapIndexed { index, tableLocation ->
                    DiscoveredTable(CATALOG, name, "table_$index", isTemporary = false, location = tableLocation)
                },
        )

    private fun unresolvedTable(
        database: String,
        table: String,
    ): DiscoveredTable =
        DiscoveredTable(CATALOG, database, table, isTemporary = false, location = null, unresolvedReason = "metadata missing")
}
