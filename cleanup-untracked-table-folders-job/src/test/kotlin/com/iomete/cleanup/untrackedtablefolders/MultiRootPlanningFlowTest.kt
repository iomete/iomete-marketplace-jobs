package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val DATABASE_LOCATION = "s3a://bucket/lake/sales.db"
private const val REST_ROOT = "s3a://bucket/lake/sales"

class MultiRootPlanningFlowTest {

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
    fun `orphans in every accepted root are reported but only the cleanup root deletes`() {
        folders(DATABASE_LOCATION, "t_hive", "orphan_hive")
        folders(REST_ROOT, "t_rest", "orphan_rest")
        folders("s3a://bucket/custom/x", "t_custom", "orphan_custom")
        val flow =
            flow(
                tables = listOf("$DATABASE_LOCATION/t_hive", "$REST_ROOT/t_rest", "s3a://bucket/custom/x/t_custom"),
                restNamespaceRoot = REST_ROOT,
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals("partial_scan_coverage", record.statusReason)
        assertEquals(listOf("$DATABASE_LOCATION/orphan_hive"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_ROOT/orphan_rest/part-0.parquet"))
        assertTrue(flow.objectExists("s3a://bucket/custom/x/orphan_custom/part-0.parquet"))
        assertEquals(listOf("$REST_ROOT/orphan_rest"), reportOnlyFolders(record))
        assertTrue("s3a://bucket/custom/x=not_scanned:outside_database_boundary:visible_tables=1" in rootLines(record))
        assertEquals("partial", record.diagnosticDetails["scan_plan_coverage"])
        assertEquals("s3a://bucket/custom/x/t_custom", record.diagnosticDetails["scan_plan_uncovered_table_locations_sample"])
        assertNull(InMemoryS3FileSystem.listStatusCalls["s3a://bucket/custom/x"])
    }

    @Test
    fun `report-only folders are never walked for age or size`() {
        folders(DATABASE_LOCATION, "t_hive", "orphan_hive")
        folders(REST_ROOT, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DATABASE_LOCATION/t_hive", "$REST_ROOT/t_rest"), restNamespaceRoot = REST_ROOT)

        flow.run()

        assertEquals(1, InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/orphan_hive"])
        assertNull(InMemoryS3FileSystem.listStatusCalls["$REST_ROOT/orphan_rest"])
    }

    @Test
    fun `nine parents across two buckets name every root that stays unsupported`() {
        folders(DATABASE_LOCATION, "t1", "orphan", "archive", "landing")
        folders("$DATABASE_LOCATION/archive", "t4")
        folders("$DATABASE_LOCATION/landing", "t5")
        folders(REST_ROOT, "t2", "t3")
        val outside =
            listOf("s3a://bucket/custom/a", "s3a://bucket/custom/b", "s3a://bucket2/ext/a", "s3a://bucket2/ext/b", "s3a://bucket2/ext/c")
        outside.forEachIndexed { index, parent -> folders(parent, "t${index + 6}", "orphan") }
        val flow =
            flow(
                tables =
                    listOf("$DATABASE_LOCATION/t1", "$REST_ROOT/t2", "$REST_ROOT/t3", "$DATABASE_LOCATION/archive/t4", "$DATABASE_LOCATION/landing/t5") +
                        outside.mapIndexed { index, parent -> "$parent/t${index + 6}" },
                restNamespaceRoot = REST_ROOT,
            )

        flow.run()

        val record = flow.auditRecords.single()
        val lines = rootLines(record)
        assertEquals("9", record.diagnosticDetails["scan_root_count"])
        assertTrue("$DATABASE_LOCATION=cleanup:scanned:visible_tables=3" in lines)
        assertTrue("$REST_ROOT=report_only:scanned:visible_tables=2" in lines)
        assertTrue("$DATABASE_LOCATION/archive=report_only:scanned:visible_tables=1" in lines)
        outside.forEach { assertTrue("$it=not_scanned:outside_database_boundary:visible_tables=1" in lines) }
        assertEquals("5", record.diagnosticDetails["scan_plan_uncovered_table_location_count"])
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        outside.forEach {
            assertTrue(flow.objectExists("$it/orphan/part-0.parquet"))
            assertNull(InMemoryS3FileSystem.listStatusCalls[it])
        }
    }

    @Test
    fun `a root whose tables are all hidden from the job is reported and never deleted`() {
        folders(REST_ROOT, "t_rest", "orphan_rest")
        folders(DATABASE_LOCATION, "hidden_hive_table")
        val flow = flow(tables = listOf("$REST_ROOT/t_rest"), restNamespaceRoot = REST_ROOT)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(REST_ROOT, record.storageScanLocation)
        assertFalse(flow.objectExists("$REST_ROOT/orphan_rest/part-0.parquet"))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/hidden_hive_table/part-0.parquet"))
        assertTrue("$DATABASE_LOCATION=report_only:scanned:visible_tables=0" in rootLines(record))
        assertEquals(listOf("$DATABASE_LOCATION/hidden_hive_table"), reportOnlyFolders(record))
    }

    @Test
    fun `a derived REST root with no visible table is reported and never deleted`() {
        folders(DATABASE_LOCATION, "t_hive", "orphan_hive")
        folders(REST_ROOT, "hidden_iceberg_table")
        val flow = flow(tables = listOf("$DATABASE_LOCATION/t_hive"), restNamespaceRoot = REST_ROOT)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertNull(record.statusReason)
        assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_ROOT/hidden_iceberg_table/part-0.parquet"))
        assertTrue("$REST_ROOT=report_only:scanned:visible_tables=0" in rootLines(record))
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
    }

    @Test
    fun `a fallback cleanup root with no visible table blocks deletion`() {
        folders(DATABASE_LOCATION, "hidden_hive_table")
        folders(REST_ROOT, "t_rest")
        folders("s3a://bucket/custom/x", "t_custom")
        val flow = flow(tables = listOf("$REST_ROOT/t_rest", "s3a://bucket/custom/x/t_custom"), restNamespaceRoot = REST_ROOT)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(DATABASE_LOCATION, record.storageScanLocation)
        assertEquals("BLOCKED", record.status)
        assertEquals("cleanup_root_has_no_visible_table", record.statusReason)
        assertEquals(listOf("$DATABASE_LOCATION/hidden_hive_table"), record.candidateFolders)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DATABASE_LOCATION/hidden_hive_table/part-0.parquet"))
    }

    @Test
    fun `a REST root in another bucket is scanned for reporting only`() {
        val databaseLocation = "s3a://bucket2/dbs/sales.db"
        folders(databaseLocation, "t_hive", "orphan_hive")
        folders(REST_ROOT, "t_rest", "orphan_rest")
        val flow =
            flow(
                tables = listOf("$databaseLocation/t_hive", "$REST_ROOT/t_rest"),
                restNamespaceRoot = REST_ROOT,
                databaseLocation = databaseLocation,
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$databaseLocation/orphan_hive"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_ROOT/orphan_rest/part-0.parquet"))
        assertEquals(listOf("$REST_ROOT/orphan_rest"), reportOnlyFolders(record))
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
    }

    @Test
    fun `a missing REST root is reported as absent`() {
        folders(DATABASE_LOCATION, "t_hive", "orphan_hive")
        val flow = flow(tables = listOf("$DATABASE_LOCATION/t_hive"), restNamespaceRoot = REST_ROOT)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertTrue("$REST_ROOT=report_only:absent:visible_tables=0" in rootLines(record))
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
    }

    @Test
    fun `a failed listing of a report-only root does not change deletion and marks coverage partial`() {
        folders(DATABASE_LOCATION, "t_hive", "orphan_hive")
        folders(REST_ROOT, "t_rest", "orphan_rest")
        InMemoryS3FileSystem.failListFor = REST_ROOT
        val flow = flow(tables = listOf("$DATABASE_LOCATION/t_hive", "$REST_ROOT/t_rest"), restNamespaceRoot = REST_ROOT)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DATABASE_LOCATION/orphan_hive"), record.deletedFolders)
        assertTrue("$REST_ROOT=report_only:listing_failed:visible_tables=1" in rootLines(record))
        assertEquals("partial", record.diagnosticDetails["scan_plan_coverage"])
    }

    @Test
    fun `a folder is reported by one root only when roots are nested`() {
        folders(DATABASE_LOCATION, "archive", "orphan_top")
        folders("$DATABASE_LOCATION/archive", "t1", "orphan_nested")
        val flow = flow(tables = listOf("$DATABASE_LOCATION/archive/t1"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("$DATABASE_LOCATION/archive", record.storageScanLocation)
        assertEquals(listOf("$DATABASE_LOCATION/archive/orphan_nested"), record.deletedFolders)
        assertEquals(listOf("$DATABASE_LOCATION/orphan_top"), reportOnlyFolders(record))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_top/part-0.parquet"))
    }

    @Test
    fun `exclude_database_folders applies under every root`() {
        folders(DATABASE_LOCATION, "t_hive", "keep")
        folders(REST_ROOT, "t_rest", "keep", "orphan_rest")
        val flow =
            flow(
                tables = listOf("$DATABASE_LOCATION/t_hive", "$REST_ROOT/t_rest"),
                restNamespaceRoot = REST_ROOT,
                excludeDatabaseFolders = listOf("sales.keep"),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertTrue(flow.objectExists("$DATABASE_LOCATION/keep/part-0.parquet"))
        assertEquals(listOf("$REST_ROOT/orphan_rest"), reportOnlyFolders(record))
    }

    @Test
    fun `a single-root database keeps its existing audit row`() {
        folders(DATABASE_LOCATION, "t1", "orphan")
        val flow = flow(tables = listOf("$DATABASE_LOCATION/t1"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertNull(record.statusReason)
        assertEquals(DATABASE_LOCATION, record.storageScanLocation)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        assertEquals("$DATABASE_LOCATION/orphan", record.diagnosticDetails["candidate_folder_paths_sample"])
        assertNull(record.diagnosticDetails["unscanned_table_location_count"])
        assertEquals("1", record.diagnosticDetails["scan_root_count"])
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
        assertEquals("0", record.diagnosticDetails["report_only_folder_count"])
        assertEquals("visible_catalog_tables", record.diagnosticDetails["ownership_basis"])
    }

    @Test
    fun `an approved custom root in another bucket is listed for review and never deleted or walked`() {
        folders(DATABASE_LOCATION, "t1", "orphan")
        folders("s3a://bucket2/ext/a", "t2", "orphan_ext")
        val flow =
            flow(tables = listOf("$DATABASE_LOCATION/t1", "s3a://bucket2/ext/a/t2"), approvedRoots = listOf("s3a://bucket2/ext/a"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.candidateFolders)
        assertTrue("s3a://bucket2/ext/a=report_only_approved:scanned:visible_tables=1" in rootLines(record))
        assertEquals(listOf("s3a://bucket2/ext/a/orphan_ext"), reportOnlyFolders(record))
        assertTrue(flow.objectExists("s3a://bucket2/ext/a/orphan_ext/part-0.parquet"))
        assertNull(InMemoryS3FileSystem.listStatusCalls["s3a://bucket2/ext/a/orphan_ext"])
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
    }

    @Test
    fun `an approved root with no visible table is not listed`() {
        folders(DATABASE_LOCATION, "t1")
        folders("s3a://bucket2/ext/hidden", "hidden_table")
        val flow = flow(tables = listOf("$DATABASE_LOCATION/t1"), approvedRoots = listOf("s3a://bucket2/ext/hidden"))

        flow.run()

        val record = flow.auditRecords.single()
        assertTrue("s3a://bucket2/ext/hidden=not_scanned:no_visible_table:visible_tables=0" in rootLines(record))
        assertNull(InMemoryS3FileSystem.listStatusCalls["s3a://bucket2/ext/hidden"])
    }

    @Test
    fun `a hidden table inside an approved root is reported but not protected from review`() {
        folders(DATABASE_LOCATION, "t1")
        folders("s3a://bucket2/ext/a", "t2", "hidden_table")
        val flow = flow(tables = listOf("$DATABASE_LOCATION/t1", "s3a://bucket2/ext/a/t2"), approvedRoots = listOf("s3a://bucket2/ext/a"))

        flow.run()

        assertEquals(listOf("s3a://bucket2/ext/a/hidden_table"), reportOnlyFolders(flow.auditRecords.single()))
        assertTrue(flow.objectExists("s3a://bucket2/ext/a/hidden_table/part-0.parquet"))
    }

    @Test
    fun `invalid approvals are refused with a reason and never listed`() {
        folders(DATABASE_LOCATION, "t1")
        val flow =
            flow(
                tables = listOf("$DATABASE_LOCATION/t1", "s3a://bucket2/t2"),
                approvedRoots = listOf("s3a://bucket2", WAREHOUSE, "s3a://bucket/lake/sales.db/t1/inner"),
            )

        flow.run()

        val lines = rootLines(flow.auditRecords.single())
        assertTrue("s3a://bucket2=not_scanned:bucket_root:visible_tables=1" in lines)
        assertTrue("$WAREHOUSE=not_scanned:contains_warehouse_root:visible_tables=1" in lines)
        assertTrue("$DATABASE_LOCATION/t1/inner=not_scanned:inside_table_location:visible_tables=0" in lines)
        assertNull(InMemoryS3FileSystem.listStatusCalls["s3a://bucket2"])
        assertNull(InMemoryS3FileSystem.listStatusCalls[WAREHOUSE])
    }

    @Test
    fun `a database refused by the cleanup-root checks keeps its SKIPPED row and gets report-only roots`() {
        folders(DATABASE_LOCATION, "hidden_hive_table")
        folders("s3a://bucket2/ext/a", "t1", "orphan_ext")
        val flow = flow(tables = listOf("s3a://bucket2/ext/a/t1"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("scan_root_outside_database_boundary", record.statusReason)
        assertEquals("", record.storageScanLocation)
        assertEquals(0, record.candidateFolderCount)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue("$DATABASE_LOCATION=report_only:scanned:visible_tables=0" in rootLines(record))
        assertTrue("s3a://bucket2/ext/a=not_scanned:outside_database_boundary:visible_tables=1" in rootLines(record))
        assertEquals(listOf("$DATABASE_LOCATION/hidden_hive_table"), reportOnlyFolders(record))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/hidden_hive_table/part-0.parquet"))
    }

    @Test
    fun `an approval lets a refused database report its custom root without deleting`() {
        folders(DATABASE_LOCATION)
        folders("s3a://bucket2/ext/a", "t1", "orphan_ext")
        val flow = flow(tables = listOf("s3a://bucket2/ext/a/t1"), approvedRoots = listOf("s3a://bucket2/ext/a"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertTrue("s3a://bucket2/ext/a=report_only_approved:scanned:visible_tables=1" in rootLines(record))
        assertEquals(listOf("s3a://bucket2/ext/a/orphan_ext"), reportOnlyFolders(record))
        assertTrue(flow.objectExists("s3a://bucket2/ext/a/orphan_ext/part-0.parquet"))
    }

    @Test
    fun `a bucket-root database location is never listed`() {
        folders("s3a://bucket3/sales_data", "t1", "orphan")
        val flow = flow(tables = listOf("s3a://bucket3/sales_data/t1"), databaseLocation = "s3a://bucket3")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("database_location_is_bucket_root", record.statusReason)
        assertTrue("s3a://bucket3/sales_data=not_scanned:outside_database_boundary:visible_tables=1" in rootLines(record))
        assertNull(InMemoryS3FileSystem.listStatusCalls["s3a://bucket3"])
        assertNull(InMemoryS3FileSystem.listStatusCalls["s3a://bucket3/sales_data"])
    }

    @Test
    fun `a refused database with an unknown warehouse gets no report-only roots`() {
        folders(DATABASE_LOCATION, "t1")
        val flow = flow(tables = listOf("$DATABASE_LOCATION/t1"), warehouseRoots = emptyList())

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("catalog_warehouse_unknown", record.statusReason)
        assertNull(record.diagnosticDetails["scan_root_count"])
    }

    @Test
    fun `a planning error keeps a refused database's SKIPPED row unchanged`() {
        folders(DATABASE_LOCATION, "hidden")
        val flow = flow(tables = listOf("s3a://bucket2/ext/a/t1"))
        every { flow.catalogDiscoveryService.knownWarehouseRoots() } throws IllegalStateException("conf unavailable")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("scan_root_outside_database_boundary", record.statusReason)
        assertNull(record.diagnosticDetails["scan_root_count"])
    }

    private fun folders(
        root: String,
        vararg children: String,
    ) {
        putFolder(root)
        children.forEach { putFolder("$root/$it", "part-0.parquet") }
    }

    private fun rootLines(record: CleanupAuditRecord): List<String> =
        record.diagnosticDetails["scan_roots_sample"].orEmpty().lines()

    private fun reportOnlyFolders(record: CleanupAuditRecord): List<String> =
        record.diagnosticDetails["report_only_folders_sample"]?.lines().orEmpty()

    private fun flow(
        tables: List<String>,
        restNamespaceRoot: String? = null,
        databaseLocation: String = DATABASE_LOCATION,
        excludeDatabaseFolders: List<String> = emptyList(),
        approvedRoots: List<String> = emptyList(),
        warehouseRoots: List<String> = listOf(WAREHOUSE),
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(
                    catalog = CATALOG,
                    databases = listOf("sales"),
                    excludeDatabaseFolders = excludeDatabaseFolders,
                    dryRun = false,
                    deleteEnabled = true,
                    reportOnlyStorageRoots = if (approvedRoots.isEmpty()) emptyMap() else mapOf("sales" to approvedRoots),
                ),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns
            DiscoveredDatabase(
                catalog = CATALOG,
                database = "sales",
                location = databaseLocation,
                tables =
                    tables.mapIndexed { index, location ->
                        DiscoveredTable(CATALOG, "sales", "table_$index", isTemporary = false, location = location)
                    },
            )
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns warehouseRoots
        every { flow.catalogDiscoveryService.restNamespaceRoot(CATALOG, "sales") } returns restNamespaceRoot
        return flow
    }
}
