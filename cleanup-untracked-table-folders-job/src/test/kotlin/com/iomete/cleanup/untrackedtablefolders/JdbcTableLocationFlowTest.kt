package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.catalog.RestNamespaceRoot
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val DATABASE_LOCATION = "s3a://bucket/lake/sales.db"
private const val REST_ROOT = "s3a://bucket/lake/sales"
private const val JDBC = "org.apache.spark.sql.jdbc"

class JdbcTableLocationFlowTest {

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
    fun `a JDBC table's location in the db folder does not choose the scan root or leave coverage partial`() {
        folders(DATABASE_LOCATION)
        folders(REST_ROOT, "orders", "customers", "orphan")
        val flow =
            flow(
                table("$REST_ROOT/orders", "iceberg"),
                table("$REST_ROOT/customers", "iceberg"),
                table("$DATABASE_LOCATION/crm_accounts", JDBC),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(REST_ROOT, record.storageScanLocation)
        assertEquals("SUCCESS", record.status)
        assertNull(record.statusReason)
        assertEquals(listOf("$REST_ROOT/orphan"), record.deletedFolders)
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
        assertTrue("$DATABASE_LOCATION/crm_accounts" in record.diagnosticDetails["active_table_locations_sample"].orEmpty())
    }

    @Test
    fun `a JDBC table's location in the db folder does not pull the scan root away when the REST root is not derived`() {
        folders(DATABASE_LOCATION)
        folders(REST_ROOT, "orders", "customers", "orphan")
        val flow =
            flow(
                table("$REST_ROOT/orders", "iceberg"),
                table("$REST_ROOT/customers", "iceberg"),
                table("$DATABASE_LOCATION/crm_accounts", JDBC),
                restNamespaceRoot = RestNamespaceRoot(location = null, status = "unrecognized_uri"),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(REST_ROOT, record.storageScanLocation)
        assertEquals("SUCCESS", record.status)
        assertNull(record.statusReason)
        assertEquals(listOf("$REST_ROOT/orphan"), record.deletedFolders)
    }

    @Test
    fun `a folder at a JDBC table's location is never a candidate`() {
        folders(DATABASE_LOCATION, "orders", "crm_accounts", "orphan")
        val flow = flow(table("$DATABASE_LOCATION/orders", "iceberg"), table("$DATABASE_LOCATION/crm_accounts", JDBC))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.candidateFolders)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        assertTrue(flow.objectExists("$DATABASE_LOCATION/crm_accounts/part-0.parquet"))
    }

    @Test
    fun `a folder at a JDBC table's location in a report-only root is not reported`() {
        folders(REST_ROOT, "orders", "orphan")
        folders(DATABASE_LOCATION, "crm_accounts", "old_orphan")
        val flow = flow(table("$REST_ROOT/orders", "iceberg"), table("$DATABASE_LOCATION/crm_accounts", JDBC))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(REST_ROOT, record.storageScanLocation)
        assertEquals(listOf("$DATABASE_LOCATION/old_orphan"), record.diagnosticDetails["report_only_folders_sample"]?.lines())
        assertTrue(flow.objectExists("$DATABASE_LOCATION/crm_accounts/part-0.parquet"))
    }

    @Test
    fun `a database with only JDBC tables has no table to anchor cleanup`() {
        folders(DATABASE_LOCATION, "leftover")
        val flow = flow(table("$DATABASE_LOCATION/crm_accounts", JDBC))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("no_active_tables_in_database", record.statusReason)
        assertTrue(flow.objectExists("$DATABASE_LOCATION/leftover/part-0.parquet"))
    }

    @Test
    fun `a JDBC table in the db folder does not make it a cleanup root next to the derived REST root`() {
        folders(DATABASE_LOCATION, "crm_accounts", "leftover")
        folders(REST_ROOT, "orders", "sub", "orphan")
        val flow =
            flow(
                table("$REST_ROOT/orders", "iceberg"),
                table("$REST_ROOT/sub/events", "iceberg"),
                table("$DATABASE_LOCATION/crm_accounts", JDBC),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertNull(record.statusReason)
        assertEquals(REST_ROOT, record.diagnosticDetails["cleanup_roots"])
        assertEquals(listOf("$REST_ROOT/orphan"), record.deletedFolders)
        assertEquals(listOf("$DATABASE_LOCATION/leftover"), record.diagnosticDetails["report_only_folders_sample"]?.lines())
        assertTrue(flow.objectExists("$DATABASE_LOCATION/leftover/part-0.parquet"))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/crm_accounts/part-0.parquet"))
    }

    @Test
    fun `a JDBC table never anchors the db folder when the REST root is not derived`() {
        folders(DATABASE_LOCATION, "crm_accounts", "leftover")
        folders(REST_ROOT, "orders", "sub", "orphan")
        val flow =
            flow(
                table("$REST_ROOT/orders", "iceberg"),
                table("$REST_ROOT/sub/events", "iceberg"),
                table("$DATABASE_LOCATION/crm_accounts", JDBC),
                restNamespaceRoot = RestNamespaceRoot(location = null, status = "unrecognized_uri"),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(DATABASE_LOCATION, record.storageScanLocation)
        assertEquals("refused:rest_root_not_derived", record.diagnosticDetails["managed_root_status"])
        assertEquals("BLOCKED", record.status)
        assertEquals("cleanup_root_has_no_visible_table", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DATABASE_LOCATION/leftover/part-0.parquet"))
    }

    @Test
    fun `an unresolved table next to a JDBC table stays blocked as unresolved`() {
        folders(DATABASE_LOCATION, "leftover")
        val flow =
            flow(
                table("$DATABASE_LOCATION/crm_accounts", JDBC),
                DiscoveredTable(CATALOG, "sales", "broken", isTemporary = false, location = null, unresolvedReason = "metadata missing"),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertTrue(flow.objectExists("$DATABASE_LOCATION/leftover/part-0.parquet"))
    }

    @Test
    fun `a CSV table in the db folder keeps it a cleanup root next to the REST root`() {
        folders(DATABASE_LOCATION, "raw_csv", "orphan")
        folders(REST_ROOT, "orders")
        val flow = flow(table("$REST_ROOT/orders", "iceberg"), table("$DATABASE_LOCATION/raw_csv", "csv"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals("$DATABASE_LOCATION\n$REST_ROOT", record.diagnosticDetails["cleanup_roots"])
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
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
        provider: String,
    ): DiscoveredTable =
        DiscoveredTable(CATALOG, "sales", location.substringAfterLast('/'), isTemporary = false, location = location, provider = provider)

    private fun flow(
        vararg tables: DiscoveredTable,
        restNamespaceRoot: RestNamespaceRoot = RestNamespaceRoot(location = REST_ROOT, status = "derived"),
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(catalog = CATALOG, databases = listOf("sales"), dryRun = false, deleteEnabled = true),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns
            DiscoveredDatabase(catalog = CATALOG, database = "sales", location = DATABASE_LOCATION, tables = tables.toList())
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { flow.catalogDiscoveryService.restNamespaceRoot(CATALOG, "sales") } returns restNamespaceRoot
        return flow
    }
}
