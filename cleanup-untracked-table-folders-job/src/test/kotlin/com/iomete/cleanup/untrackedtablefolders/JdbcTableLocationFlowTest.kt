package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.catalog.RestNamespaceRoot
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummaryLogger
import com.iomete.cleanup.untrackedtablefolders.logging.NoVerifiedLocationSummary
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import io.mockk.slot
import io.mockk.verify
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
    fun `a database with only JDBC tables is skipped and its audit row still counts them`() {
        folders(DATABASE_LOCATION, "accounts", "contacts", "leftover")
        val flow =
            flow(
                table("$DATABASE_LOCATION/accounts", JDBC),
                table("$DATABASE_LOCATION/contacts", JDBC),
                table("$DATABASE_LOCATION/leads", JDBC),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("no_active_tables_in_database", record.statusReason)
        assertEquals(
            "Database has no active table that stores files at a known location; cleanup skipped to avoid deleting an unanchored storage root.",
            record.errorMessage,
        )
        assertEquals(3, record.activeTableCount)
        assertEquals(0, record.unresolvedTableCount)
        assertEquals(
            listOf("$DATABASE_LOCATION/accounts", "$DATABASE_LOCATION/contacts", "$DATABASE_LOCATION/leads"),
            record.diagnosticDetails["active_table_locations_sample"]?.lines(),
        )
        assertTrue(flow.objectExists("$DATABASE_LOCATION/leftover/part-0.parquet"))
    }

    @Test
    fun `a database with only JDBC tables and no location is skipped the same way`() {
        val flow = flow(table("$DATABASE_LOCATION/accounts", JDBC), databaseLocation = null)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("no_active_tables_in_database", record.statusReason)
        assertEquals(1, record.activeTableCount)
        assertEquals("$DATABASE_LOCATION/accounts", record.diagnosticDetails["active_table_locations_sample"])
    }

    @Test
    fun `a database with no location still counts and lists its JDBC tables`() {
        val flow = flow(table("$REST_ROOT/orders", "iceberg"), table("$DATABASE_LOCATION/accounts", JDBC), databaseLocation = null)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("database_location_missing", record.statusReason)
        assertEquals(2, record.activeTableCount)
        assertEquals(
            listOf("$DATABASE_LOCATION/accounts", "$REST_ROOT/orders"),
            record.diagnosticDetails["active_table_locations_sample"]?.lines(),
        )
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
        assertEquals("", record.storageScanLocation)
        assertEquals(2, record.activeTableCount)
        assertEquals(1, record.unresolvedTableCount)
        assertEquals("$DATABASE_LOCATION/crm_accounts", record.diagnosticDetails["active_table_locations_sample"])
        assertTrue(flow.objectExists("$DATABASE_LOCATION/leftover/part-0.parquet"))

        val summary = slot<NoVerifiedLocationSummary>()
        verify { flow.service.cleanupSummaryLogger.logNoVerifiedLocationReport(capture(summary)) }
        val report = CleanupSummaryLogger().buildNoVerifiedLocationReportLines(summary.captured)
        assertTrue("  Tables found                                     2" in report)
        assertTrue("  Tables with verified storage locations           1" in report)
        assertTrue("  Tables whose storage location could not be read  1" in report)
        assertTrue("  Storage was not scanned. No table that stores files has a verified location, so" in report)
        val jdbcNote = "  The verified locations here are JDBC table locations from the catalog. They hold"
        assertTrue(jdbcNote in report)
        assertTrue(jdbcNote !in CleanupSummaryLogger().buildNoVerifiedLocationReportLines(summary.captured.copy(verifiedTableCount = 0)))
    }

    @Test
    fun `another database's JDBC table protects its folder in the cleanup root`() {
        folders(DATABASE_LOCATION, "orders", "theirs", "orphan")
        val flow = flow(table("$DATABASE_LOCATION/orders", "iceberg"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns
            mapOf("sales" to DATABASE_LOCATION, "crm" to "$WAREHOUSE/crm.db")
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "crm") } returns listOf(crmTable("$DATABASE_LOCATION/theirs"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.candidateFolders)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        assertTrue(flow.objectExists("$DATABASE_LOCATION/theirs/part-0.parquet"))
    }

    @Test
    fun `a JDBC table that another database places in a candidate folder before the final check keeps that folder`() {
        folders(DATABASE_LOCATION, "orders", "theirs", "orphan")
        val flow = flow(table("$DATABASE_LOCATION/orders", "iceberg"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns
            mapOf("sales" to DATABASE_LOCATION, "crm" to "$WAREHOUSE/crm.db")
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "crm") } returnsMany
            listOf(emptyList(), listOf(crmTable("$DATABASE_LOCATION/theirs")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DATABASE_LOCATION/orphan", "$DATABASE_LOCATION/theirs"), record.candidateFolders)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        assertEquals("$DATABASE_LOCATION/theirs", record.diagnosticDetails["folder_kept_at_final_check_paths_sample"])
        assertTrue(flow.objectExists("$DATABASE_LOCATION/theirs/part-0.parquet"))
    }

    @Test
    fun `a JDBC table that appears at a candidate folder before deletion keeps that folder`() {
        folders(DATABASE_LOCATION, "orders", "orphan")
        val flow = flow(table("$DATABASE_LOCATION/orders", "iceberg"))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returnsMany
            listOf(
                database(table("$DATABASE_LOCATION/orders", "iceberg")),
                database(table("$DATABASE_LOCATION/orders", "iceberg"), table("$DATABASE_LOCATION/orphan", JDBC)),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.candidateFolders)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan/part-0.parquet"))
    }

    @Test
    fun `storage roots are planned from tables that store files while JDBC locations stay protected`() {
        folders(DATABASE_LOCATION, "crm_accounts", "leftover")
        folders(REST_ROOT, "orders", "sub", "orphan")
        val flow =
            flow(
                table("$REST_ROOT/orders", "iceberg"),
                table("$REST_ROOT/sub/events", "iceberg"),
                table("$DATABASE_LOCATION/crm_accounts", JDBC),
                table("s3a://bucket/lake/old_sales.db/crm_leads", JDBC),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(
            listOf(
                "$REST_ROOT=cleanup:scanned:visible_tables=2",
                "$REST_ROOT/sub=report_only:scanned:visible_tables=1",
                "$DATABASE_LOCATION=report_only:scanned:visible_tables=0",
            ),
            record.diagnosticDetails["scan_roots_sample"]?.lines(),
        )
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
        assertEquals(listOf("$DATABASE_LOCATION/leftover"), record.diagnosticDetails["report_only_folders_sample"]?.lines())
        assertTrue(flow.objectExists("$DATABASE_LOCATION/crm_accounts/part-0.parquet"))
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

    private fun crmTable(location: String): DiscoveredTable =
        DiscoveredTable(CATALOG, "crm", location.substringAfterLast('/'), isTemporary = false, location = location, provider = JDBC)

    private fun database(
        vararg tables: DiscoveredTable,
        location: String? = DATABASE_LOCATION,
    ): DiscoveredDatabase = DiscoveredDatabase(catalog = CATALOG, database = "sales", location = location, tables = tables.toList())

    private fun flow(
        vararg tables: DiscoveredTable,
        restNamespaceRoot: RestNamespaceRoot = RestNamespaceRoot(location = REST_ROOT, status = "derived"),
        databaseLocation: String? = DATABASE_LOCATION,
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(catalog = CATALOG, databases = listOf("sales"), dryRun = false, deleteEnabled = true),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns database(*tables, location = databaseLocation)
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { flow.catalogDiscoveryService.restNamespaceRoot(CATALOG, "sales") } returns restNamespaceRoot
        return flow
    }
}
