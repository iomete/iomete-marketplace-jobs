package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.ENDPOINT
import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
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
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val DB_FOLDER = "s3a://bucket/lake/sales.db"
private const val REST_FOLDER = "s3a://bucket/lake/sales"

// A database whose tables sit in both <warehouse>/sales.db and <warehouse>/sales, where the catalog derives
// <warehouse>/sales for this database. Both folders are cleanup roots.
class AutomaticMultiRootFlowTest {

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
    fun `orphans in both folders are deleted and every table folder survives`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertNull(record.statusReason)
        assertEquals(DB_FOLDER, record.storageScanLocation)
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.candidateFolders)
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertEquals(4, record.storageFolderCount)
        assertTrue(flow.objectExists("$DB_FOLDER/t_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/t_rest/part-0.parquet"))
        assertEquals("$DB_FOLDER\n$REST_FOLDER", record.diagnosticDetails["cleanup_roots"])
        assertEquals("used", record.diagnosticDetails["managed_root_status"])
        assertEquals("$DB_FOLDER=2\n$REST_FOLDER=2", record.diagnosticDetails["cleanup_root_folder_counts"])
        assertEquals("complete", record.diagnosticDetails["scan_plan_coverage"])
        assertEquals("0", record.diagnosticDetails["report_only_folder_count"])
        // Each folder is one root, listed once.
        assertEquals("2", record.diagnosticDetails["scan_root_count"])
        assertEquals(
            listOf("$DB_FOLDER=cleanup:scanned:visible_tables=1", "$REST_FOLDER=cleanup:scanned:visible_tables=1"),
            rootLines(record).sorted(),
        )
        val summary = flow.summaryLines()
        assertTrue("  Folders scanned under                            $DB_FOLDER" in summary)
        assertTrue("                                                   $REST_FOLDER" in summary)
        assertTrue("  Folders are deleted only under the cleanup roots. Other roots are listed for review only." in summary)
    }

    @Test
    fun `a dry run lists orphans in both folders and deletes nothing`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"), dryRun = true)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.candidateFolders)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertEquals("used", record.diagnosticDetails["managed_root_status"])
    }

    @Test
    fun `tables only in the folder without db plus one external table clean that folder instead of blocking`() {
        folders(REST_FOLDER, "t1", "t2", "orphan")
        folders("s3a://bucket2/ext", "t_ext", "orphan_ext")
        val flow = flow(tables = listOf("$REST_FOLDER/t1", "$REST_FOLDER/t2", "s3a://bucket2/ext/t_ext"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals("partial_scan_coverage", record.statusReason)
        assertEquals(REST_FOLDER, record.storageScanLocation)
        assertEquals(listOf("$REST_FOLDER/orphan"), record.deletedFolders)
        assertTrue(flow.objectExists("s3a://bucket2/ext/orphan_ext/part-0.parquet"))
        assertEquals(REST_FOLDER, record.diagnosticDetails["cleanup_roots"])
        assertEquals("used", record.diagnosticDetails["managed_root_status"])
        assertTrue("$DB_FOLDER=report_only:absent:visible_tables=0" in rootLines(record))
        // The .db folder that does not exist is never listed as a cleanup root.
        assertNull(InMemoryS3FileSystem.listStatusCalls[DB_FOLDER])
    }

    @Test
    fun `tables only in the db folder plus one external table keep the db folder alone`() {
        folders(DB_FOLDER, "t1", "orphan")
        val flow = flow(tables = listOf("$DB_FOLDER/t1", "s3a://bucket2/ext/t_ext"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DB_FOLDER/orphan"), record.deletedFolders)
        assertEquals(DB_FOLDER, record.diagnosticDetails["cleanup_roots"])
        assertEquals("refused:no_visible_table", record.diagnosticDetails["managed_root_status"])
        assertTrue("$REST_FOLDER=report_only:absent:visible_tables=0" in rootLines(record))
        assertTrue("  The database folder without .db is not a cleanup root: no_visible_table." in flow.summaryLines())
    }

    @Test
    fun `a folder that does not exist leaves the other folder cleaned`() {
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        // The table in the .db folder has no objects yet, so that folder does not exist.
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertEquals("$DB_FOLDER\n$REST_FOLDER", record.diagnosticDetails["cleanup_roots"])
        assertTrue("$DB_FOLDER=cleanup:absent:visible_tables=1" in rootLines(record))
        assertEquals("$REST_FOLDER=2", record.diagnosticDetails["cleanup_root_folder_counts"])
    }

    @Test
    fun `an empty folder is scanned with no folders and the other folder is cleaned`() {
        putFolder(DB_FOLDER)
        putFolder("$DB_FOLDER/t_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertTrue("- $DB_FOLDER  cleanup  scanned  visible tables at any depth: 1" in flow.summaryLines())
    }

    @Test
    fun `folders are found and deleted without directory marker objects`() {
        InMemoryS3FileSystem.impliedDirectories = true
        listOf("$DB_FOLDER/t_hive", "$DB_FOLDER/orphan_hive", "$REST_FOLDER/t_rest", "$REST_FOLDER/orphan_rest").forEach {
            InMemoryS3FileSystem.putFile(ENDPOINT, "$it/part-0.parquet", length = 1)
        }
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertTrue(flow.objectExists("$DB_FOLDER/t_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/t_rest/part-0.parquet"))
        assertFalse(InMemoryS3FileSystem.paths(ENDPOINT).any { it == DB_FOLDER || it == REST_FOLDER })
    }

    @Test
    fun `a listing failure in the folder without db leaves only the db folder cleaned`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        InMemoryS3FileSystem.failListFor = REST_FOLDER
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals("partial_scan_coverage", record.statusReason)
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertEquals(DB_FOLDER, record.diagnosticDetails["cleanup_roots"])
        assertEquals("refused:listing_failed", record.diagnosticDetails["managed_root_status"])
        assertTrue("$REST_FOLDER=cleanup:listing_failed:visible_tables=1" in rootLines(record))
        assertEquals("partial", record.diagnosticDetails["scan_plan_coverage"])
    }

    @Test
    fun `a listing failure in the db folder leaves only the other folder cleaned`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        InMemoryS3FileSystem.failListFor = DB_FOLDER
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("partial_scan_coverage", record.statusReason)
        assertEquals(REST_FOLDER, record.storageScanLocation)
        assertEquals(listOf("$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertEquals("used", record.diagnosticDetails["managed_root_status"])
        assertTrue("$DB_FOLDER=cleanup:listing_failed:visible_tables=1" in rootLines(record))
    }

    @Test
    fun `a listing failure in both folders fails the database and deletes nothing`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        InMemoryS3FileSystem.listFailures[DB_FOLDER] = java.io.IOException("denied")
        InMemoryS3FileSystem.listFailures[REST_FOLDER] = java.io.IOException("denied")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
    }

    @Test
    fun `an unresolved table blocks deletion in both folders`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"), unresolved = listOf(unresolved("broken")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertEquals("$DB_FOLDER\n$REST_FOLDER", record.diagnosticDetails["cleanup_roots"])
        assertEquals("used", record.diagnosticDetails["managed_root_status"])
    }

    @Test
    fun `an unresolved table's guessed folder never makes the folder without db a cleanup root`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "broken", "orphan_rest")
        val flow =
            flow(
                tables = listOf("$DB_FOLDER/t_hive", "s3a://bucket2/ext/t_ext"),
                unresolved = listOf(unresolved("broken", knownLocation = "$REST_FOLDER/broken")),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertTrue(record.deletedFolders.isEmpty())
        assertEquals(DB_FOLDER, record.diagnosticDetails["cleanup_roots"])
        assertEquals("refused:no_visible_table", record.diagnosticDetails["managed_root_status"])
        assertTrue("$REST_FOLDER=report_only:scanned:visible_tables=0" in rootLines(record))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
    }

    @Test
    fun `unknown storage of other databases blocks deletion in both folders`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } throws IllegalStateException("listing denied")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertEquals("$DB_FOLDER\n$REST_FOLDER", record.diagnosticDetails["cleanup_roots"])
    }

    @Test
    fun `another database stored inside the folder without db survives`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest", "x_data")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns
            mapOf("sales" to DB_FOLDER, "x" to "$REST_FOLDER/x_data")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_FOLDER/x_data/part-0.parquet"))
    }

    @Test
    fun `the candidate limit counts folders from both roots`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"), maxCandidateFolders = 1)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("too_many_candidate_folders", record.statusReason)
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.candidateFolders)
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertEquals("$DB_FOLDER\n$REST_FOLDER", record.diagnosticDetails["cleanup_roots"])
    }

    @Test
    fun `exclude_database_folders protects the folder under both roots`() {
        folders(DB_FOLDER, "t_hive", "keep", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "keep", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"), excludeDatabaseFolders = listOf("sales.keep"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertTrue(flow.objectExists("$DB_FOLDER/keep/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/keep/part-0.parquet"))
        assertTrue("$DB_FOLDER/keep" in record.excludedPaths)
        assertTrue("$REST_FOLDER/keep" in record.excludedPaths)
    }

    @Test
    fun `a table that appears in the folder without db before deletion protects its folder`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "late_table")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returnsMany
            listOf(
                database(listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest")),
                database(listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest", "$REST_FOLDER/late_table")),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/late_table"), record.candidateFolders)
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_FOLDER/late_table/part-0.parquet"))
    }

    @Test
    fun `a failed delete in the second root keeps the deletions already made`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        InMemoryS3FileSystem.failDeleteFor = "$REST_FOLDER/orphan_rest"
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertEquals("$REST_FOLDER/orphan_rest", record.diagnosticDetails["deletion_failed_folder_path"])
        assertEquals("$DB_FOLDER\n$REST_FOLDER", record.diagnosticDetails["cleanup_roots"])
        assertEquals("used", record.diagnosticDetails["managed_root_status"])
    }

    @Test
    fun `a REST root that is not derived keeps the folder without db report-only`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"), restNamespaceRoot = RestNamespaceRoot(null, "unrecognized_uri"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertEquals("refused:rest_root_not_derived", record.diagnosticDetails["managed_root_status"])
        assertEquals(listOf("$REST_FOLDER/orphan_rest"), reportOnlyFolders(record))
    }

    @Test
    fun `a derived REST root elsewhere keeps the folder without db report-only`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow =
            flow(
                tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"),
                restNamespaceRoot = RestNamespaceRoot("s3a://bucket/other_lake/sales", "derived"),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertEquals("refused:rest_root_differs", record.diagnosticDetails["managed_root_status"])
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
    }

    @Test
    fun `a single-parent database keeps one cleanup root and no managed root`() {
        folders(DB_FOLDER, "t1", "t2", "orphan")
        folders(REST_FOLDER, "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t1", "$DB_FOLDER/t2"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DB_FOLDER/orphan"), record.deletedFolders)
        assertEquals(DB_FOLDER, record.diagnosticDetails["cleanup_roots"])
        assertEquals("not_applicable", record.diagnosticDetails["managed_root_status"])
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertTrue("  Folders are deleted only under the cleanup root. Other roots are listed for review only." in flow.summaryLines())
    }

    @Test
    fun `another database stored at the folder without db protects every folder in it`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns
            mapOf("sales" to DB_FOLDER, "sales_copy" to REST_FOLDER)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertEquals("$DB_FOLDER\n$REST_FOLDER", record.diagnosticDetails["cleanup_roots"])
    }

    @Test
    fun `a table of a database at the warehouse that sits at the folder without db protects that folder`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns mapOf("sales" to DB_FOLDER, "default" to WAREHOUSE)
        every { flow.catalogDiscoveryService.tableLocationIfExists(CATALOG, "default", "sales") } returns REST_FOLDER

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
    }

    @Test
    fun `a failed other-database lookup under one root blocks both roots`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns mapOf("sales" to DB_FOLDER, "default" to WAREHOUSE)
        every { flow.catalogDiscoveryService.tableLocationIfExists(CATALOG, "default", "sales") } throws IllegalStateException("denied")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertTrue("default_folders_of_other_databases" in notChecked(record))
    }

    @Test
    fun `an unresolved table still blocks the db folder when the folder without db cannot be listed`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "broken")
        InMemoryS3FileSystem.failListFor = REST_FOLDER
        val flow =
            flow(
                tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"),
                unresolved = listOf(unresolved("broken", knownLocation = "$REST_FOLDER/broken")),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertEquals(DB_FOLDER, record.diagnosticDetails["cleanup_roots"])
        assertEquals("refused:listing_failed", record.diagnosticDetails["managed_root_status"])
    }

    @Test
    fun `a write path in the db folder of a table in the unlisted folder is protected`() {
        folders(DB_FOLDER, "t_hive", "rest_writes", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        InMemoryS3FileSystem.failListFor = REST_FOLDER
        val flow = flow(tables = emptyList())
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns
            DiscoveredDatabase(
                CATALOG,
                "sales",
                DB_FOLDER,
                listOf(table("t_hive", "$DB_FOLDER/t_hive"), table("t_rest", "$REST_FOLDER/t_rest", "$DB_FOLDER/rest_writes/data")),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.candidateFolders)
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertTrue(flow.objectExists("$DB_FOLDER/rest_writes/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
    }

    @Test
    fun `an external table's write path inside the cleanup root is protected`() {
        folders(REST_FOLDER, "t1", "ext_writes", "orphan")
        val flow = flow(tables = emptyList())
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns
            DiscoveredDatabase(
                CATALOG,
                "sales",
                DB_FOLDER,
                listOf(table("t1", "$REST_FOLDER/t1"), table("t_ext", "s3a://bucket2/ext/t_ext", "$REST_FOLDER/ext_writes/data")),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals("partial_scan_coverage", record.statusReason)
        assertEquals(listOf("$REST_FOLDER/orphan"), record.candidateFolders)
        assertEquals(listOf("$REST_FOLDER/orphan"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_FOLDER/ext_writes/part-0.parquet"))
    }

    @Test
    fun `partial coverage with an unresolved table blocks deletion`() {
        folders(REST_FOLDER, "t1", "orphan")
        val flow = flow(tables = listOf("$REST_FOLDER/t1", "s3a://bucket2/ext/t_ext"), unresolved = listOf(unresolved("broken")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$REST_FOLDER/orphan/part-0.parquet"))
    }

    @Test
    fun `counts and root fields cover only the roots that were listed`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest", "orphan_rest_2")
        InMemoryS3FileSystem.failListFor = REST_FOLDER
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(2, record.storageFolderCount)
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.candidateFolders)
        assertEquals(record.candidateFolders, record.deletedFolders)
        assertEquals(DB_FOLDER, record.diagnosticDetails["cleanup_roots"])
        assertEquals("$DB_FOLDER=2", record.diagnosticDetails["cleanup_root_folder_counts"])
    }

    // Another catalog on the same warehouse keeps its own sales database at <warehouse>/sales.
    @Test
    fun `another catalog's database at the folder without db protects every folder in it while the db folder is cleaned`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "views_t", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("views" to listOf(WAREHOUSE))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("views") } returns mapOf("sales" to REST_FOLDER)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB_FOLDER/orphan_hive"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_FOLDER/views_t/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
    }

    @Test
    fun `a table that a catalog with no warehouse setting keeps in the folder without db survives`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "remote_t", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("remote" to emptyList())
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("remote") } returns mapOf("reports" to "s3a://bucket/remote/reports")
        every { flow.catalogDiscoveryService.discoverTables("remote", "reports") } returns
            listOf(DiscoveredTable("remote", "reports", "remote_t", false, location = "$REST_FOLDER/remote_t"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertTrue(flow.objectExists("$REST_FOLDER/remote_t/part-0.parquet"))
        assertTrue("visible_storage_of_same_bucket_session_catalogs" in checks(record))
        assertTrue("final_recheck_before_deletion" in checks(record))
    }

    // A database at the warehouse could hold a table on the path of either root. Only the lookup for the folder
    // without .db fails.
    @Test
    fun `another catalog's storage that cannot be read under the folder without db blocks both roots`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("views" to listOf(WAREHOUSE))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("views") } returns mapOf("default" to WAREHOUSE)
        every { flow.catalogDiscoveryService.tableLocationIfExists("views", "default", "sales") } throws IllegalStateException("denied")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        verify(exactly = 1) { flow.catalogDiscoveryService.tableLocationIfExists("views", "default", "sales.db") }
        assertFalse("visible_storage_of_same_bucket_session_catalogs" in checks(record))
        assertTrue("same_bucket_session_catalogs" in notChecked(record))
        assertTrue("final_recheck_before_deletion" in notChecked(record))
    }

    @Test
    fun `another catalog's storage that cannot be read under the folder without db at the final check blocks both roots`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("views" to listOf(WAREHOUSE))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("views") } returns mapOf("default" to WAREHOUSE)
        every { flow.catalogDiscoveryService.tableLocationIfExists("views", "default", "sales") } returns null andThenThrows
            IllegalStateException("denied")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("other_database_storage_unknown", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$DB_FOLDER/orphan_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/orphan_rest/part-0.parquet"))
        assertTrue("visible_storage_of_same_bucket_session_catalogs" in checks(record))
        assertFalse("final_recheck_before_deletion" in checks(record))
        assertTrue("final_recheck_before_deletion" in notChecked(record))
    }

    @Test
    fun `a table that another catalog places in the folder without db before the final check keeps that folder`() {
        folders(DB_FOLDER, "t_hive", "orphan_hive")
        folders(REST_FOLDER, "t_rest", "late_t", "orphan_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("views" to listOf(WAREHOUSE))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("views") } returns mapOf("reports" to "$WAREHOUSE/reports.db")
        every { flow.catalogDiscoveryService.discoverTables("views", "reports") } returnsMany
            listOf(emptyList(), listOf(DiscoveredTable("views", "reports", "late_t", false, location = "$REST_FOLDER/late_t")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/late_t", "$REST_FOLDER/orphan_rest"), record.candidateFolders)
        assertEquals(listOf("$DB_FOLDER/orphan_hive", "$REST_FOLDER/orphan_rest"), record.deletedFolders)
        assertEquals("$REST_FOLDER/late_t", record.diagnosticDetails["folder_kept_at_final_check_paths_sample"])
        assertTrue(flow.objectExists("$REST_FOLDER/late_t/part-0.parquet"))
    }

    @Test
    fun `when the final check keeps every candidate in both roots nothing is deleted and the row counts them as kept`() {
        folders(DB_FOLDER, "t_hive", "late_hive")
        folders(REST_FOLDER, "t_rest", "late_rest")
        val flow = flow(tables = listOf("$DB_FOLDER/t_hive", "$REST_FOLDER/t_rest"))
        every { flow.catalogDiscoveryService.sessionCatalogWarehouses() } returns mapOf("views" to listOf(WAREHOUSE))
        every { flow.catalogDiscoveryService.visibleDatabaseLocations("views") } returns mapOf("reports" to "$WAREHOUSE/reports.db")
        every { flow.catalogDiscoveryService.discoverTables("views", "reports") } returnsMany
            listOf(
                emptyList(),
                listOf(
                    DiscoveredTable("views", "reports", "late_hive", false, location = "$DB_FOLDER/late_hive"),
                    DiscoveredTable("views", "reports", "late_rest", false, location = "$REST_FOLDER/late_rest"),
                ),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB_FOLDER/late_hive", "$REST_FOLDER/late_rest"), record.candidateFolders)
        assertTrue(record.deletedFolders.isEmpty())
        assertEquals("2", record.diagnosticDetails["folder_kept_at_final_check_count"])
        assertTrue("final_recheck_before_deletion" in checks(record))
        assertTrue(flow.objectExists("$DB_FOLDER/late_hive/part-0.parquet"))
        assertTrue(flow.objectExists("$REST_FOLDER/late_rest/part-0.parquet"))
        assertTrue("  Folders kept by the final check                  2" in flow.summaryLines())
    }

    private fun checks(record: CleanupAuditRecord): List<String> = record.diagnosticDetails.getValue("ownership_checks").split(",")

    private fun notChecked(record: CleanupAuditRecord): List<String> =
        record.diagnosticDetails.getValue("ownership_not_checked").split(",")

    private fun table(
        name: String,
        location: String,
        vararg writeLocations: String,
    ): DiscoveredTable = DiscoveredTable(CATALOG, "sales", name, isTemporary = false, location = location, writeLocations = writeLocations.toList())

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

    private fun unresolved(
        name: String,
        knownLocation: String? = null,
    ): DiscoveredTable =
        DiscoveredTable(
            CATALOG,
            "sales",
            name,
            isTemporary = false,
            location = null,
            unresolvedReason = "metadata file is missing",
            knownLocation = knownLocation,
        )

    private fun database(
        tables: List<String>,
        unresolved: List<DiscoveredTable> = emptyList(),
    ): DiscoveredDatabase =
        DiscoveredDatabase(
            catalog = CATALOG,
            database = "sales",
            location = DB_FOLDER,
            tables =
                tables.mapIndexed { index, location ->
                    DiscoveredTable(CATALOG, "sales", "table_$index", isTemporary = false, location = location)
                } + unresolved,
        )

    private fun flow(
        tables: List<String>,
        unresolved: List<DiscoveredTable> = emptyList(),
        restNamespaceRoot: RestNamespaceRoot? = RestNamespaceRoot(REST_FOLDER, "derived"),
        dryRun: Boolean = false,
        maxCandidateFolders: Int = 100,
        excludeDatabaseFolders: List<String> = emptyList(),
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(
                    catalog = CATALOG,
                    databases = listOf("sales"),
                    excludeDatabaseFolders = excludeDatabaseFolders,
                    dryRun = dryRun,
                    deleteEnabled = !dryRun,
                    maxCandidateFoldersPerDatabase = maxCandidateFolders,
                ),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns database(tables, unresolved)
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { flow.catalogDiscoveryService.restNamespaceRoot(CATALOG, "sales") } returns restNamespaceRoot
        return flow
    }
}
