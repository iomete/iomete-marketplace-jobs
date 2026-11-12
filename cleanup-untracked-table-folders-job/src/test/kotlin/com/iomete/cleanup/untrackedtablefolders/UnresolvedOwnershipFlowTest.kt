package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.OwnershipChecks
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.NoVerifiedLocationSummary
import com.iomete.cleanup.untrackedtablefolders.logging.TooManyFoldersSummary
import com.iomete.cleanup.untrackedtablefolders.logging.UnresolvedTableDetail
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
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
private const val SALES = "s3a://bucket/lake/sales.db"

class UnresolvedOwnershipFlowTest {

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
    fun `a database with only unresolved tables is blocked without touching storage`() {
        folders("t1", "t2", "orphan")
        val flow = flow(tables = listOf(unresolved("t1"), unresolved("t2")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals(2, record.unresolvedTableCount)
        assertEquals(2, record.activeTableCount)
        assertEquals("", record.storageScanLocation)
        assertNull(record.cutoffTime)
        assertEquals(0, record.storageFolderCount)
        assertEquals(0, record.potentiallyUntrackedFolderCount)
        assertEquals(0, record.candidateFolderCount)
        assertTrue(record.diagnosticDetails.getValue("unresolved_tables_sample").contains("$CATALOG.sales.t1"))
        assertTrue(InMemoryS3FileSystem.listStatusCalls.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertTrue(flow.summaries.isEmpty())
        verify(exactly = 1) {
            flow.service.cleanupSummaryLogger.logNoVerifiedLocationReport(
                NoVerifiedLocationSummary(
                    catalog = CATALOG,
                    database = "sales",
                    discoveredDatabaseLocation = SALES,
                    catalogTableCount = 2,
                    unresolvedTableNames = listOf("$CATALOG.sales.t1", "$CATALOG.sales.t2"),
                    dryRun = false,
                    unresolvedTableDetails =
                        mapOf(
                            "$CATALOG.sales.t1" to UnresolvedTableDetail("metadata could not be read", null),
                            "$CATALOG.sales.t2" to UnresolvedTableDetail("metadata could not be read", null),
                        ),
                ),
            )
        }
    }

    @Test
    fun `a dry run of a database with only unresolved tables gives the same row`() {
        folders("t1")
        val flow = flow(tables = listOf(unresolved("t1")), deleteEnabled = false)

        flow.run()

        assertEquals("unresolved_catalog_ownership", flow.auditRecords.single().statusReason)
        assertTrue(InMemoryS3FileSystem.listStatusCalls.isEmpty())
    }

    @Test
    fun `the folder limit cannot hide a database with only unresolved tables`() {
        folders(*(1..12).map { "t$it" }.toTypedArray())
        val flow = flow(tables = listOf(unresolved("t1")), maxCandidateFolders = 1)

        flow.run()

        assertEquals("unresolved_catalog_ownership", flow.auditRecords.single().statusReason)
    }

    @Test
    fun `a missing database location cannot hide a database with only unresolved tables`() {
        val flow = flow(tables = listOf(unresolved("t1")), databaseLocation = null)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals(1, record.unresolvedTableCount)
    }

    @Test
    fun `an unsafe database location cannot hide a database with only unresolved tables`() {
        val flow = flow(tables = listOf(unresolved("t1")), databaseLocation = "s3a://bucket")

        flow.run()

        assertEquals("unresolved_catalog_ownership", flow.auditRecords.single().statusReason)
    }

    @Test
    fun `a view alongside an unresolved table does not change the outcome`() {
        val flow = flow(tables = listOf(unresolved("t1"), view("v1")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals(1, record.unresolvedTableCount)
        assertEquals(2, record.activeTableCount)
    }

    @Test
    fun `a database with only views is still skipped as having no active tables`() {
        val flow = flow(tables = listOf(view("v1")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("no_active_tables_in_database", record.statusReason)
        assertEquals(0, record.unresolvedTableCount)
    }

    @Test
    fun `over the limit with unresolved tables stays skipped and reports both conditions`() {
        folders("t1", "t2", "orphan_a", "orphan_b")
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2")), maxCandidateFolders = 1)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("too_many_candidate_folders", record.statusReason)
        assertEquals(1, record.unresolvedTableCount)
        assertEquals(3, record.potentiallyUntrackedFolderCount)
        assertEquals(0, record.candidateFolderCount)
        assertTrue(record.candidateFolders.isEmpty())
        assertEquals(1, record.maxCandidateFoldersPerDatabase)
        assertEquals(
            "Detected 3 candidate untracked table folder(s), which exceeds max_candidate_folders_per_database=1. " +
                "1 catalog table(s) also have unresolved storage ownership. Raising the limit will not make these folders deletable.",
            record.errorMessage,
        )
        assertNull(InMemoryS3FileSystem.listStatusCalls["$SALES/t2"])
        assertNull(InMemoryS3FileSystem.listStatusCalls["$SALES/orphan_a"])
        assertTrue(flow.objectExists("$SALES/orphan_a/part-0.parquet"))
        assertTrue(flow.summaries.isEmpty())
        verify(exactly = 1) {
            flow.service.cleanupSummaryLogger.logTooManyFoldersReport(
                TooManyFoldersSummary(
                    catalog = CATALOG,
                    database = "sales",
                    discoveredDatabaseLocation = SALES,
                    storageScanLocation = SALES,
                    catalogTableCount = 2,
                    verifiedTableCount = 1,
                    unresolvedTableNames = listOf("$CATALOG.sales.t2"),
                    storageFolderCount = 4,
                    unmatchedFolderPaths = listOf("$SALES/orphan_a", "$SALES/orphan_b", "$SALES/t2"),
                    maxCandidateFolders = 1,
                    dryRun = false,
                    unresolvedTableDetails = mapOf("$CATALOG.sales.t2" to UnresolvedTableDetail("metadata could not be read", null)),
                    ownershipChecks = OwnershipChecks(otherDatabaseFolders = true, otherTables = true),
                ),
            )
        }
    }

    @Test
    fun `raising the limit with unresolved tables blocks deletion instead of allowing it`() {
        folders("t1", "t2", "orphan_a", "orphan_b")
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2")), maxCandidateFolders = 10)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan_a/part-0.parquet"))
    }

    @Test
    fun `over the limit without unresolved tables is unchanged`() {
        folders("t1", "orphan_a", "orphan_b")
        val flow = flow(tables = listOf(verified("t1")), maxCandidateFolders = 1)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("too_many_candidate_folders", record.statusReason)
        assertEquals(0, record.unresolvedTableCount)
        assertEquals(0, record.potentiallyUntrackedFolderCount)
        assertEquals(2, record.candidateFolderCount)
        assertEquals(listOf("$SALES/orphan_a", "$SALES/orphan_b"), record.candidateFolders)
        assertEquals("Detected 2 candidate untracked table folder(s), which exceeds max_candidate_folders_per_database=1", record.errorMessage)
        verify(exactly = 1) {
            flow.service.cleanupSummaryLogger.logTooManyFoldersReport(
                TooManyFoldersSummary(
                    catalog = CATALOG,
                    database = "sales",
                    discoveredDatabaseLocation = SALES,
                    storageScanLocation = SALES,
                    catalogTableCount = 1,
                    verifiedTableCount = 1,
                    unresolvedTableNames = emptyList(),
                    storageFolderCount = 3,
                    unmatchedFolderPaths = listOf("$SALES/orphan_a", "$SALES/orphan_b"),
                    maxCandidateFolders = 1,
                    dryRun = false,
                    ownershipChecks = OwnershipChecks(otherDatabaseFolders = true, otherTables = true),
                ),
            )
        }
    }

    @Test
    fun `a missing database location keeps its reason and reports the unresolved tables`() {
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2")), databaseLocation = null)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("database_location_missing", record.statusReason)
        assertEquals(1, record.unresolvedTableCount)
        assertEquals("$CATALOG.sales.t2", record.diagnosticDetails["unresolved_tables_sample"])
    }

    @Test
    fun `an unsafe scan root keeps its reason and reports the unresolved tables`() {
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2")), databaseLocation = WAREHOUSE)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED", record.status)
        assertEquals("database_location_contains_warehouse_root", record.statusReason)
        assertEquals(1, record.unresolvedTableCount)
        assertEquals("$CATALOG.sales.t2", record.diagnosticDetails["unresolved_tables_sample"])
    }

    @Test
    fun `a storage failure after discovery keeps its reason and reports the unresolved tables`() {
        folders("t1")
        InMemoryS3FileSystem.failListFor = SALES
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertEquals("unexpected_error", record.statusReason)
        assertTrue(record.errorMessage.orEmpty().startsWith("Failed to list immediate child folders"))
        assertEquals(1, record.unresolvedTableCount)
        assertEquals("$CATALOG.sales.t2", record.diagnosticDetails["unresolved_tables_sample"])
    }

    @Test
    fun `a discovery failure leaves the unresolved count at zero because it is unknown`() {
        val flow = flow(tables = emptyList())
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } throws IllegalStateException("catalog unavailable")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertEquals(0, record.unresolvedTableCount)
    }

    @Test
    fun `a known location reduces the unmatched folders but unresolved ownership still blocks deletion`() {
        folders("t1", "t2", "orphan")
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2", knownLocation = "$SALES/t2")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals(1, record.unresolvedTableCount)
        assertEquals(1, record.potentiallyUntrackedFolderCount)
        assertEquals("$SALES/orphan", record.diagnosticDetails["potentially_untracked_folder_paths_sample"])
        assertEquals(0, record.candidateFolderCount)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertTrue(flow.objectExists("$SALES/t2/part-0.parquet"))
        assertTrue(flow.summaryLines().contains("- $SALES/t2"))
        assertTrue(
            flow.summaryLines().joinToString("\n")
                .contains("- $CATALOG.sales.t2\n    reason: metadata could not be read\n    known folder: $SALES/t2"),
        )
        verify(exactly = 1) { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") }
    }

    @Test
    fun `unresolved ownership still blocks when a known location leaves no unmatched folder`() {
        folders("t1", "t2")
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2", knownLocation = "$SALES/t2")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals(0, record.potentiallyUntrackedFolderCount)
        assertTrue(record.deletedFolders.isEmpty())
    }

    @Test
    fun `an unresolved table leaves scan coverage partial even when its known folder is under the cleanup root`() {
        folders("t1", "t2", "orphan")
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2", knownLocation = "$SALES/t2")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("partial", record.diagnosticDetails["scan_plan_coverage"])
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertTrue(record.deletedFolders.isEmpty())
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `a known location that brings the count within the limit still blocks deletion`() {
        folders("t1", "t2", "orphan")
        val flow = flow(tables = listOf(verified("t1"), unresolved("t2", knownLocation = "$SALES/t2")), maxCandidateFolders = 1)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals(1, record.potentiallyUntrackedFolderCount)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    @Test
    fun `a known location does not anchor a scan when no table is verified`() {
        folders("t1", "orphan")
        val flow = flow(tables = listOf(unresolved("t1", knownLocation = "$SALES/t1")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals("", record.storageScanLocation)
        assertTrue(InMemoryS3FileSystem.listStatusCalls.isEmpty())
    }

    @Test
    fun `a known location does not select the scan root or count as verified`() {
        putFolder("$SALES/nested")
        putFolder("$SALES/nested/t1", "part-0.parquet")
        putFolder("$SALES/t2", "part-0.parquet")
        val flow =
            flow(
                tables =
                    listOf(
                        DiscoveredTable(CATALOG, "sales", "t1", isTemporary = false, location = "$SALES/nested/t1"),
                        unresolved("t2", knownLocation = "$SALES/t2"),
                    ),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("$SALES/nested", record.storageScanLocation)
        assertEquals("$SALES/nested/t1", record.diagnosticDetails["active_table_locations_sample"])
        assertEquals(listOf("$SALES/nested/t1"), flow.summaries.single().activeTableLocations)
    }

    @Test
    fun `a known location at or above the scan root does not hide the unmatched folders`() {
        listOf(SALES, WAREHOUSE, "s3a://bucket").forEach { knownLocation ->
            InMemoryS3FileSystem.reset()
            folders("t1", "t2", "orphan")
            val flow = flow(tables = listOf(verified("t1"), unresolved("t2", knownLocation = knownLocation)), maxCandidateFolders = 1)

            flow.run()

            val record = flow.auditRecords.single()
            assertEquals("too_many_candidate_folders", record.statusReason, knownLocation)
            assertEquals(2, record.potentiallyUntrackedFolderCount, knownLocation)
        }
    }

    @Test
    fun `a known location equal to a scan root below the database location does not hide its folders`() {
        putFolder("$SALES/nested")
        listOf("t1", "orphan").forEach { putFolder("$SALES/nested/$it", "part-0.parquet") }
        val flow =
            flow(
                tables =
                    listOf(
                        DiscoveredTable(CATALOG, "sales", "t1", isTemporary = false, location = "$SALES/nested/t1"),
                        unresolved("t2", knownLocation = "$SALES/nested"),
                    ),
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("$SALES/nested", record.storageScanLocation)
        assertEquals("BLOCKED", record.status)
        assertEquals(1, record.potentiallyUntrackedFolderCount)
        assertEquals("$SALES/nested/orphan", record.diagnosticDetails["potentially_untracked_folder_paths_sample"])
    }

    private fun folders(vararg children: String) {
        putFolder(SALES)
        children.forEach { putFolder("$SALES/$it", "part-0.parquet") }
    }

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
            unresolvedReason = "metadata could not be read",
            knownLocation = knownLocation,
        )

    private fun verified(name: String): DiscoveredTable = DiscoveredTable(CATALOG, "sales", name, isTemporary = false, location = "$SALES/$name")

    private fun view(name: String): DiscoveredTable = DiscoveredTable(CATALOG, "sales", name, isTemporary = false, location = null)

    private fun flow(
        tables: List<DiscoveredTable>,
        deleteEnabled: Boolean = true,
        databaseLocation: String? = SALES,
        maxCandidateFolders: Int = 10,
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(
                    catalog = CATALOG,
                    databases = listOf("sales"),
                    dryRun = !deleteEnabled,
                    deleteEnabled = deleteEnabled,
                    maxCandidateFoldersPerDatabase = maxCandidateFolders,
                ),
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns DiscoveredDatabase(CATALOG, "sales", databaseLocation, tables)
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns mapOf("sales" to SALES)
        return flow
    }
}
