package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

private const val CATALOG = "example_catalog"
private const val ENDPOINT = CleanupFlowHarness.ENDPOINT
private const val DATABASE_LOCATION = "s3a://bucket/lake/sales.db"

class CandidateAgeFlowTest {

    private val now = System.currentTimeMillis()
    private val tenDaysAgo = now - Duration.ofDays(10).toMillis()

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        InMemoryS3FileSystem.putDirectory(ENDPOINT, DATABASE_LOCATION)
        folder("orders", tenDaysAgo)
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `folder with a recently written object survives`() {
        folder("orphan_old", tenDaysAgo)
        folder("orphan_fresh", now - 5_000)
        val flow = flow(deleteEnabled = true)

        flow.run()

        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_fresh/part-0.parquet"))
        assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan_old/part-0.parquet"))
    }

    @Test
    fun `empty folder is never a candidate`() {
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$DATABASE_LOCATION/empty")
        val flow = flow(deleteEnabled = false)

        flow.run()

        assertFalse("$DATABASE_LOCATION/empty" in flow.auditRecords.single().candidateFolders)
    }

    @Test
    fun `folder whose listing fails survives`() {
        folder("orphan_old", tenDaysAgo)
        folder("orphan_unreadable", tenDaysAgo)
        InMemoryS3FileSystem.failListFor = "$DATABASE_LOCATION/orphan_unreadable"
        val flow = flow(deleteEnabled = true)

        flow.run()

        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_unreadable/part-0.parquet"))
        assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan_old/part-0.parquet"))
    }

    @Test
    fun `object with an invalid modification time keeps its folder out of the candidates`() {
        folder("orphan_invalid", 0)
        val flow = flow(deleteEnabled = false)

        flow.run()

        assertFalse("$DATABASE_LOCATION/orphan_invalid" in flow.auditRecords.single().candidateFolders)
    }

    @Test
    fun `collect_size_statistics false still collects statistics and checks age`() {
        folder("orphan_old", tenDaysAgo)
        folder("orphan_fresh", now - 5_000)
        val flow = flow(deleteEnabled = true, collectSizeStatistics = false)

        flow.run()

        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_fresh/part-0.parquet"))
        assertNotNull(flow.auditRecords.single().candidateObjectCount)
    }

    @Test
    fun `partial scan coverage is reported on success`() {
        folder("orphan_old", tenDaysAgo)
        val flow = flow(deleteEnabled = false, tableLocations = listOf("$DATABASE_LOCATION/orders", "s3a://bucket/other/events"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals("partial_scan_coverage", record.statusReason)
    }

    @Test
    fun `blocked database keeps its reason and reports partial coverage`() {
        folder("orphan_old", tenDaysAgo)
        val flow =
            flow(
                deleteEnabled = true,
                tableLocations = listOf("$DATABASE_LOCATION/orders", "s3a://bucket/other/events"),
                unresolvedTable = "broken",
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals("1", record.diagnosticDetails["unscanned_table_location_count"])
    }

    @Test
    fun `candidate cap is enforced before any recursive traversal`() {
        folder("orphan_a", tenDaysAgo)
        folder("orphan_b", tenDaysAgo)
        val flow = flow(deleteEnabled = false, maxCandidateFolders = 1)

        flow.run()

        assertEquals("too_many_candidate_folders", flow.auditRecords.single().statusReason)
        assertNull(InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/orphan_a"])
        assertNull(InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/orphan_b"])
    }

    @Test
    fun `each candidate is traversed once for both statistics and age`() {
        folder("orphan_a", tenDaysAgo)
        folder("orphan_b", tenDaysAgo)
        val flow = flow(deleteEnabled = false)

        flow.run()

        assertEquals(1, InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/orphan_a"])
        assertEquals(1, InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/orphan_b"])
    }

    private fun folder(
        name: String,
        objectModificationTimeMillis: Long,
    ) {
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$DATABASE_LOCATION/$name")
        InMemoryS3FileSystem.putFile(
            ENDPOINT,
            "$DATABASE_LOCATION/$name/part-0.parquet",
            length = 1,
            modificationTimeMillis = objectModificationTimeMillis,
        )
    }

    private fun flow(
        deleteEnabled: Boolean,
        collectSizeStatistics: Boolean = true,
        maxCandidateFolders: Int = 10,
        tableLocations: List<String> = listOf("$DATABASE_LOCATION/orders"),
        unresolvedTable: String? = null,
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(
                    catalog = CATALOG,
                    databases = listOf("sales"),
                    dryRun = !deleteEnabled,
                    deleteEnabled = deleteEnabled,
                    maxCandidateFoldersPerDatabase = maxCandidateFolders,
                    collectSizeStatistics = collectSizeStatistics,
                ),
            )
        val tables =
            tableLocations.mapIndexed { index, location ->
                DiscoveredTable(CATALOG, "sales", "table_$index", isTemporary = false, location = location)
            } +
                listOfNotNull(
                    unresolvedTable?.let {
                        DiscoveredTable(CATALOG, "sales", it, isTemporary = false, location = null, unresolvedReason = "metadata missing")
                    },
                )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns
            DiscoveredDatabase(CATALOG, "sales", DATABASE_LOCATION, tables)
        return flow
    }
}
