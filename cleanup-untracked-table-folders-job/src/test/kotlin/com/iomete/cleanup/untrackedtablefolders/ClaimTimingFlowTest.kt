package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.ENDPOINT
import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import com.iomete.cleanup.untrackedtablefolders.storage.OWNERSHIP_SNAPSHOT_MAX_AGE
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

private const val Q_CATALOG = "example_catalog"
private const val Q_SALES = "s3a://bucket/lake/sales.db"
private const val Q_A = "$Q_SALES/orphan_a"
private const val Q_BIG = "$Q_SALES/orphan_big"
private const val Q_Z = "$Q_SALES/orphan_z"

// The same late owner of one folder, first seen by a read again at different moments of that folder.
class ClaimTimingFlowTest {
    private val clock = TestClock()
    private val bigKeys = (0 until 30).map { "part-%02d.parquet".format(it) }

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        putFolder(Q_SALES)
        putFolder("$Q_SALES/t1", "part-0.parquet")
        putFolder(Q_BIG, *bigKeys.toTypedArray())
        putFolder(Q_Z, "part-0.parquet")
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `an owner seen at the read again before the folder keeps it, and the rest are deleted`() {
        putFolder(Q_A, "part-0.parquet")
        val flow = flow()
        InMemoryS3FileSystem.beforeDelete = { path -> if (path == "$Q_A/part-0.parquet") clock.advance(Duration.ofSeconds(250)) }

        flow.run()

        assertKeptAndContinued(flow.auditRecords.single(), deleted = listOf(Q_A, Q_Z))
    }

    @Test
    fun `an owner seen while the first folder's keys are read back keeps it, and the rest are deleted`() {
        val flow = flow()
        InMemoryS3FileSystem.beforeGetFileStatus = { path -> if (path.startsWith("$Q_BIG/")) clock.advance(Duration.ofSeconds(10)) }

        flow.run()

        assertKeptAndContinued(flow.auditRecords.single(), deleted = listOf(Q_Z))
    }

    @Test
    fun `an owner seen while a later folder's keys are read back keeps it, and the rest are deleted`() {
        putFolder(Q_A, "part-0.parquet")
        val flow = flow()
        InMemoryS3FileSystem.beforeGetFileStatus = { path -> if (path.startsWith("$Q_BIG/")) clock.advance(Duration.ofSeconds(10)) }

        flow.run()

        assertKeptAndContinued(flow.auditRecords.single(), deleted = listOf(Q_A, Q_Z))
    }

    @Test
    fun `an owner seen after the keys are read back but before the first delete keeps the folder`() {
        val flow = flow()
        // The last read-back status read ages the snapshot past the point where it is read again, which then happens
        // before the first status read of the delete loop.
        InMemoryS3FileSystem.beforeGetFileStatus = { path ->
            if (path == "$Q_BIG/${bigKeys.last()}" && InMemoryS3FileSystem.getFileStatusCalls[path] == 1) clock.advance(Duration.ofSeconds(250))
        }

        flow.run()

        assertKeptAndContinued(flow.auditRecords.single(), deleted = listOf(Q_Z))
    }

    @Test
    fun `an owner seen after some of the folder's objects were deleted stops the run, with every key deleted or remaining`() {
        val flow = flow()
        InMemoryS3FileSystem.beforeDelete = { path -> if (path == "$Q_BIG/part-04.parquet") clock.advance(Duration.ofSeconds(250)) }

        flow.run()

        val record = flow.auditRecords.single()
        val details = record.diagnosticDetails
        assertEquals("FAILED" to "deletion_stopped_at_recheck", record.status to record.statusReason, record.errorMessage)
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertEquals("claimed_during_deletion", details["deletion_stop_cause"])
        assertEquals(Q_Z, details["folder_not_attempted_paths_sample"])
        assertEquals(Q_BIG, details["partially_deleted_folder_path"])
        assertEquals("5", details["partially_deleted_folder_deleted_object_count"])
        assertEquals("25", details["partially_deleted_folder_remaining_object_count"])
        assertEquals("0", details["partially_deleted_folder_unknown_state_object_count"])
        assertEquals(bigKeys.take(5).map { "$Q_BIG/$it" }, InMemoryS3FileSystem.deleteCalls.map { it.first }.filter { it.startsWith("$Q_BIG/") })
        assertEquals(bigKeys.drop(5).map { "$Q_BIG/$it" }.toSet(), InMemoryS3FileSystem.paths(ENDPOINT).filter { it.startsWith("$Q_BIG/") }.toSet())
        assertTrue(flow.objectExists("$Q_Z/part-0.parquet"))
    }

    // Reads of this database: the service's first read, the gate's first read, then each read again. The owner of
    // orphan_big is there from the first read again on.
    private fun flow(): CleanupFlowHarness {
        var reads = 0
        return CleanupFlowHarness(
            ApplicationConfig(catalog = Q_CATALOG, databases = listOf("sales"), dryRun = false, deleteEnabled = true),
            ticker = clock,
            snapshotMaxAge = OWNERSHIP_SNAPSHOT_MAX_AGE,
        ).also {
            val t1 = DiscoveredTable(Q_CATALOG, "sales", "t1", isTemporary = false, location = "$Q_SALES/t1")
            val late = DiscoveredTable(Q_CATALOG, "sales", "late", isTemporary = false, location = Q_BIG)
            every { it.catalogDiscoveryService.discoverDatabase(Q_CATALOG, "sales") } answers {
                reads += 1
                DiscoveredDatabase(Q_CATALOG, "sales", Q_SALES, if (reads >= 3) listOf(t1, late) else listOf(t1))
            }
            every { it.catalogDiscoveryService.sharedWarehouseRoots(Q_CATALOG) } returns listOf("s3a://bucket/lake")
            every { it.catalogDiscoveryService.visibleDatabaseLocations(Q_CATALOG) } returns mapOf("sales" to Q_SALES)
        }
    }

    private fun assertKeptAndContinued(
        record: CleanupAuditRecord,
        deleted: List<String>,
    ) {
        val details = record.diagnosticDetails
        assertEquals("SUCCESS" to null, record.status to record.statusReason, record.errorMessage)
        assertEquals(deleted, record.deletedFolders)
        assertEquals(Q_BIG, details["folder_kept_at_final_check_paths_sample"], details.toString())
        assertFalse(details.containsKey("deletion_stop_cause"), details.toString())
        assertFalse(details.containsKey("partially_deleted_folder_path"), details.toString())
        assertTrue((details["ownership_refresh_count"]?.toInt() ?: 0) >= 1, details.toString())
        assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first.startsWith("$Q_BIG/") || it.first == Q_BIG })
        assertEquals(bigKeys.map { "$Q_BIG/$it" }.toSet(), InMemoryS3FileSystem.paths(ENDPOINT).filter { it.startsWith("$Q_BIG/") }.toSet())
        deleted.forEach { folder -> assertTrue(InMemoryS3FileSystem.paths(ENDPOINT).none { it.startsWith("$folder/") }, folder) }
    }
}
