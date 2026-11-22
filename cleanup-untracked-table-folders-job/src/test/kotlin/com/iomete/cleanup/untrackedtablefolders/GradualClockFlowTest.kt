package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.DELETE_START_MARGIN
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

private const val G_CATALOG = "example_catalog"
private const val G_WAREHOUSE = "s3a://bucket/lake"
private const val G_SALES = "s3a://bucket/lake/sales.db"
private const val G_ARCHIVE = "s3a://bucket/lake/archive.db"

// Time passes evenly in every delete, and in one test in every status read, instead of jumping past a limit once.
// Every test checks that no delete starts with an ownership read older than the limit minus the start margin.
class GradualClockFlowTest {
    private val clock = TestClock()
    private val readStarts = mutableListOf<Long>()
    private val deleteAges = mutableListOf<Duration>()

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        putFolder(G_SALES)
        putFolder("$G_SALES/t1", "part-0.parquet")
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `four folders at 95 s per delete are all deleted, with ownership read again on the way`() {
        val folders = folders("orphan_a", "orphan_b", "orphan_c", "orphan_d")
        val flow = flow()
        stepPerDelete(Duration.ofSeconds(95))

        flow.run()

        val record = flow.auditRecords.single()
        assertDeleted(record, folders)
        assertTrue(refreshes(record) >= 1, record.diagnosticDetails.toString())
        assertEveryDeleteWithinLimit()
    }

    @Test
    fun `ten folders at 40 s per delete are all deleted, with ownership read again on the way`() {
        val folders = folders(*(0 until 10).map { "orphan_$it" }.toTypedArray())
        val flow = flow()
        stepPerDelete(Duration.ofSeconds(40))

        flow.run()

        val record = flow.auditRecords.single()
        assertDeleted(record, folders)
        assertTrue(refreshes(record) >= 1, record.diagnosticDetails.toString())
        assertEveryDeleteWithinLimit()
    }

    @Test
    fun `a folder that takes longer than two ownership reads is deleted, each key once`() {
        val keys = (0 until 30).map { "part-%02d.parquet".format(it) }
        putFolder("$G_SALES/orphan_big", *keys.toTypedArray())
        val others = folders("orphan_z")
        val flow = flow()
        stepPerDelete(Duration.ofSeconds(20))

        flow.run()

        val record = flow.auditRecords.single()
        assertDeleted(record, listOf("$G_SALES/orphan_big") + others)
        assertTrue(refreshes(record) >= 2, record.diagnosticDetails.toString())
        val sent = InMemoryS3FileSystem.deleteCalls.map { it.first }.filter { it.startsWith("$G_SALES/orphan_big/part-") }
        assertEquals(keys.map { "$G_SALES/orphan_big/$it" }, sent)
        assertEveryDeleteWithinLimit()
    }

    @Test
    fun `time spent in status reads and deletes still lets every folder be deleted`() {
        val folders = (0 until 3).map { "orphan_$it" }.map { "$G_SALES/$it" }
        folders.forEach { folder -> putFolder(folder, *(0 until 10).map { "part-$it.parquet" }.toTypedArray()) }
        val flow = flow()
        InMemoryS3FileSystem.beforeGetFileStatus = { path -> if (path.endsWith(".parquet")) clock.advance(Duration.ofSeconds(3)) }
        stepPerDelete(Duration.ofSeconds(9))

        flow.run()

        val record = flow.auditRecords.single()
        assertDeleted(record, folders)
        assertTrue(refreshes(record) >= 1, record.diagnosticDetails.toString())
        assertEveryDeleteWithinLimit()
    }

    @Test
    fun `a read again in the middle of a folder that takes more than half the limit stops with every key deleted or remaining`() {
        val keys = (0 until 30).map { "part-%02d.parquet".format(it) }
        putFolder("$G_SALES/orphan_big", *keys.toTypedArray())
        folders("orphan_z")
        val flow = flow(slowReadAfterStart = OWNERSHIP_SNAPSHOT_MAX_AGE.dividedBy(2).plusSeconds(1))
        stepPerDelete(Duration.ofSeconds(20))

        flow.run()

        val record = flow.auditRecords.single()
        val details = record.diagnosticDetails
        assertStopped(record, "recheck_too_slow", notAttempted = listOf("$G_SALES/orphan_z"))
        assertTrue(record.errorMessage.orEmpty().contains("during the deletion of folder=$G_SALES/orphan_big"), record.errorMessage)
        assertPartial(details, "$G_SALES/orphan_big", keys)
        assertTrue(flow.objectExists("$G_SALES/orphan_z/part-0.parquet"))
        assertEveryDeleteWithinLimit()
    }

    @Test
    fun `an owner found by the read again in the middle of a folder stops there, and the rest of the folder stays`() {
        val keys = (0 until 30).map { "part-%02d.parquet".format(it) }
        putFolder("$G_SALES/orphan_big", *keys.toTypedArray())
        folders("orphan_z")
        val flow = flow(ownerAfterStart = "$G_SALES/orphan_big")
        stepPerDelete(Duration.ofSeconds(20))

        flow.run()

        val record = flow.auditRecords.single()
        val details = record.diagnosticDetails
        assertStopped(record, "claimed_during_deletion", notAttempted = listOf("$G_SALES/orphan_z"))
        assertPartial(details, "$G_SALES/orphan_big", keys)
        assertTrue(flow.objectExists("$G_SALES/orphan_z/part-0.parquet"))
        assertEveryDeleteWithinLimit()
    }

    @Test
    fun `a read again that fails in the middle of a folder stops with every key deleted or remaining`() {
        val keys = (0 until 30).map { "part-%02d.parquet".format(it) }
        putFolder("$G_SALES/orphan_big", *keys.toTypedArray())
        folders("orphan_z")
        val flow = flow(failAfterStart = true)
        stepPerDelete(Duration.ofSeconds(20))

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "own_database_recheck_failed", notAttempted = listOf("$G_SALES/orphan_z"))
        assertPartial(record.diagnosticDetails, "$G_SALES/orphan_big", keys)
        assertEveryDeleteWithinLimit()
    }

    private fun folders(vararg names: String): List<String> =
        names.map { "$G_SALES/$it" }.onEach { putFolder(it, "part-0.parquet") }

    // A step per object delete; every delete, directories too, records how old the last ownership read is.
    private fun stepPerDelete(step: Duration) {
        InMemoryS3FileSystem.beforeDelete = { path ->
            deleteAges += Duration.ofNanos(clock.nanos() - readStarts.last())
            if (path.endsWith(".parquet")) clock.advance(step)
        }
    }

    // Reads of this database after the clock started moving are reads again during deletion.
    private fun flow(
        slowReadAfterStart: Duration? = null,
        ownerAfterStart: String? = null,
        failAfterStart: Boolean = false,
    ): CleanupFlowHarness =
        CleanupFlowHarness(
            ApplicationConfig(catalog = G_CATALOG, databases = listOf("sales"), dryRun = false, deleteEnabled = true),
            ticker = clock,
            snapshotMaxAge = OWNERSHIP_SNAPSHOT_MAX_AGE,
        ).also {
            val t1 = DiscoveredTable(G_CATALOG, "sales", "t1", isTemporary = false, location = "$G_SALES/t1")
            every { it.catalogDiscoveryService.discoverDatabase(G_CATALOG, "sales") } answers {
                readStarts += clock.nanos()
                val again = clock.nanos() > 0
                if (again && failAfterStart) throw IllegalStateException("metastore unavailable")
                if (again && slowReadAfterStart != null) clock.advance(slowReadAfterStart)
                val owner = ownerAfterStart?.takeIf { again }?.let { DiscoveredTable(G_CATALOG, "sales", "late", isTemporary = false, location = it) }
                DiscoveredDatabase(G_CATALOG, "sales", G_SALES, listOfNotNull(t1, owner))
            }
            every { it.catalogDiscoveryService.sharedWarehouseRoots(G_CATALOG) } returns listOf(G_WAREHOUSE)
            every { it.catalogDiscoveryService.visibleDatabaseLocations(G_CATALOG) } returns mapOf("sales" to G_SALES, "archive" to G_ARCHIVE)
        }

    private fun refreshes(record: CleanupAuditRecord): Int = record.diagnosticDetails["ownership_refresh_count"]?.toInt() ?: 0

    private fun assertDeleted(
        record: CleanupAuditRecord,
        folders: List<String>,
    ) {
        assertEquals("SUCCESS" to null, record.status to record.statusReason, record.errorMessage)
        assertEquals(folders.sorted(), record.deletedFolders)
        assertFalse(record.diagnosticDetails.containsKey("deletion_stop_cause"), record.diagnosticDetails.toString())
        folders.forEach { folder -> assertTrue(InMemoryS3FileSystem.paths(G_ENDPOINT).none { it.startsWith("$folder/") }, folder) }
    }

    private fun assertStopped(
        record: CleanupAuditRecord,
        cause: String,
        notAttempted: List<String>,
    ) {
        assertEquals("FAILED" to "deletion_stopped_at_recheck", record.status to record.statusReason, record.errorMessage)
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertEquals(cause, record.diagnosticDetails["deletion_stop_cause"])
        assertEquals(notAttempted.joinToString("\n"), record.diagnosticDetails["folder_not_attempted_paths_sample"])
    }

    // The folder that was being deleted: what the row says was deleted is gone, what it says remains is still there.
    private fun assertPartial(
        details: Map<String, String>,
        folder: String,
        keys: List<String>,
    ) {
        assertEquals(folder, details["partially_deleted_folder_path"])
        assertEquals(keys.size.toString(), details["partially_deleted_folder_listed_object_count"])
        assertEquals("0", details["partially_deleted_folder_unknown_state_object_count"], details.toString())
        val deleted = details.getValue("partially_deleted_folder_deleted_object_count").toInt()
        val remaining = details.getValue("partially_deleted_folder_remaining_object_count").toInt()
        assertEquals(keys.size, deleted + remaining)
        assertTrue(deleted in 1 until keys.size, "deleted=$deleted")
        val sent = InMemoryS3FileSystem.deleteCalls.map { it.first }.filter { it.startsWith("$folder/") }
        assertEquals(keys.take(deleted).map { "$folder/$it" }, sent)
        assertEquals(keys.drop(deleted).map { "$folder/$it" }.toSet(), InMemoryS3FileSystem.paths(G_ENDPOINT).filter { it.startsWith("$folder/") }.toSet())
    }

    private fun assertEveryDeleteWithinLimit() {
        val limit = OWNERSHIP_SNAPSHOT_MAX_AGE.minus(DELETE_START_MARGIN)
        assertTrue(deleteAges.isNotEmpty())
        assertTrue(deleteAges.all { it <= limit }, "ages at each delete in seconds: ${deleteAges.map { it.seconds }}")
    }

    private companion object {
        val G_ENDPOINT: String = CleanupFlowHarness.ENDPOINT
    }
}
