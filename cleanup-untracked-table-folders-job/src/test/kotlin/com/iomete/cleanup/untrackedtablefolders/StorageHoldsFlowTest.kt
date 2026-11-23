package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.ENDPOINT
import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.AuditHistoryRow
import com.iomete.cleanup.untrackedtablefolders.audit.AuditHistoryUnavailableException
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import java.time.Duration
import java.time.Instant
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
private const val DB = "s3a://bucket/lake/a.db"
private const val JOB_ID = "job-1"

class StorageHoldsFlowTest {

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        putFolder(DB)
        putFolder("$DB/orders", "part-00000-a.parquet")
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `commit scratch seen unchanged by a run 72 hours earlier is deleted, after an intent row that names it`() {
        putFolder("$DB/scratch", "_temporary/0/part-00000", "_SUCCESS")
        var intentsAtFirstDelete: List<CleanupAuditRecord>? = null

        val (first, second) =
            confirmedRuns(
                beforeSecond = { flow ->
                    InMemoryS3FileSystem.beforeDelete = { if (intentsAtFirstDelete == null) intentsAtFirstDelete = flow.intentRecords.toList() }
                },
            )

        val firstRecord = first.auditRecords.single()
        assertTrue(firstRecord.candidateFingerprints!!.getValue("$DB/scratch").startsWith("fp1:"))
        val record = second.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DB/scratch"), record.deletedFolders)
        assertEquals(firstRecord.runId, record.prevRunId)
        assertEquals(firstRecord.candidateFingerprints, record.candidateFingerprints)
        assertEquals(listOf("$DB/scratch"), intentsAtFirstDelete!!.single().candidateFolders)
        assertEquals("deletion_intent", second.intentRecords.single().statusReason)
        assertEquals(record.runId, second.intentRecords.single().runId)
        assertFalse(second.objectExists("$DB/scratch/_SUCCESS"))
    }

    @Test
    fun `a first deletion run after rows without fingerprints holds, records sightings, and the next run 72 hours later deletes`() {
        putFolder("$DB/scratch", "_temporary/0/part-00000")
        val legacy = AuditHistoryRow("legacy", "SUCCESS", Instant.now().minus(Duration.ofDays(5)), listOf("$DB/scratch"), emptyList(), null, null)

        val first = flow(dryRun = false, history = listOf(legacy)).also { it.run() }

        val firstRecord = first.auditRecords.single()
        assertHeld(first, "awaiting_confirmation", "$DB/scratch")
        assertEquals("SUCCESS", firstRecord.status)
        assertEquals("legacy", firstRecord.prevRunId)
        assertEquals(setOf("$DB/scratch"), firstRecord.candidateFingerprints!!.keys)
        assertTrue(first.intentRecords.isEmpty())

        val stored =
            AuditHistoryRow(
                firstRecord.runId,
                firstRecord.status,
                Instant.now().minus(Duration.ofHours(73)),
                firstRecord.candidateFolders,
                firstRecord.deletedFolders,
                firstRecord.candidateFingerprints,
                firstRecord.prevRunId,
            )
        val second = flow(dryRun = false, history = listOf(stored, legacy)).also { it.run() }

        assertEquals(listOf("$DB/scratch"), second.auditRecords.single().deletedFolders)
        assertEquals(firstRecord.runId, second.auditRecords.single().prevRunId)
    }

    @Test
    fun `a folder not yet confirmed is held as awaiting confirmation and no intent is written`() {
        putFolder("$DB/scratch", "_temporary/0/part-00000")

        val flow = flow(dryRun = false, history = emptyList()).also { it.run() }

        assertHeld(flow, "awaiting_confirmation", "$DB/scratch")
        assertTrue(flow.intentRecords.isEmpty())
        assertTrue(flow.objectExists("$DB/scratch/_temporary/0/part-00000"))
    }

    @Test
    fun `confirmation needs 72 hours`() {
        putFolder("$DB/scratch", "_temporary/0/part-00000")

        val (_, second) = confirmedRuns(age = Duration.ofHours(71))

        assertHeld(second, "awaiting_confirmation", "$DB/scratch")
        assertTrue(second.objectExists("$DB/scratch/_temporary/0/part-00000"))
    }

    @Test
    fun `a folder that changed since the earlier run is not confirmed`() {
        putFolder("$DB/scratch", "_temporary/0/part-00000")

        val (_, second) = confirmedRuns(beforeSecond = { InMemoryS3FileSystem.putFile(ENDPOINT, "$DB/scratch/_temporary/0/part-00001", 1) })

        assertHeld(second, "awaiting_confirmation", "$DB/scratch")
        assertTrue(second.objectExists("$DB/scratch/_temporary/0/part-00000"))
    }

    @Test
    fun `plain Spark output is held even when confirmed`() {
        putFolder("$DB/output", "part-00000-1a2b.snappy.parquet", "_SUCCESS")

        val (_, second) = confirmedRuns()

        assertHeld(second, "data_only_unknown_provenance", "$DB/output")
        assertTrue(second.objectExists("$DB/output/part-00000-1a2b.snappy.parquet"))
    }

    @Test
    fun `other data-only files and format state are held even when confirmed`() {
        putFolder("$DB/other", "part-0.parquet")
        putFolder("$DB/delta", "_delta_log/00000000000000000000.json", "part-00000-a.parquet")

        val (_, second) = confirmedRuns()

        assertHeld(second, "data_only_not_plain_output", "$DB/delta", "$DB/other")
        assertTrue(second.objectExists("$DB/other/part-0.parquet"))
        assertTrue(second.objectExists("$DB/delta/_delta_log/00000000000000000000.json"))
    }

    @Test
    fun `a dropped table's own metadata folder is deleted once confirmed`() {
        putMetadataFolder("dropped", """{"format-version":2,"table-uuid":"u1","location":"$DB/dropped"}""")

        val (_, second) = confirmedRuns()

        assertEquals(listOf("$DB/dropped"), second.auditRecords.single().deletedFolders)
        assertFalse(second.objectExists("$DB/dropped/metadata/v1.metadata.json"))
    }

    @Test
    fun `metadata of a view, without a uuid, for another location or unreadable holds the folder`() {
        putMetadataFolder("view", """{"view-uuid":"v1","format-version":1,"location":"$DB/view"}""")
        putMetadataFolder("no_uuid", """{"format-version":1,"location":"$DB/no_uuid"}""")
        putMetadataFolder("moved", """{"format-version":2,"table-uuid":"u2","location":"$DB/orders"}""")
        putMetadataFolder("broken", """{"format-version":2,""")
        putFolder("$DB/hint_only", "metadata/version-hint.text", "data/part-00000-a.parquet")
        // One own file does not clear a folder whose other metadata file is not its own.
        putMetadataFolder("mixed", """{"table-uuid":"u3","location":"$DB/mixed"}""", """{"table-uuid":"u3","location":"$DB/elsewhere"}""")

        val (_, second) = confirmedRuns()

        val record = second.auditRecords.single()
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertEquals(listOf("$DB/view"), samplePaths(record, "metadata_view"))
        assertEquals(listOf("$DB/no_uuid"), samplePaths(record, "metadata_no_uuid"))
        assertEquals(listOf("$DB/mixed", "$DB/moved"), samplePaths(record, "metadata_location_mismatch"))
        assertEquals(listOf("$DB/broken", "$DB/hint_only"), samplePaths(record, "metadata_unreadable"))
        listOf("view", "no_uuid", "moved", "broken", "mixed").forEach { assertTrue(second.objectExists("$DB/$it/metadata/v1.metadata.json"), it) }
    }

    @Test
    fun `without a job id nothing is confirmed`() {
        putFolder("$DB/scratch", "_temporary/0/part-00000")

        val (_, second) = confirmedRuns(jobId = null)

        assertHeld(second, "confirmation_no_job_id", "$DB/scratch")
        assertTrue(second.objectExists("$DB/scratch/_temporary/0/part-00000"))
    }

    @Test
    fun `unreadable history holds every folder`() {
        putFolder("$DB/scratch", "_temporary/0/part-00000")

        val flow = flow(dryRun = false, history = null).also { it.run() }

        assertHeld(flow, "confirmation_history_unreadable", "$DB/scratch")
        assertNull(flow.auditRecords.single().prevRunId)
        assertTrue(flow.objectExists("$DB/scratch/_temporary/0/part-00000"))
    }

    @Test
    fun `an intent row that does not read back blocks deletion`() {
        putFolder("$DB/scratch", "_temporary/0/part-00000")

        val (_, second) = confirmedRuns(beforeSecond = { every { it.auditTableService.intentRecorded(any(), any(), any(), any()) } returns false })

        val record = second.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("deletion_intent_not_recorded", record.statusReason)
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertTrue(second.objectExists("$DB/scratch/_temporary/0/part-00000"))
    }

    @Test
    fun `a dry run reports the holds and writes no intent`() {
        putFolder("$DB/output", "part-00000-a.parquet")
        putFolder("$DB/scratch", "_temporary/0/part-00000")

        val flow = flow(dryRun = true, history = emptyList()).also { it.run() }

        val record = flow.auditRecords.single()
        assertEquals("data_only_unknown_provenance=1\nawaiting_confirmation=1", record.diagnosticDetails["held_folder_counts"])
        assertTrue(flow.intentRecords.isEmpty())
        val lines = flow.summaryLines()
        assertTrue("FOLDERS HELD BY STORAGE CHECKS" in lines)
        assertTrue(lines.any { it.startsWith("  data_only_unknown_provenance: ") })
    }

    @Test
    fun `the storage holds delete a subset of what the job deleted before them`() {
        val folders =
            mapOf(
                "scratch" to listOf("_temporary/0/part-00000"),
                "magic" to listOf("__magic_job/x.pending", "_SUCCESS"),
                "output" to listOf("part-00000-a.parquet"),
                "other" to listOf("data.orc"),
                "partitioned" to listOf("dt=1/part-00000-a.parquet"),
                "delta" to listOf("_delta_log/0.json"),
            )
        val withHolds = deletedFolders(folders, realHolds = true)
        val before = deletedFolders(folders, realHolds = false)

        assertEquals(folders.keys.map { "$DB/$it" }.sorted(), before)
        assertEquals(listOf("$DB/magic", "$DB/scratch"), withHolds)
        assertTrue(before.containsAll(withHolds))
    }

    private fun deletedFolders(
        folders: Map<String, List<String>>,
        realHolds: Boolean,
    ): List<String> {
        InMemoryS3FileSystem.reset()
        setUp()
        folders.forEach { (name, files) -> putFolder("$DB/$name", *files.toTypedArray()) }
        val flow =
            if (realHolds) {
                confirmedRuns().second
            } else {
                flow(dryRun = false, history = emptyList(), realHolds = false).also { it.run() }
            }
        return flow.auditRecords.single().deletedFolders.sorted()
    }

    // A dry run, then a deletion run that reads the dry run's row as if it ended [age] earlier.
    private fun confirmedRuns(
        age: Duration = Duration.ofHours(73),
        jobId: String? = JOB_ID,
        beforeSecond: (CleanupFlowHarness) -> Unit = {},
    ): Pair<CleanupFlowHarness, CleanupFlowHarness> {
        val first = flow(dryRun = true, history = emptyList(), jobId = jobId).also { it.run() }
        val earlier = first.auditRecords.single()
        val second =
            flow(
                dryRun = false,
                history =
                    listOf(
                        AuditHistoryRow(
                            runId = earlier.runId,
                            status = earlier.status,
                            endTime = Instant.now().minus(age),
                            candidateFolders = earlier.candidateFolders,
                            deletedFolders = earlier.deletedFolders,
                            candidateFingerprints = earlier.candidateFingerprints,
                            prevRunId = earlier.prevRunId,
                        ),
                    ),
                jobId = jobId,
            )
        beforeSecond(second)
        second.run()
        return first to second
    }

    private fun flow(
        dryRun: Boolean,
        history: List<AuditHistoryRow>?,
        jobId: String? = JOB_ID,
        realHolds: Boolean = true,
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(catalog = CATALOG, databases = listOf("a"), dryRun = dryRun, deleteEnabled = !dryRun),
                realStorageHolds = realHolds,
            )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "a") } returns
            DiscoveredDatabase(CATALOG, "a", DB, listOf(DiscoveredTable(CATALOG, "a", "orders", isTemporary = false, location = "$DB/orders")))
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { flow.auditTableService.currentExternalJobId() } returns jobId
        if (history == null) {
            every { flow.auditTableService.readHistory(any(), any(), any(), any()) } throws
                AuditHistoryUnavailableException("could not read the audit history")
        } else {
            every { flow.auditTableService.readHistory(JOB_ID, CATALOG, "a", any()) } returns history
        }
        return flow
    }

    private fun putMetadataFolder(
        name: String,
        vararg metadata: String,
    ) {
        putFolder("$DB/$name", "data/part-00000-a.parquet")
        metadata.forEachIndexed { index, json ->
            InMemoryS3FileSystem.putFileWithContent(ENDPOINT, "$DB/$name/metadata/v${index + 1}.metadata.json", json.toByteArray())
        }
    }

    private fun samplePaths(
        record: CleanupAuditRecord,
        reason: String,
    ): List<String> = record.diagnosticDetails["${reason}_paths_sample"]?.split("\n").orEmpty()

    private fun assertHeld(
        flow: CleanupFlowHarness,
        reason: String,
        vararg paths: String,
    ) {
        val record = flow.auditRecords.single()
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertEquals(paths.toList(), samplePaths(record, reason))
        assertEquals("$reason=${paths.size}", record.diagnosticDetails["held_folder_counts"])
    }
}
