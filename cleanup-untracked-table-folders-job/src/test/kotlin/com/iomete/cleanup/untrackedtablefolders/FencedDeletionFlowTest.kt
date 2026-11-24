package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.ENDPOINT
import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.DatabaseNotFoundException
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.catalog.NamedTableStorage
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.Blocker
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import com.iomete.cleanup.untrackedtablefolders.storage.OWNERSHIP_SNAPSHOT_MAX_AGE
import com.iomete.cleanup.untrackedtablefolders.storage.Ticker
import com.iomete.cleanup.untrackedtablefolders.storage.awaitUntil
import com.iomete.cleanup.untrackedtablefolders.storage.elapsedSince
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
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException
import java.time.Duration
import java.util.concurrent.atomic.AtomicLong

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val SALES = "s3a://bucket/lake/sales.db"
private const val ARCHIVE = "s3a://bucket/lake/archive.db"
private const val ORPHAN_A = "$SALES/orphan_a"
private const val ORPHAN_B = "$SALES/orphan_b"
private const val ORPHAN_C = "$SALES/orphan_c"

class FencedDeletionFlowTest {

    private val clock = TestClock()

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        putFolder(SALES)
        putFolder("$SALES/t1", "part-0.parquet")
    }

    @AfterEach
    fun tearDown() {
        val recursive = InMemoryS3FileSystem.deleteCalls.filter { it.second }
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
        assertEquals(emptyList<Pair<String, Boolean>>(), recursive, "no delete may be recursive")
    }

    @Test
    fun `a file added between inspection and deletion keeps the folder as changed, and the row keeps its normal status`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        changeAfterInspection(ORPHAN_A) { InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/late.parquet", length = 1) }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS" to null, record.status to record.statusReason)
        assertEquals(listOf(ORPHAN_A), record.candidateFolders)
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertKept(record, "changed_since_inspection", ORPHAN_A)
        assertTrue(flow.objectExists("$ORPHAN_A/part-0.parquet") && flow.objectExists("$ORPHAN_A/late.parquet"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
        val lines = flow.summaryLines()
        assertTrue("  Folders changed since inspection                 1" in lines, lines.joinToString("\n"))
        assertTrue("- $ORPHAN_A" in lines)
    }

    @Test
    fun `a file removed between inspection and deletion keeps the folder as changed while other folders are deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        changeAfterInspection(ORPHAN_A) { InMemoryS3FileSystem.store(ENDPOINT).remove("$ORPHAN_A/part-1.parquet") }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf(ORPHAN_B), record.deletedFolders)
        assertKept(record, "changed_since_inspection", ORPHAN_A)
        assertTrue(flow.objectExists("$ORPHAN_A/part-0.parquet"))
        assertFalse(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `a file rewritten with the same length and time but a new ETag keeps the folder as changed`() {
        putFolder(ORPHAN_A)
        InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/part-0.parquet", length = 1, modificationTimeMillis = 1L, etag = "etag-1")
        changeAfterInspection(ORPHAN_A) {
            InMemoryS3FileSystem.overwriteFile(ENDPOINT, "$ORPHAN_A/part-0.parquet", length = 1, modificationTimeMillis = 1L, etag = "etag-2")
        }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertKept(record, "changed_since_inspection", ORPHAN_A)
        assertTrue(flow.objectExists("$ORPHAN_A/part-0.parquet"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a key written after the deletion listing survives and the row reports the partly deleted folder`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.afterListFiles = { root, call ->
            if (root == ORPHAN_A && call == 2) InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/late.parquet", length = 1)
        }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED" to "deletion_failed", record.status to record.statusReason)
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertEquals(ORPHAN_A, record.diagnosticDetails["deletion_failed_folder_path"])
        assertEquals(ORPHAN_A, record.diagnosticDetails["partially_deleted_folder_path"])
        // The listed key is deleted; the key written after the listing is unlisted, not remaining.
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), unlisted = listOf("late.parquet"))
        assertEquals("1", record.diagnosticDetails["folder_not_attempted_count"])
        assertEquals(ORPHAN_B, record.diagnosticDetails["folder_not_attempted_paths_sample"])
        assertTrue(flow.objectExists("$ORPHAN_A/late.parquet"))
        assertFalse(flow.objectExists("$ORPHAN_A/part-0.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `a table that claims the second folder once the snapshot is older than the limit keeps it, and the first is deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val flow = flow()
        // First read, final check, then the read again before the second folder.
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returnsMany
            listOf(emptyList(), emptyList(), listOf(archiveTable(ORPHAN_B)))
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf(ORPHAN_A), record.deletedFolders)
        assertKept(record, "at_final_check", ORPHAN_B)
        assertEquals("1", record.diagnosticDetails["ownership_refresh_count"])
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
        verify(exactly = 3) { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") }
        verify(exactly = 3) { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") }
        assertTrue("  Ownership read again during deletion             1" in flow.summaryLines(), flow.summaryLines().joinToString("\n"))
    }

    @Test
    fun `within the limit the gate reads ownership once for all folders`() {
        listOf(ORPHAN_A, ORPHAN_B, ORPHAN_C).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf(ORPHAN_A, ORPHAN_B, ORPHAN_C), record.deletedFolders)
        assertNull(record.diagnosticDetails["ownership_refresh_count"])
        // The first call is the service's own first read; the second is the gate's, shared by every folder.
        verify(exactly = 2) { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") }
        verify(exactly = 2) { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) }
        verify(exactly = 2) { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") }
    }

    @Test
    fun `a failed read of other storage after a deletion stops the rest and keeps the deletion on the row with the new reason`() {
        listOf(ORPHAN_A, ORPHAN_B, ORPHAN_C).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returns emptyList() andThen emptyList() andThenThrows
            IllegalStateException("catalog unreachable")
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "other_storage_incomplete", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B, ORPHAN_C))
        assertTrue(record.errorMessage.orEmpty().startsWith("Deletion stopped at a safety recheck after 1 deleted folder(s): "), record.errorMessage)
        assertTrue(record.diagnosticDetails.getValue("ownership_unreadable_sample").contains("archive"))
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet") && flow.objectExists("$ORPHAN_C/part-0.parquet"))
    }

    @Test
    fun `a table of this database that becomes unresolved after a deletion stops the rest`() {
        listOf(ORPHAN_A, ORPHAN_B).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        val unresolved = DiscoveredTable(CATALOG, "sales", "broken", isTemporary = false, location = null, unresolvedReason = "metadata missing")
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returnsMany
            listOf(salesDatabase(), salesDatabase(), salesDatabase(extra = listOf(unresolved)))
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        assertStopped(flow.auditRecords.single(), "own_tables_unresolved", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B))
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `a read again that takes longer than the limit stops the rest`() {
        listOf(ORPHAN_A, ORPHAN_B).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        var archiveReads = 0
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } answers {
            if (++archiveReads == 3) clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
            emptyList()
        }
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "recheck_too_slow", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B))
        assertTrue(record.errorMessage.orEmpty().contains("more than the 150 s a read may use of the 300 s limit"), record.errorMessage)
    }

    @Test
    fun `a snapshot that grows too old in the middle of a folder stops before its next object and reports it as partly deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        expireSnapshotWhileDeleting("$ORPHAN_A/part-0.parquet")
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "snapshot_expired_during_deletion", deleted = emptyList(), notAttempted = listOf(ORPHAN_B))
        assertEquals(ORPHAN_A, record.diagnosticDetails["partially_deleted_folder_path"])
        assertEquals("1", record.diagnosticDetails["partially_deleted_folder_deleted_object_count"])
        assertEquals("part-1.parquet", record.diagnosticDetails["partially_deleted_folder_remaining_keys_sample"])
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-1.parquet"))
        assertTrue(record.errorMessage.orEmpty().contains("with folder $ORPHAN_A partly deleted"), record.errorMessage)
        assertTrue(flow.objectExists("$ORPHAN_A/part-1.parquet") && flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `a failed read again before anything is deleted blocks the database with the existing reason`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val flow = flow()
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returns emptyList() andThen emptyList() andThenThrows
            IllegalStateException("catalog unreachable")
        InMemoryS3FileSystem.afterListFiles = { root, call ->
            if (root == ORPHAN_A && call == 1) InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/late.parquet", length = 1)
            if (root == ORPHAN_A && call == 2) clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
        }

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED" to "other_database_storage_unknown", record.status to record.statusReason)
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertKept(record, "changed_since_inspection", ORPHAN_A)
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a database that disappears at a read again before anything is deleted is skipped as not found`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val flow = flow()
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns salesDatabase() andThen salesDatabase() andThenThrows
            DatabaseNotFoundException(CATALOG, "sales", RuntimeException("dropped"))
        InMemoryS3FileSystem.afterListFiles = { root, call ->
            if (root == ORPHAN_A && call == 1) InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/late.parquet", length = 1)
            if (root == ORPHAN_A && call == 2) clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
        }

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SKIPPED" to "database_not_found", record.status to record.statusReason)
        assertTrue("final_recheck_before_deletion" in record.diagnosticDetails.getValue("ownership_checks").split(","))
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a table created in the folder under its name since the ownership read keeps it`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val flow = flow()
        every { flow.catalogDiscoveryService.tableStorageIfExists(CATALOG, "sales", "orphan_a") } returns NamedTableStorage.Found(ORPHAN_A)
        every { flow.catalogDiscoveryService.tableStorageIfExists(CATALOG, "sales", "orphan_b") } returns
            NamedTableStorage.Found("s3a://bucket/elsewhere/orphan_b")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf(ORPHAN_B), record.deletedFolders)
        assertKept(record, "table_named_like_folder", ORPHAN_A)
        assertTrue(flow.objectExists("$ORPHAN_A/part-0.parquet"))
    }

    @Test
    fun `deletion mode lists each candidate twice and a dry run lists it once`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        flow(dryRun = true).run()
        assertEquals(1, InMemoryS3FileSystem.listFilesCalls[ORPHAN_A])
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())

        InMemoryS3FileSystem.listFilesCalls.clear()
        flow().run()
        assertEquals(2, InMemoryS3FileSystem.listFilesCalls[ORPHAN_A])
        assertFalse(InMemoryS3FileSystem.paths(ENDPOINT).any { it.startsWith(ORPHAN_A) })
    }

    @Test
    fun `a candidate folder is deleted key by key and never with a recursive delete`() {
        putFolder(ORPHAN_A, "part-0.parquet", "sub/part-1.parquet")
        val flow = flow()

        flow.run()

        assertEquals(listOf(ORPHAN_A), flow.auditRecords.single().deletedFolders)
        val candidateDeletes = InMemoryS3FileSystem.deleteCalls.filter { it.first == ORPHAN_A || it.first.startsWith("$ORPHAN_A/") }
        assertTrue(candidateDeletes.isNotEmpty())
        assertTrue(candidateDeletes.none { it.second }, candidateDeletes.toString())
        assertEquals(listOf("$ORPHAN_A/part-0.parquet", "$ORPHAN_A/sub/part-1.parquet"), candidateDeletes.take(2).map { it.first })
    }

    @Test
    fun `a snapshot that grows too old during the status read right before a delete sends no delete`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        // The first read of the key is the check of every key before the first delete; the second is right before its delete.
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == "$ORPHAN_B/part-0.parquet" && InMemoryS3FileSystem.getFileStatusCalls[target] == 2) {
                clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
            }
        }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "snapshot_expired_during_deletion", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B))
        assertFalse(record.diagnosticDetails.containsKey("partially_deleted_folder_path"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first.startsWith("$ORPHAN_B/") }, InMemoryS3FileSystem.deleteCalls.toString())
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `a folder with a stored key that its listed name does not address is held whole and the other folders are deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet", "x//part-1.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS" to null, record.status to record.statusReason)
        assertEquals(listOf(ORPHAN_B), record.deletedFolders)
        assertKept(record, "key_not_addressable", ORPHAN_A)
        assertFalse(record.diagnosticDetails.containsKey("partially_deleted_folder_path"))
        assertTrue(flow.objectExists("$ORPHAN_A/part-0.parquet") && flow.objectExists("$ORPHAN_A/x//part-1.parquet"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first == ORPHAN_A || it.first.startsWith("$ORPHAN_A/") })
        val lines = flow.summaryLines()
        assertTrue("  Folders with a key not found by its name         1" in lines, lines.joinToString("\n"))
        assertTrue(
            "  folder_kept_key_not_addressable: before the first delete, a listed object was not found by its own name, or two had one name." in lines,
            lines.joinToString("\n"),
        )
    }

    @Test
    fun `an object gone between the listing before deletion and the first delete holds the folder and the next folder is deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.afterListFiles = { root, call ->
            if (root == ORPHAN_A && call == 2) InMemoryS3FileSystem.store(ENDPOINT).remove("$ORPHAN_A/part-1.parquet")
        }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS" to null, record.status to record.statusReason)
        assertEquals(listOf(ORPHAN_B), record.deletedFolders)
        assertKept(record, "key_not_addressable", ORPHAN_A)
        assertTrue(flow.objectExists("$ORPHAN_A/part-0.parquet"))
    }

    @Test
    fun `a delete that throws on the first object puts the folder on the row with that object unknown and stops the database`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.failDeleteFor = "$ORPHAN_A/part-0.parquet"
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        assertPartialKeys(record, remaining = listOf("part-1.parquet"), unknown = listOf("part-0.parquet"))
        assertTrue(record.errorMessage.orEmpty().contains("1 in an unknown state (a delete may still complete)"), record.errorMessage)
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a delete that throws on a later object reports the deleted, unknown and remaining keys apart`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet", "part-2.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.failDeleteFor = "$ORPHAN_A/part-1.parquet"
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-2.parquet"), unknown = listOf("part-1.parquet"))
        assertEquals(listOf("$ORPHAN_A/part-0.parquet", "$ORPHAN_A/part-1.parquet").map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a refused delete reports that object as remaining, not unknown`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet", "part-2.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = "$ORPHAN_A/part-1.parquet"
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-1.parquet", "part-2.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_A/part-1.parquet") && flow.objectExists("$ORPHAN_A/part-2.parquet"))
    }

    @Test
    fun `an object gone right before its delete puts the folder on the row as vanished even when nothing was deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == "$ORPHAN_A/part-0.parquet" && InMemoryS3FileSystem.getFileStatusCalls[target] == 2) InMemoryS3FileSystem.store(ENDPOINT).remove(target)
        }
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        assertPartialKeys(record, remaining = listOf("part-1.parquet"), vanished = listOf("part-0.parquet"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `an object overwritten after the check of every key is kept and reported as remaining`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet", "part-2.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "$ORPHAN_A/part-0.parquet") InMemoryS3FileSystem.overwriteFile(ENDPOINT, "$ORPHAN_A/part-1.parquet", 2, 1L)
        }
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-1.parquet", "part-2.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_A/part-1.parquet"))
    }

    @Test
    fun `a status read that fails after a delete reports the later objects as remaining`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet", "part-2.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "$ORPHAN_A/part-0.parquet") InMemoryS3FileSystem.statusFailures["$ORPHAN_A/part-1.parquet"] = IOException("simulated status failure")
        }
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-1.parquet", "part-2.parquet"))
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    @Timeout(value = 20, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a delete that does not return within the limit is reported unknown, never sent again, and may still complete`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val blocker = Blocker()
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == "$ORPHAN_A/part-0.parquet") blocker.block() }
        val flow = flow(ticker = Ticker.SYSTEM, snapshotMaxAge = Duration.ofMillis(500))

        try {
            val started = System.nanoTime()
            flow.run()
            val elapsed = elapsedSince(started)
            assertTrue(elapsed < Duration.ofSeconds(10), elapsed.toString())

            val record = assertDeletionFailed(flow)
            assertPartialKeys(record, remaining = listOf("part-1.parquet"), unknown = listOf("part-0.parquet"))
            assertTrue(record.errorMessage.orEmpty().contains("1 in an unknown state (a delete may still complete)"), record.errorMessage)
            assertTrue(flow.objectExists("$ORPHAN_A/part-0.parquet"))
        } finally {
            blocker.release()
        }
        assertTrue(awaitUntil(Duration.ofSeconds(5)) { !flow.objectExists("$ORPHAN_A/part-0.parquet") })
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a failed read of this database at the read again after a deletion stops the rest`() {
        listOf(ORPHAN_A, ORPHAN_B).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns salesDatabase() andThen salesDatabase() andThenThrows
            IllegalStateException("catalog unreachable")
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "own_database_recheck_failed", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B))
        assertNoPartialFolder(record)
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `an unresolved table of another database at the read again after a deletion stops the rest`() {
        listOf(ORPHAN_A, ORPHAN_B).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        val unresolved = DiscoveredTable(CATALOG, "archive", "broken", isTemporary = false, location = null, unresolvedReason = "metadata missing")
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returnsMany listOf(emptyList(), emptyList(), listOf(unresolved))
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "other_storage_incomplete", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B))
        assertNoPartialFolder(record)
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `another database whose location can't be placed in a bucket at the read again after a deletion stops the rest`() {
        listOf(ORPHAN_A, ORPHAN_B).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        every { flow.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returnsMany
            listOf(mapOf("sales" to SALES, "archive" to ARCHIVE), mapOf("sales" to SALES, "archive" to ARCHIVE), mapOf("sales" to SALES, "archive" to "lake/archive.db"))
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "other_storage_incomplete", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B))
        assertNoPartialFolder(record)
        assertTrue(record.diagnosticDetails.getValue("ownership_unreadable_sample").contains("no scheme and bucket"), record.diagnosticDetails.toString())
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `a table named like the folder whose location has no scheme and bucket stops the rest after a deletion`() {
        listOf(ORPHAN_A, ORPHAN_B).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        every { flow.catalogDiscoveryService.tableStorageIfExists(CATALOG, "sales", "orphan_b") } returns NamedTableStorage.Found("/lake/sales.db/orphan_b")

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "name_probe_failed", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B))
        assertNoPartialFolder(record)
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `an owner found at the read again after a deletion keeps only its folder, and later folders are still deleted`() {
        listOf(ORPHAN_A, ORPHAN_B, ORPHAN_C).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } returnsMany
            listOf(emptyList(), emptyList(), listOf(archiveTable(ORPHAN_B)))
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS" to null, record.status to record.statusReason)
        assertEquals(listOf(ORPHAN_A, ORPHAN_C), record.deletedFolders)
        assertKept(record, "at_final_check", ORPHAN_B)
        assertNoPartialFolder(record)
        assertFalse(record.diagnosticDetails.containsKey("folder_not_attempted_count"))
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
        assertFalse(flow.objectExists("$ORPHAN_C/part-0.parquet"))
    }

    @Test
    fun `a table of this database with a path without scheme and bucket in a folder at the read again keeps that folder`() {
        listOf(ORPHAN_A, ORPHAN_B, ORPHAN_C).forEach { putFolder(it, "part-0.parquet") }
        val flow = flow()
        val bare = DiscoveredTable(CATALOG, "sales", "table-2", isTemporary = false, location = null, unqualifiedPaths = listOf("/lake/sales.db/orphan_b"))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returnsMany
            listOf(salesDatabase(), salesDatabase(), salesDatabase(extra = listOf(bare)))
        expireSnapshotWhileDeleting(ORPHAN_A)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf(ORPHAN_A, ORPHAN_C), record.deletedFolders)
        assertKept(record, "at_final_check", ORPHAN_B)
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a read again after a deletion that never returns stops the rest within the limit`() {
        listOf(ORPHAN_A, ORPHAN_B, ORPHAN_C).forEach { putFolder(it, "part-0.parquet") }
        val blocker = Blocker()
        val limit = Duration.ofSeconds(2)
        val skew = AtomicLong()
        val flow = flow(ticker = { System.nanoTime() + skew.get() }, snapshotMaxAge = limit)
        var archiveReads = 0
        every { flow.catalogDiscoveryService.discoverTables(CATALOG, "archive") } answers {
            if (++archiveReads == 3) blocker.block()
            emptyList()
        }
        // Each wait ends when the snapshot reaches the limit, so only a jump of the clock inside the first folder's last
        // delete makes the snapshot too old at the second folder with every call done.
        InMemoryS3FileSystem.beforeDelete = { path -> if (path == ORPHAN_A) skew.addAndGet(limit.toNanos()) }

        try {
            val started = System.nanoTime()
            flow.run()
            assertTrue(elapsedSince(started) < Duration.ofSeconds(15), elapsedSince(started).toString())

            val record = flow.auditRecords.single()
            assertStopped(record, "recheck_too_slow", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_B, ORPHAN_C))
            assertNoPartialFolder(record)
            assertTrue(record.errorMessage.orEmpty().contains("did not finish within the 1 s a read may use of the 2 s limit"), record.errorMessage)
            assertEquals(1, blocker.entered.get())
        } finally {
            blocker.release()
        }
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet") && flow.objectExists("$ORPHAN_C/part-0.parquet"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first.startsWith("$ORPHAN_B/") || it.first.startsWith("$ORPHAN_C/") })
    }

    @Test
    fun `a snapshot that grows too old in the second folder keeps the first as deleted, the second partly deleted and the third not attempted`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_C, "part-0.parquet")
        expireSnapshotWhileDeleting("$ORPHAN_B/part-0.parquet")
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertStopped(record, "snapshot_expired_during_deletion", deleted = listOf(ORPHAN_A), notAttempted = listOf(ORPHAN_C))
        assertEquals(ORPHAN_B, record.diagnosticDetails["partially_deleted_folder_path"])
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-1.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_B/part-1.parquet") && flow.objectExists("$ORPHAN_C/part-0.parquet"))
    }

    @Test
    fun `a delete that throws in the second folder keeps the first as deleted and the second partly deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet", "part-1.parquet", "part-2.parquet")
        putFolder(ORPHAN_C, "part-0.parquet")
        InMemoryS3FileSystem.failDeleteFor = "$ORPHAN_B/part-1.parquet"
        val flow = flow()

        flow.run()

        val record = assertSecondFolderFailed(flow)
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-2.parquet"), unknown = listOf("part-1.parquet"))
    }

    @Test
    fun `a key changed after the check of every key in the second folder is remaining, and the first stays deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_C, "part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "$ORPHAN_B/part-0.parquet") InMemoryS3FileSystem.overwriteFile(ENDPOINT, "$ORPHAN_B/part-1.parquet", 2, 1L)
        }
        val flow = flow()

        flow.run()

        val record = assertSecondFolderFailed(flow)
        assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-1.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_B/part-1.parquet"))
    }

    @Test
    @Timeout(value = 20, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a delete in the second folder that does not return within the limit is unknown, and the first stays deleted`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_C, "part-0.parquet")
        val blocker = Blocker()
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == "$ORPHAN_B/part-0.parquet") blocker.block() }
        val flow = flow(ticker = Ticker.SYSTEM, snapshotMaxAge = Duration.ofMillis(500))

        try {
            flow.run()

            val record = assertSecondFolderFailed(flow)
            assertPartialKeys(record, remaining = listOf("part-1.parquet"), unknown = listOf("part-0.parquet"))
            assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
        } finally {
            blocker.release()
        }
        assertEquals(1, InMemoryS3FileSystem.deleteCalls.count { it.first == "$ORPHAN_B/part-0.parquet" })
    }

    @Test
    fun `a snapshot that grows too old during the last delete leaves the folder partly deleted, so a new object there is not hidden`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.afterListFiles = { root, call ->
            if (root == ORPHAN_A && call == 2) InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/new.parquet", length = 1)
        }
        InMemoryS3FileSystem.beforeDelete = { path -> if (path == "$ORPHAN_A/part-0.parquet") clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1)) }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        val details = record.diagnosticDetails
        assertStopped(record, "snapshot_expired_during_deletion", deleted = emptyList(), notAttempted = listOf(ORPHAN_B))
        assertEquals(ORPHAN_A, details["partially_deleted_folder_path"])
        assertPartialKeys(record, deleted = listOf("part-0.parquet"))
        assertEquals("not_finished", details["partially_deleted_folder_directory_cleanup"])
        assertEquals(ORPHAN_A, details["partially_deleted_folder_kept_directory_paths_sample"])
        assertNull(details["partially_deleted_folder_unknown_state_directory_count"])
        assertTrue(record.errorMessage.orEmpty().contains("with folder $ORPHAN_A partly deleted"), record.errorMessage)
        assertTrue(flow.objectExists("$ORPHAN_A/new.parquet") && flow.objectExists(ORPHAN_A))
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    @Timeout(value = 20, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a directory delete that does not return within the limit leaves the folder partly deleted with that directory unknown`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val blocker = Blocker()
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == ORPHAN_A) blocker.block() }
        val flow = flow(ticker = Ticker.SYSTEM, snapshotMaxAge = Duration.ofMillis(500))

        try {
            val started = System.nanoTime()
            flow.run()
            assertTrue(elapsedSince(started) < Duration.ofSeconds(10), elapsedSince(started).toString())

            val record = assertDeletionFailed(flow)
            val details = record.diagnosticDetails
            assertPartialKeys(record, deleted = listOf("part-0.parquet"))
            assertEquals("not_finished", details["partially_deleted_folder_directory_cleanup"])
            assertEquals("1", details["partially_deleted_folder_unknown_state_directory_count"])
            assertEquals(ORPHAN_A, details["partially_deleted_folder_unknown_state_directory_paths_sample"])
            assertNull(details["partially_deleted_folder_kept_directory_count"])
            assertTrue(record.errorMessage.orEmpty().contains("1 directory delete(s) may still complete"), record.errorMessage)
            assertTrue(flow.objectExists(ORPHAN_A))
        } finally {
            blocker.release()
        }
        assertTrue(awaitUntil(Duration.ofSeconds(5)) { !flow.objectExists(ORPHAN_A) })
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false, ORPHAN_A to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    @Timeout(value = 20, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a status read before a later delete that does not return within the limit stops with the folder partly deleted on the row`() {
        putFolder(ORPHAN_A, "part-0.parquet", "part-1.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val blocker = Blocker()
        // The first read of the key is the check of every key before the first delete; the second is right before its delete.
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == "$ORPHAN_A/part-1.parquet" && InMemoryS3FileSystem.getFileStatusCalls[target] == 2) blocker.block()
        }
        val flow = flow(ticker = Ticker.SYSTEM, snapshotMaxAge = Duration.ofMillis(500))

        try {
            flow.run()

            val record = flow.auditRecords.single()
            assertStopped(record, "read_timed_out", deleted = emptyList(), notAttempted = listOf(ORPHAN_B))
            assertEquals(ORPHAN_A, record.diagnosticDetails["partially_deleted_folder_path"])
            assertPartialKeys(record, deleted = listOf("part-0.parquet"), remaining = listOf("part-1.parquet"))
            assertEquals("not_run", record.diagnosticDetails["partially_deleted_folder_directory_cleanup"])
            assertTrue(flow.objectExists("$ORPHAN_A/part-1.parquet") && flow.objectExists("$ORPHAN_B/part-0.parquet"))
        } finally {
            blocker.release()
        }
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a deleted folder whose own directory delete is refused stays deleted, and the row names that directory`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = ORPHAN_A
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS" to null, record.status to record.statusReason, record.diagnosticDetails.toString())
        assertEquals(listOf(ORPHAN_A, ORPHAN_B), record.deletedFolders)
        assertEquals("1", record.diagnosticDetails["deleted_folder_kept_directory_count"])
        assertEquals(ORPHAN_A, record.diagnosticDetails["deleted_folder_kept_directory_paths_sample"])
        assertNoPartialFolder(record)
        assertTrue(flow.objectExists(ORPHAN_A))
        assertFalse(flow.objectExists("$ORPHAN_A/part-0.parquet"))
    }

    @Test
    fun `a deleted folder with a kept directory is still named on the row when a later folder fails`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = ORPHAN_A
        InMemoryS3FileSystem.failDeleteFor = "$ORPHAN_B/part-0.parquet"
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED" to "deletion_failed", record.status to record.statusReason, record.diagnosticDetails.toString())
        assertEquals(listOf(ORPHAN_A), record.deletedFolders)
        assertEquals("1", record.diagnosticDetails["deleted_folder_kept_directory_count"])
        assertEquals(ORPHAN_A, record.diagnosticDetails["deleted_folder_kept_directory_paths_sample"])
        assertEquals(ORPHAN_B, record.diagnosticDetails["partially_deleted_folder_path"])
    }

    @Test
    fun `a folder kept before a later delete throws is reported as kept, not as not found at deletion`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        changeAfterInspection(ORPHAN_A) { InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/late.parquet", length = 1) }
        InMemoryS3FileSystem.failDeleteFor = "$ORPHAN_B/part-0.parquet"
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED" to "deletion_failed", record.status to record.statusReason, record.diagnosticDetails.toString())
        assertKept(record, "changed_since_inspection", ORPHAN_A)
        assertTrue(record.diagnosticDetails.keys.none { it.startsWith("folder_not_found_at_deletion") }, record.diagnosticDetails.toString())
        assertEquals(ORPHAN_B, record.diagnosticDetails["partially_deleted_folder_path"])
        assertPartialKeys(record, unknown = listOf("part-0.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_A/part-0.parquet") && flow.objectExists("$ORPHAN_A/late.parquet"))
    }

    // Real time. Most of each object's time is in its delete, so without the start margin the limit would usually
    // fall inside a delete, and that object would end in an unknown state. Each delete here takes far less than the
    // margin, so the folder stops with every listed key deleted or remaining.
    @Test
    @Timeout(value = 20, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a folder that takes longer than one ownership read is read again in its middle and deleted, each key once`() {
        val keys = (0 until 60).map { "part-%02d.parquet".format(it) }
        putFolder(ORPHAN_A, *keys.toTypedArray())
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.beforeGetFileStatus = { target -> if (target.startsWith("$ORPHAN_A/")) Thread.sleep(2) }
        InMemoryS3FileSystem.beforeDelete = { target -> if (target.startsWith("$ORPHAN_A/part-")) Thread.sleep(30) }
        val flow = flow(ticker = Ticker.SYSTEM, snapshotMaxAge = Duration.ofMillis(1500))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS" to null, record.status to record.statusReason, record.errorMessage)
        assertEquals(listOf(ORPHAN_A, ORPHAN_B), record.deletedFolders)
        assertTrue(record.diagnosticDetails.getValue("ownership_refresh_count").toInt() >= 1, record.diagnosticDetails.toString())
        assertNoPartialFolder(record)
        val sent = InMemoryS3FileSystem.deleteCalls.map { it.first }.filter { it.startsWith("$ORPHAN_A/part-") }
        assertEquals(keys.map { "$ORPHAN_A/$it" }, sent, "every key was sent once, in order")
        assertFalse(flow.objectExists("$ORPHAN_B/part-0.parquet"))
    }

    // An unlisted object makes the folder changed, which is a failure. The stop that ended the removal of empty
    // directories is not a fence stop on the row, but the error message names it.
    @Test
    fun `a snapshot that grows too old after an unlisted object was found names both on a deletion_failed row`() {
        putFolder(ORPHAN_A, "sub/b.parquet")
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$ORPHAN_A/sub")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.afterListFiles = { root, call ->
            if (root == ORPHAN_A && call == 2) putFolder("$ORPHAN_A/aaa", "new.parquet")
        }
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == "$ORPHAN_A/sub") clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
        }
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        val details = record.diagnosticDetails
        assertPartialKeys(record, deleted = listOf("sub/b.parquet"), unlisted = listOf("aaa/new.parquet"))
        assertEquals("not_finished", details["partially_deleted_folder_directory_cleanup"])
        assertEquals(listOf("$ORPHAN_A/aaa", "$ORPHAN_A/sub", ORPHAN_A).joinToString("\n"), details["partially_deleted_folder_kept_directory_paths_sample"])
        assertNull(details["deletion_stop_cause"])
        assertTrue(
            record.errorMessage.orEmpty().contains(
                "1 unlisted object(s) found; removing its empty directories did not finish (the ownership snapshot became older than 300 s during deletion)",
            ),
            record.errorMessage,
        )
        assertTrue(flow.objectExists("$ORPHAN_A/aaa/new.parquet") && flow.objectExists("$ORPHAN_A/sub"))
        assertEquals(listOf("$ORPHAN_A/sub/b.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    // S3A lists a directory that only holds objects, so the store implies directories here.
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `an object written below an already removed directory at the folder delete gives a deletion_failed row, not a deleted folder`(deleteThrows: Boolean) {
        InMemoryS3FileSystem.impliedDirectories = true
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = deleteThrows
        putFolder(ORPHAN_A, "data/part-0.parquet")
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$ORPHAN_A/data")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == ORPHAN_A) InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/data/new.parquet", length = 1)
        }
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        assertPartialKeys(record, deleted = listOf("data/part-0.parquet"), unlisted = listOf("data/new.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_A/data/new.parquet"))
        assertFalse(flow.objectExists("$ORPHAN_A/data/part-0.parquet"))
    }

    @Test
    fun `an object written below a removed directory while a sibling directory is kept gives a deletion_failed row`() {
        InMemoryS3FileSystem.impliedDirectories = true
        putFolder(ORPHAN_A, "a/part-0.parquet", "b/part-1.parquet")
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$ORPHAN_A/a")
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$ORPHAN_A/b")
        putFolder(ORPHAN_B, "part-0.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = "$ORPHAN_A/b"
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "$ORPHAN_A/b") InMemoryS3FileSystem.putFile(ENDPOINT, "$ORPHAN_A/a/new.parquet", length = 1)
        }
        val flow = flow()

        flow.run()

        val record = assertDeletionFailed(flow)
        assertPartialKeys(record, deleted = listOf("a/part-0.parquet", "b/part-1.parquet"), unlisted = listOf("a/new.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_A/a/new.parquet"))
    }

    private fun assertSecondFolderFailed(flow: CleanupFlowHarness): CleanupAuditRecord {
        val record = flow.auditRecords.single()
        assertEquals("FAILED" to "deletion_failed", record.status to record.statusReason, record.diagnosticDetails.toString())
        assertEquals(listOf(ORPHAN_A), record.deletedFolders)
        assertEquals(1L, record.deletedFolderCount)
        assertEquals(ORPHAN_B, record.diagnosticDetails["deletion_failed_folder_path"])
        assertEquals(ORPHAN_B, record.diagnosticDetails["partially_deleted_folder_path"])
        assertEquals("1", record.diagnosticDetails["folder_not_attempted_count"])
        assertEquals(ORPHAN_C, record.diagnosticDetails["folder_not_attempted_paths_sample"])
        assertFalse(flow.objectExists("$ORPHAN_A/part-0.parquet"))
        assertTrue(flow.objectExists("$ORPHAN_C/part-0.parquet"))
        return record
    }

    private fun assertNoPartialFolder(record: CleanupAuditRecord) {
        val partial = record.diagnosticDetails.keys.filter { it.startsWith("partially_deleted_folder_") }
        assertEquals(emptyList<String>(), partial, record.diagnosticDetails.toString())
    }

    private fun assertDeletionFailed(flow: CleanupFlowHarness): CleanupAuditRecord {
        val record = flow.auditRecords.single()
        assertEquals("FAILED" to "deletion_failed", record.status to record.statusReason)
        assertEquals(emptyList<String>(), record.deletedFolders)
        assertEquals(ORPHAN_A, record.diagnosticDetails["deletion_failed_folder_path"])
        assertEquals(ORPHAN_A, record.diagnosticDetails["partially_deleted_folder_path"])
        assertEquals("1", record.diagnosticDetails["folder_not_attempted_count"])
        assertEquals(ORPHAN_B, record.diagnosticDetails["folder_not_attempted_paths_sample"])
        assertTrue(flow.objectExists("$ORPHAN_B/part-0.parquet"))
        return record
    }

    // Each listed key is in exactly one set, so the four listed sets add up to the listed count.
    private fun assertPartialKeys(
        record: CleanupAuditRecord,
        deleted: List<String> = emptyList(),
        remaining: List<String> = emptyList(),
        unknown: List<String> = emptyList(),
        vanished: List<String> = emptyList(),
        unlisted: List<String> = emptyList(),
    ) {
        val details = record.diagnosticDetails
        val sets = mapOf("deleted" to deleted, "remaining" to remaining, "unknown_state" to unknown, "vanished" to vanished, "unlisted" to unlisted)
        sets.forEach { (set, keys) ->
            assertEquals(keys.size.toString(), details["partially_deleted_folder_${set}_object_count"], "$set: $details")
            assertEquals(keys.joinToString("\n").ifEmpty { null }, details["partially_deleted_folder_${set}_keys_sample"], "$set: $details")
            assertEquals(if (keys.isEmpty()) null else "false", details["partially_deleted_folder_${set}_keys_truncated"], set)
        }
        assertEquals((deleted.size + remaining.size + unknown.size + vanished.size).toString(), details["partially_deleted_folder_listed_object_count"])
        val folder = details.getValue("partially_deleted_folder_path")
        assertFalse(folder in record.deletedFolders, record.deletedFolders.toString())
    }

    private fun assertKept(
        record: CleanupAuditRecord,
        reason: String,
        path: String,
    ) {
        assertEquals("1", record.diagnosticDetails["folder_kept_${reason}_count"], record.diagnosticDetails.toString())
        assertEquals(path, record.diagnosticDetails["folder_kept_${reason}_paths_sample"])
        assertEquals("false", record.diagnosticDetails["folder_kept_${reason}_paths_truncated"])
    }

    private fun assertStopped(
        record: CleanupAuditRecord,
        cause: String,
        deleted: List<String>,
        notAttempted: List<String>,
    ) {
        assertEquals("FAILED" to "deletion_stopped_at_recheck", record.status to record.statusReason)
        assertEquals(deleted, record.deletedFolders)
        assertEquals(deleted.size.toLong(), record.deletedFolderCount)
        assertEquals(cause, record.diagnosticDetails["deletion_stop_cause"])
        assertEquals("300", record.diagnosticDetails["ownership_snapshot_max_age_seconds"])
        assertEquals(notAttempted.size.toString(), record.diagnosticDetails["folder_not_attempted_count"])
        assertEquals(notAttempted.joinToString("\n"), record.diagnosticDetails["folder_not_attempted_paths_sample"])
        assertFalse(record.diagnosticDetails.containsKey("deletion_failed_folder_path"))
        deleted.forEach { assertFalse(InMemoryS3FileSystem.paths(ENDPOINT).any { path -> path.startsWith("$it/") }) }
    }

    // The first listing of a candidate is its inspection; the change is seen by the listing before deletion.
    private fun changeAfterInspection(
        folder: String,
        change: () -> Unit,
    ) {
        InMemoryS3FileSystem.afterListFiles = { root, call -> if (root == folder && call == 1) change() }
    }

    // Given a folder, the snapshot ages inside the delete of the folder's own directory. That is its last delete, so the
    // folder is deleted in full and the snapshot is too old at the next folder.
    private fun expireSnapshotWhileDeleting(objectPath: String) {
        InMemoryS3FileSystem.beforeDelete = { path -> if (path == objectPath) clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1)) }
    }

    private fun archiveTable(location: String): DiscoveredTable =
        DiscoveredTable(CATALOG, "archive", location.substringAfterLast('/'), isTemporary = false, location = location)

    private fun salesDatabase(extra: List<DiscoveredTable> = emptyList()): DiscoveredDatabase =
        DiscoveredDatabase(CATALOG, "sales", SALES, listOf(DiscoveredTable(CATALOG, "sales", "t1", isTemporary = false, location = "$SALES/t1")) + extra)

    private fun flow(
        dryRun: Boolean = false,
        ticker: Ticker = clock,
        snapshotMaxAge: Duration = OWNERSHIP_SNAPSHOT_MAX_AGE,
    ): CleanupFlowHarness =
        CleanupFlowHarness(
            ApplicationConfig(catalog = CATALOG, databases = listOf("sales"), dryRun = dryRun, deleteEnabled = !dryRun),
            ticker = ticker,
            snapshotMaxAge = snapshotMaxAge,
        ).also {
            every { it.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns salesDatabase()
            every { it.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
            every { it.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } returns mapOf("sales" to SALES, "archive" to ARCHIVE)
        }
}
