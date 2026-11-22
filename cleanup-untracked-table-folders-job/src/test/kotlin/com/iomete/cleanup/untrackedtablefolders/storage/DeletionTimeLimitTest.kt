package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.candidate.StorageFolderReconciliation
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.NamedTableStorage
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

private const val CATALOG = "example_catalog"
private const val COMPAT = CatalogStorageTestHarness.COMPAT_ENDPOINT
private const val DB = "s3a://example-bucket/db"
private const val ORPHAN_A = "$DB/orphan_a"
private const val ORPHAN_B = "$DB/orphan_b"
private val LIMIT: Duration = Duration.ofMillis(500)
private val MARGIN: Duration = Duration.ofSeconds(3)

/**
 * Real time. In each test one catalog or storage call never returns and ignores interrupts, and the snapshot limit is
 * [LIMIT]. Each test must return within its limit plus [MARGIN]; the class timeout catches a wait that is not bounded.
 */
@Timeout(value = 15, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DeletionTimeLimitTest {

    private val catalogDiscoveryService = mockk<CatalogDiscoveryService>()
    private val blocker = Blocker()
    private val skew = AtomicLong()
    private val otherStorageReads = AtomicInteger()
    private val accepted = AtomicInteger()
    private val readsReturned = AtomicInteger()
    private lateinit var harness: CatalogStorageTestHarness

    @Volatile
    private var blockOtherStorageRead = 0

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        InMemoryS3FileSystem.impliedDirectories = true
        harness = CatalogStorageTestHarness(mapOf(CATALOG to CatalogStorageTestHarness.s3CompatibleCatalogProperties()))
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } returns DiscoveredDatabase(CATALOG, "analytics", DB, emptyList())
        every { catalogDiscoveryService.tableStorageIfExists(any(), any(), any()) } returns NamedTableStorage.NotFound
    }

    @AfterEach
    fun tearDown() {
        blocker.release()
        val recursive = InMemoryS3FileSystem.deleteCalls.filter { it.second }
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
        assertEquals(emptyList<Pair<String, Boolean>>(), recursive, "no delete may be recursive")
    }

    @Test
    fun `a first read of other storage that never returns stops within the limit, and its late result is never used`() {
        put("orphan_a")
        put("orphan_b")
        blockOtherStorageRead = 1
        val fence = fence()
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)

        assertWithin(started, LIMIT, atLeast = true)
        assertEquals(FenceStopCause.RECHECK_TOO_SLOW, result.fence.stop?.cause)
        assertEquals("the ownership recheck did not finish within the 500 ms limit", result.fence.stop?.message)
        assertEquals(listOf(ORPHAN_A, ORPHAN_B), result.fence.notAttemptedPaths)
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())

        blocker.release()
        assertTrue(awaitUntil(Duration.ofSeconds(5)) { otherStorageReads.get() == 1 && readsReturned.get() == 1 })
        assertEquals(0, accepted.get(), "a read the gate stopped waiting for must never be recorded")
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a read of this database's tables that never returns stops within the limit`() {
        put("orphan_a")
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } answers {
            blocker.block()
            DiscoveredDatabase(CATALOG, "analytics", DB, emptyList())
        }
        val fence = fence()
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A), emptyList(), fence)

        assertWithin(started, LIMIT)
        assertEquals(FenceStopCause.RECHECK_TOO_SLOW, result.fence.stop?.cause)
        assertEquals(listOf(ORPHAN_A), result.fence.notAttemptedPaths)
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a name probe that never returns stops the remaining folders within the limit`() {
        put("orphan_a")
        put("orphan_b")
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orphan_a") } answers {
            blocker.block()
            NamedTableStorage.NotFound
        }
        val fence = fence()
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)

        assertWithin(started, LIMIT)
        assertEquals(FenceStopCause.READ_TIMED_OUT, result.fence.stop?.cause)
        assertEquals(
            "the job stopped waiting to look up the table named like the folder for catalog=$CATALOG, database=analytics, " +
                "table=orphan_a when the ownership snapshot reached the 500 ms limit",
            result.fence.stop?.message,
        )
        assertEquals(listOf(ORPHAN_A, ORPHAN_B), result.fence.notAttemptedPaths)
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a read again before a later folder that never returns stops the remaining folders within the limit`() {
        put("orphan_a")
        put("orphan_b")
        blockOtherStorageRead = 2
        val fence = fence()
        // The snapshot grows older than the limit inside the delete of the first folder's own directory, its last
        // delete, so that folder is deleted in full and the second needs a read again.
        InMemoryS3FileSystem.putDirectory(COMPAT, ORPHAN_A)
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == ORPHAN_A) skew.addAndGet(LIMIT.multipliedBy(2).toNanos()) }
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)

        assertWithin(started, LIMIT)
        assertEquals(listOf(ORPHAN_A), result.deletedPaths)
        assertEquals(FenceStopCause.RECHECK_TOO_SLOW, result.fence.stop?.cause)
        assertEquals("the ownership recheck before folder=$ORPHAN_B did not finish within the 500 ms limit", result.fence.stop?.message)
        assertEquals(listOf(ORPHAN_B), result.fence.notAttemptedPaths)
        assertEquals(1, accepted.get())
        assertTrue(exists("$ORPHAN_B/part-0.parquet"))
    }

    @Test
    fun `a listing before deletion that never returns stops the remaining folders within the limit`() {
        put("orphan_a")
        put("orphan_b")
        val fence = fence()
        InMemoryS3FileSystem.afterListFiles = { root, _ -> if (root == ORPHAN_A) blocker.block() }
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)

        assertWithin(started, LIMIT)
        assertEquals(FenceStopCause.READ_TIMED_OUT, result.fence.stop?.cause)
        assertTrue(result.fence.stop!!.message.startsWith("the job stopped waiting to list storage folder before deletion for location=$ORPHAN_A"))
        assertEquals(listOf(ORPHAN_A, ORPHAN_B), result.fence.notAttemptedPaths)
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a status read before the first delete that never returns stops within the limit with nothing deleted`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        put("orphan_b")
        val fence = fence()
        InMemoryS3FileSystem.beforeGetFileStatus = { target -> if (target == "$ORPHAN_A/part-1.parquet") blocker.block() }
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)

        assertWithin(started, LIMIT)
        assertEquals(FenceStopCause.READ_TIMED_OUT, result.fence.stop?.cause)
        assertTrue(result.fence.stop!!.message.startsWith("the job stopped waiting to read storage object status for location=$ORPHAN_A/part-1.parquet"))
        assertNull(result.fence.partialFolder)
        assertEquals(emptyList<String>(), result.deletedPaths)
        assertEquals(listOf(ORPHAN_A, ORPHAN_B), result.fence.notAttemptedPaths)
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a status read after a delete that never returns stops within the limit and reports the folder partly deleted`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        put("orphan_b")
        val fence = fence()
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == "$ORPHAN_A/part-1.parquet" && InMemoryS3FileSystem.getFileStatusCalls[target] == 2) blocker.block()
        }
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)

        assertWithin(started, LIMIT)
        assertEquals(FenceStopCause.READ_TIMED_OUT, result.fence.stop?.cause)
        val partial = result.fence.partialFolder!!
        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, partial.outcome)
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(listOf("part-1.parquet"), partial.remainingKeys)
        assertEquals(emptyList<String>(), partial.unknownKeys + partial.vanishedKeys)
        assertEquals(listOf(ORPHAN_B), result.fence.notAttemptedPaths)
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a delete that never returns leaves its object unknown, is never sent again, and may still complete later`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        put("orphan_b")
        val fence = fence()
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == "$ORPHAN_A/part-0.parquet") blocker.block() }
        val started = System.nanoTime()

        val error =
            assertThrows(PartialDeletionException::class.java) {
                gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)
            }

        assertWithin(started, LIMIT, atLeast = true)
        val partial = error.folderResult!!
        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, partial.outcome)
        assertEquals(emptyList<String>(), partial.deletedKeys)
        assertEquals(listOf("part-0.parquet"), partial.unknownKeys)
        assertEquals(listOf("part-1.parquet"), partial.remainingKeys)
        assertTrue(partial.failure is CallTimedOutException)
        assertTrue(error.message!!.contains("1 in an unknown state (a delete may still complete)"), error.message)
        assertEquals(ORPHAN_A, error.failedPath)
        assertEquals(listOf(ORPHAN_B), error.notAttemptedPaths)
        assertEquals(ORPHAN_A, error.fenceReport().partialFolder?.folderPath)
        assertTrue(exists("$ORPHAN_A/part-0.parquet"))

        blocker.release()

        assertTrue(awaitUntil(Duration.ofSeconds(5)) { !exists("$ORPHAN_A/part-0.parquet") }, "the held delete completes after the job stopped waiting")
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists("$ORPHAN_A/part-1.parquet") && exists("$ORPHAN_B/part-0.parquet"))
    }

    // The gate waits for a delete until the snapshot deadline has passed, so no read can follow there. This fence
    // leaves time for one, to show the service never reads an uncertain key again.
    @Test
    fun `a delete not waited for to the end stays unknown even when there is time to read the object again`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == "$ORPHAN_A/part-0.parquet") blocker.block() }
        val fence =
            object : FolderDeletionFence {
                override fun readWait(): Duration = MARGIN

                override fun deleteWait(): Duration = LIMIT
            }
        val started = System.nanoTime()

        val error = assertThrows(PartialDeletionException::class.java) { harness.deletionService().deleteFolders(CATALOG, listOf(ORPHAN_A), fence) }

        assertWithin(started, LIMIT, atLeast = true)
        assertEquals(listOf("part-0.parquet"), error.folderResult!!.unknownKeys)
        assertEquals(listOf("part-1.parquet"), error.folderResult!!.remainingKeys)
        assertEquals(2, InMemoryS3FileSystem.getFileStatusCalls["$ORPHAN_A/part-0.parquet"], "one read before the first delete, one right before its delete")
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    // The directory delete may still remove an object written at the directory's name, so it is uncertain like an
    // object delete: the folder is partly deleted and the rest of the database stops.
    @Test
    fun `a directory delete that never returns is unknown, the folder is partly deleted and the next folder is not attempted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, ORPHAN_A)
        put("orphan_a")
        put("orphan_b")
        val fence = fence()
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == ORPHAN_A) blocker.block() }
        val started = System.nanoTime()

        val error =
            assertThrows(PartialDeletionException::class.java) {
                gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)
            }

        // Waited for only until the snapshot reached its limit.
        assertWithin(started, LIMIT, atLeast = true)
        val partial = error.folderResult!!
        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, partial.outcome)
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(emptyList<String>(), partial.remainingKeys + partial.unknownKeys + partial.leftoverDirectoryPaths)
        assertEquals(listOf(ORPHAN_A), partial.unknownDirectoryPaths)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, partial.directoryCleanup)
        assertTrue(partial.failure is CallTimedOutException)
        assertEquals(ORPHAN_A, error.failedPath)
        assertEquals(emptyList<String>(), error.deletedPaths)
        assertEquals(listOf(ORPHAN_B), error.notAttemptedPaths)
        assertEquals(partial, error.fenceReport().partialFolder)
        assertEquals(1, accepted.get())
        assertEquals(1, InMemoryS3FileSystem.deleteCalls.count { it.first == ORPHAN_A })
        assertTrue(exists(ORPHAN_A) && exists("$ORPHAN_B/part-0.parquet"))

        blocker.release()
        assertTrue(awaitUntil(Duration.ofSeconds(5)) { !exists(ORPHAN_A) }, "the held directory delete completes after the job stopped waiting")
        assertTrue(exists("$ORPHAN_B/part-0.parquet"))
    }

    // The limit here is longer than the class timeout. A delete waited for a full limit from when it is sent would
    // fail the test; it must be waited for only until the snapshot reaches its limit.
    @Test
    fun `a delete is waited for only until the snapshot reaches its limit, so a delete counted as done returned before then`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        put("orphan_b")
        val fence = fence()
        val limit = Duration.ofSeconds(60)
        val left = Duration.ofMillis(200)
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == "$ORPHAN_A/part-1.parquet" && InMemoryS3FileSystem.getFileStatusCalls[target] == 2) skew.addAndGet((limit - left).toNanos())
        }
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == "$ORPHAN_A/part-1.parquet") blocker.block() }
        val started = System.nanoTime()

        val error =
            assertThrows(PartialDeletionException::class.java) {
                gate(limit, startMargin = left.dividedBy(2)).deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)
            }

        assertWithin(started, left)
        val partial = error.folderResult!!
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(listOf("part-1.parquet"), partial.unknownKeys)
        assertEquals(emptyList<String>(), partial.remainingKeys + partial.vanishedKeys)
        val timedOut = partial.failure as CallTimedOutException
        assertTrue(timedOut.started)
        assertTrue(timedOut.waited > Duration.ZERO && timedOut.waited <= left, "waited ${timedOut.waited}")
        assertEquals(listOf(ORPHAN_B), error.notAttemptedPaths)
        assertEquals(listOf("$ORPHAN_A/part-0.parquet", "$ORPHAN_A/part-1.parquet").map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a delete with no time left is never sent and its key stays remaining`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        val fence =
            object : FolderDeletionFence {
                override fun readWait(): Duration = MARGIN

                override fun deleteWait(): Duration = Duration.ZERO
            }

        val error = assertThrows(PartialDeletionException::class.java) { harness.deletionService().deleteFolders(CATALOG, listOf(ORPHAN_A), fence) }

        val partial = error.folderResult!!
        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, partial.outcome)
        assertEquals(listOf("part-0.parquet", "part-1.parquet"), partial.remainingKeys)
        assertEquals(emptyList<String>(), partial.deletedKeys + partial.unknownKeys + partial.vanishedKeys)
        assertEquals(FenceStopCause.READ_TIMED_OUT, error.stop?.cause)
        assertEquals("the job did not delete storage object for location=$ORPHAN_A/part-0.parquet, because no time was left", error.stop?.message)
        assertNull(error.fenceReport().partialFolder)
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a directory delete with no time left is never sent and the folder is partly deleted with that directory kept`() {
        put("orphan_a", "part-0.parquet")
        InMemoryS3FileSystem.putDirectory(COMPAT, ORPHAN_A)
        val deletes = AtomicInteger()
        // The object delete has time; the delete of the folder's own directory after it has none.
        val fence =
            object : FolderDeletionFence {
                override fun readWait(): Duration = MARGIN

                override fun deleteWait(): Duration = if (deletes.incrementAndGet() == 1) MARGIN else Duration.ZERO
            }

        val error = assertThrows(PartialDeletionException::class.java) { harness.deletionService().deleteFolders(CATALOG, listOf(ORPHAN_A), fence) }

        val partial = error.folderResult!!
        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, partial.outcome)
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, partial.directoryCleanup)
        assertEquals(listOf(ORPHAN_A), partial.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), partial.unknownDirectoryPaths)
        assertEquals(FenceStopCause.READ_TIMED_OUT, error.stop?.cause)
        assertEquals("the job did not delete empty storage directory for location=$ORPHAN_A, because no time was left", error.stop?.message)
        assertNull(error.failedPath)
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(ORPHAN_A))
    }

    // A refused delete has returned, but the status read after it is not waited for to the end, so what the delete
    // left is not known.
    @Test
    fun `a refused delete whose status read after it never returns leaves that key unknown and the rest remaining`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        put("orphan_b")
        val fence = fence()
        InMemoryS3FileSystem.refuseDeleteFor = "$ORPHAN_A/part-0.parquet"
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == "$ORPHAN_A/part-0.parquet" && InMemoryS3FileSystem.getFileStatusCalls[target] == 3) blocker.block()
        }
        val started = System.nanoTime()

        val error =
            assertThrows(PartialDeletionException::class.java) {
                gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)
            }

        assertWithin(started, LIMIT)
        val partial = error.folderResult!!
        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, partial.outcome)
        assertEquals(listOf("part-0.parquet"), partial.unknownKeys)
        assertEquals(listOf("part-1.parquet"), partial.remainingKeys)
        assertEquals(emptyList<String>(), partial.deletedKeys + partial.vanishedKeys)
        assertTrue(partial.failure is CallTimedOutException)
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertEquals(listOf(ORPHAN_B), error.notAttemptedPaths)
    }

    // Each case blocks one read of the directory step. A read cut off at the limit is unknown: the step stops there,
    // and the folder's directory is kept rather than counted as gone.
    @ParameterizedTest
    @ValueSource(strings = ["status read before the delete", "status read at the recheck", "listing at the recheck"])
    fun `a directory read that never returns stops the step there and the folder is partly deleted with its directory kept`(read: String) {
        InMemoryS3FileSystem.putDirectory(COMPAT, ORPHAN_A)
        put("orphan_a")
        put("orphan_b")
        val fence = fence()
        val afterDelete = { InMemoryS3FileSystem.deleteCalls.any { it.first == ORPHAN_A } }
        when (read) {
            "status read before the delete" -> InMemoryS3FileSystem.beforeGetFileStatus = { target -> if (target == ORPHAN_A) blocker.block() }
            "status read at the recheck" -> {
                InMemoryS3FileSystem.refuseDeleteFor = ORPHAN_A
                InMemoryS3FileSystem.beforeGetFileStatus = { target -> if (target == ORPHAN_A && afterDelete()) blocker.block() }
            }
            else -> {
                InMemoryS3FileSystem.refuseDeleteFor = ORPHAN_A
                InMemoryS3FileSystem.beforeListStatus = { target -> if (target == ORPHAN_A && afterDelete()) blocker.block() }
            }
        }
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)

        assertWithin(started, LIMIT, atLeast = true)
        val stop = result.fence.stop!!
        assertEquals(FenceStopCause.READ_TIMED_OUT, stop.cause)
        val operation = if (read.startsWith("listing")) "list storage directory after deletion" else "read storage object status"
        assertTrue(stop.message.startsWith("the job stopped waiting to $operation for location=$ORPHAN_A "), stop.message)
        val partial = result.fence.partialFolder!!
        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, partial.outcome)
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, partial.directoryCleanup)
        assertEquals(listOf(ORPHAN_A), partial.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), partial.unknownDirectoryPaths + partial.leftoverObjectPaths)
        assertEquals(emptyList<String>(), result.deletedPaths)
        assertEquals(listOf(ORPHAN_B), result.fence.notAttemptedPaths)
        val directoryDeletes = if (read.startsWith("status read before")) emptyList() else listOf(ORPHAN_A to false)
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false) + directoryDeletes, InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(ORPHAN_A) && exists("$ORPHAN_B/part-0.parquet"))
    }

    // The recheck walks a directory that appeared during the step; a listing of it cut off at the limit stops the step.
    @Test
    fun `a listing of a new directory at the recheck that never returns stops the step and the folder is partly deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, ORPHAN_A)
        put("orphan_a")
        put("orphan_b")
        val fence = fence()
        InMemoryS3FileSystem.impliedDirectories = true
        InMemoryS3FileSystem.refuseDeleteFor = ORPHAN_A
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == ORPHAN_A) InMemoryS3FileSystem.putFile(COMPAT, "$ORPHAN_A/data/new.parquet", 5) }
        InMemoryS3FileSystem.beforeListStatus = { target -> if (target == "$ORPHAN_A/data") blocker.block() }
        val started = System.nanoTime()

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(ORPHAN_A, ORPHAN_B), emptyList(), fence)

        assertWithin(started, LIMIT, atLeast = true)
        val stop = result.fence.stop!!
        assertEquals(FenceStopCause.READ_TIMED_OUT, stop.cause)
        assertTrue(stop.message.startsWith("the job stopped waiting to list storage directory after deletion for location=$ORPHAN_A/data "), stop.message)
        val partial = result.fence.partialFolder!!
        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, partial.outcome)
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, partial.directoryCleanup)
        assertEquals(listOf(ORPHAN_A), partial.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), result.deletedPaths)
        assertEquals(listOf(ORPHAN_B), result.fence.notAttemptedPaths)
        assertEquals(listOf("$ORPHAN_A/part-0.parquet" to false, ORPHAN_A to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists("$ORPHAN_A/data/new.parquet") && exists("$ORPHAN_B/part-0.parquet"))
    }

    private fun assertWithin(
        startedNanos: Long,
        limit: Duration,
        atLeast: Boolean = false,
    ) {
        val elapsed = elapsedSince(startedNanos)
        assertTrue(elapsed < limit + MARGIN, "took $elapsed, the limit is $limit")
        if (atLeast) assertTrue(elapsed >= limit, "took $elapsed, less than the limit $limit")
    }

    private fun gate(
        limit: Duration = LIMIT,
        startMargin: Duration? = null,
    ): CandidateDeletionGate =
        CandidateDeletionGate().apply {
            config = ApplicationConfig(catalog = CATALOG, databases = listOf("analytics"), dryRun = false, deleteEnabled = true)
            catalogDiscoveryService = this@DeletionTimeLimitTest.catalogDiscoveryService
            objectStorageDeletionService = harness.deletionService()
            ticker = Ticker { System.nanoTime() + skew.get() }
            snapshotMaxAge = limit
            deleteStartMargin = startMargin
        }

    // Fingerprints are taken from the folders as they are now. Listing them here also loads the storage classes, so
    // the timed part of each test only runs calls that are already warm.
    private fun fence(): DeletionFence {
        val inspected =
            InMemoryS3FileSystem.paths(COMPAT)
                .filter { it.count { char -> char == '/' } >= 4 }
                .map { it.substringBeforeLast('/') }
                .distinct()
                .associateWith { folder -> listed(folder).fingerprint }
        InMemoryS3FileSystem.listFilesCalls.clear()
        InMemoryS3FileSystem.listStatusCalls.clear()
        InMemoryS3FileSystem.listFilesReturned.clear()
        return DeletionFence(inspected, Long.MAX_VALUE) {
            if (otherStorageReads.incrementAndGet() == blockOtherStorageRead) blocker.block()
            OtherStorageRecheck(emptyList(), emptyList()) { accepted.incrementAndGet() }.also { readsReturned.incrementAndGet() }
        }
    }

    private fun listed(folder: String): FolderListing.Listed =
        FileSystem.newInstance(Path(folder).toUri(), harness.fileSystemProvider.buildConfiguration(CATALOG, Path(folder))).use {
            FolderObjectListing.list(it, folder) as FolderListing.Listed
        }

    private fun put(
        folder: String,
        vararg keys: String,
    ) = keys.ifEmpty { arrayOf("part-0.parquet") }.forEach { InMemoryS3FileSystem.putFile(COMPAT, "$DB/$folder/$it", 5) }

    private fun exists(path: String): Boolean = path in InMemoryS3FileSystem.paths(COMPAT)

    private fun eligible(vararg paths: String): StorageFolderReconciliation.DeletionEligible =
        StorageFolderReconciliation.DeletionEligible(paths.map { StorageFolder(path = it) })
}
