package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.TestClock
import com.iomete.cleanup.untrackedtablefolders.candidate.StorageFolderReconciliation
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.catalog.NamedTableStorage
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.Duration

private const val CATALOG = "example_catalog"
private const val COMPAT = CatalogStorageTestHarness.COMPAT_ENDPOINT
private const val DB = "s3a://example-bucket/db"

class CandidateDeletionGateTest {

    private val catalogDiscoveryService = mockk<CatalogDiscoveryService>()
    private val clock = TestClock()
    private lateinit var harness: CatalogStorageTestHarness
    private var otherStorageReads = 0
    private var otherStorage: () -> OtherStorageRecheck = { OtherStorageRecheck(emptyList(), emptyList()) }

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        InMemoryS3FileSystem.impliedDirectories = true
        harness = CatalogStorageTestHarness(mapOf(CATALOG to CatalogStorageTestHarness.s3CompatibleCatalogProperties()))
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } returns discoveredDatabase()
        every { catalogDiscoveryService.tableStorageIfExists(any(), any(), any()) } returns NamedTableStorage.NotFound
    }

    @AfterEach
    fun tearDown() {
        val recursive = InMemoryS3FileSystem.deleteCalls.filter { it.second }
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
        assertEquals(emptyList<Pair<String, Boolean>>(), recursive, "no delete may be recursive")
    }

    @Test
    fun `returns an empty result and performs no IO when dry_run is true`() {
        put("orphan_a")

        val result = gate(dryRun = true).deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a"), emptyList(), fence())

        assertEquals(CandidateDeletionResult(), result)
        assertEquals(0, otherStorageReads)
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `throws when delete is not dry_run but delete_enabled is false`() {
        put("orphan_a")

        val error =
            assertThrows(IllegalStateException::class.java) {
                gate(deleteEnabled = false).deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a"), emptyList(), fence())
            }

        assertEquals("delete_enabled must be true before deleting candidate folders", error.message)
        assertEquals(0, otherStorageReads)
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `returns an empty result and performs no catalog or storage calls when the candidate list is empty`() {
        val result = gate().deleteCandidates(CATALOG, "analytics", eligible(), emptyList(), fence())

        assertEquals(CandidateDeletionResult(), result)
        assertEquals(0, otherStorageReads)
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())
    }

    @Test
    fun `deletes every candidate when none is claimed, one folder at a time in path order`() {
        put("orphan_b")
        put("orphan_a")

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_b", "$DB/orphan_a"), emptyList(), fence())

        assertEquals(listOf("$DB/orphan_a", "$DB/orphan_b"), result.deletedPaths)
        assertEquals(DeletionFenceReport(), result.fence)
        assertEquals(
            listOf("$DB/orphan_a/part-0.parquet", "$DB/orphan_b/part-0.parquet").map { it to false },
            InMemoryS3FileSystem.deleteCalls.toList(),
        )
    }

    @Test
    fun `keeps a candidate when the catalog now claims that exact path as an active table`() {
        put("now_active")
        put("still_orphan")
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } returns discoveredDatabase("$DB/active_table", "$DB/now_active")

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/now_active", "$DB/still_orphan"), emptyList(), fence())

        assertEquals(listOf("$DB/still_orphan"), result.deletedPaths)
        assertEquals(mapOf(FenceKeepReason.CLAIMED to listOf("$DB/now_active")), result.fence.keptPaths)
        assertTrue(exists("$DB/now_active/part-0.parquet"))
        assertEquals(0, InMemoryS3FileSystem.listFilesCalls["$DB/now_active"] ?: 0)
    }

    @Test
    fun `keeps a candidate when the catalog now has an active table nested below it`() {
        put("team_a")
        put("abandoned")
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } returns discoveredDatabase("$DB/team_a/orders")

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/team_a", "$DB/abandoned"), emptyList(), fence())

        assertEquals(listOf("$DB/abandoned"), result.deletedPaths)
        assertEquals(mapOf(FenceKeepReason.CLAIMED to listOf("$DB/team_a")), result.fence.keptPaths)
        assertTrue(exists("$DB/team_a/part-0.parquet"))
    }

    @Test
    fun `keeps a candidate inside a current active table location`() {
        put("sales/data")
        put("orphan")

        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } returns discoveredDatabase("$DB/sales")

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/sales/data", "$DB/orphan"), emptyList(), fence())

        assertEquals(listOf("$DB/orphan"), result.deletedPaths)
        assertEquals(mapOf(FenceKeepReason.CLAIMED to listOf("$DB/sales/data")), result.fence.keptPaths)
        assertTrue(exists("$DB/sales/data/part-0.parquet"))
    }

    @Test
    fun `keeps a candidate that holds a table write path found by the recheck`() {
        put("events_data")
        put("orphan")
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } returns
            DiscoveredDatabase(
                CATALOG,
                "analytics",
                DB,
                listOf(DiscoveredTable(CATALOG, "analytics", "events", false, "$DB/events", writeLocations = listOf("$DB/events_data"))),
            )

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/events_data", "$DB/orphan"), emptyList(), fence())

        assertEquals(listOf("$DB/orphan"), result.deletedPaths)
        assertTrue(exists("$DB/events_data/part-0.parquet"))
    }

    @Test
    fun `keeps candidates that overlap an excluded path in either direction`() {
        listOf("legacy", "kept", "orphan").forEach { put(it) }
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/archive/old/part-0.parquet", 5)
        val candidates = eligible("$DB/legacy", "s3a://example-bucket/archive/old", "$DB/kept", "$DB/orphan")

        val result =
            gate().deleteCandidates(
                CATALOG,
                "analytics",
                candidates,
                listOf("$DB/legacy/keep_me", "s3://example-bucket/archive/", "$DB/kept/"),
                fence(),
            )

        assertEquals(listOf("$DB/orphan"), result.deletedPaths)
        assertEquals(
            listOf("s3a://example-bucket/archive/old", "$DB/kept", "$DB/legacy"),
            result.fence.keptPaths.getValue(FenceKeepReason.CLAIMED).sorted(),
        )
        assertTrue(exists("s3a://example-bucket/archive/old/part-0.parquet"))
    }

    @Test
    fun `reports a folder that is gone at deletion separately from deleted folders`() {
        put("present")
        val fence = fence()
        val folders = eligible("$DB/missing", "$DB/present")

        val result = gate().deleteCandidates(CATALOG, "analytics", folders, emptyList(), fence)

        assertEquals(CandidateDeletionResult(deletedPaths = listOf("$DB/present"), notFoundPaths = listOf("$DB/missing")), result)
    }

    @Test
    fun `a deletion failure propagates with the confirmed deletions and the remaining folders untouched`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach { put(it) }
        InMemoryS3FileSystem.failDeleteFor = "$DB/orphan_b/part-0.parquet"

        val error =
            assertThrows(PartialDeletionException::class.java) {
                gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b", "$DB/orphan_c"), emptyList(), fence())
            }

        assertNull(error.stop)
        assertEquals(listOf("$DB/orphan_a"), error.deletedPaths)
        assertEquals("$DB/orphan_b", error.failedPath)
        assertEquals(listOf("$DB/orphan_c"), error.notAttemptedPaths)
        assertTrue(exists("$DB/orphan_c/part-0.parquet"))
    }

    @Test
    fun `a read of this database that throws stops before any folder is listed`() {
        put("orphan_a")
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } throws IllegalStateException("catalog unreachable")

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a"), emptyList(), fence())

        assertEquals(FenceStopCause.OWN_DATABASE_RECHECK_FAILED, result.fence.stop?.cause)
        assertEquals("the tables of catalog=$CATALOG, database=analytics could not be read again", result.fence.stop?.message)
        assertEquals(listOf("$DB/orphan_a"), result.fence.notAttemptedPaths)
        assertFalse(result.deletedAnything)
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())
    }

    @Test
    fun `an unresolved table at the recheck stops before any folder is listed`() {
        put("orphan_a")
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } returns
            DiscoveredDatabase(
                CATALOG,
                "analytics",
                DB,
                listOf(
                    DiscoveredTable(CATALOG, "analytics", "table_a", false, "$DB/table_a"),
                    DiscoveredTable(CATALOG, "analytics", "table_b", false, null, "metadata file not found"),
                ),
            )

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a"), emptyList(), fence())

        assertEquals(FenceStopCause.OWN_TABLES_UNRESOLVED, result.fence.stop?.cause)
        assertTrue(result.fence.stop!!.message.startsWith("Refusing to delete: 1 catalog table(s)"), result.fence.stop!!.message)
        assertEquals(listOf("$DB/orphan_a"), result.fence.notAttemptedPaths)
        assertFalse(result.deletedAnything)
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())
    }

    @Test
    fun `incomplete other storage at the first read stops before this database is read again`() {
        put("orphan_a")
        otherStorage = { OtherStorageRecheck(null, emptyList()) }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a"), emptyList(), fence())

        assertEquals(FenceStopCause.OTHER_STORAGE_INCOMPLETE, result.fence.stop?.cause)
        assertEquals(listOf("$DB/orphan_a"), result.fence.notAttemptedPaths)
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())
    }

    @Test
    fun `an unresolved table of another database at the first read stops`() {
        put("orphan_a")
        otherStorage = { OtherStorageRecheck(emptyList(), listOf("example_catalog.other.t")) }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a"), emptyList(), fence())

        assertEquals(FenceStopCause.OTHER_STORAGE_INCOMPLETE, result.fence.stop?.cause)
        assertTrue(exists("$DB/orphan_a/part-0.parquet"))
    }

    @Test
    fun `a read of other storage that throws at the first read stops before anything is listed`() {
        put("orphan_a")
        val failure = IllegalStateException("catalog unreachable")
        otherStorage = { throw failure }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a"), emptyList(), fence())

        assertEquals(FenceStopCause.OTHER_STORAGE_INCOMPLETE, result.fence.stop?.cause)
        val stop = result.fence.stop!!
        assertSame(failure, stop.error)
        assertEquals(listOf("$DB/orphan_a"), result.fence.notAttemptedPaths)
        assertFalse(result.deletedAnything)
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        assertTrue(InMemoryS3FileSystem.listFilesCalls.isEmpty())
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a read of other storage that throws at a later read stops the remaining folders`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach { put(it) }
        val failure = IllegalStateException("catalog unreachable")
        otherStorage = { if (otherStorageReads == 2) throw failure else OtherStorageRecheck(emptyList(), emptyList()) }
        expireAfterFolder("$DB/orphan_a", OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))

        val result =
            gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b", "$DB/orphan_c"), emptyList(), fence())

        assertEquals(FenceStopCause.OTHER_STORAGE_INCOMPLETE, result.fence.stop?.cause)
        val stop = result.fence.stop!!
        assertSame(failure, stop.error)
        assertTrue(stop.message.endsWith(" before folder=$DB/orphan_b"), stop.message)
        assertEquals(2, otherStorageReads)
        assertEquals(listOf("$DB/orphan_a"), result.deletedPaths)
        assertEquals(listOf("$DB/orphan_b", "$DB/orphan_c"), result.fence.notAttemptedPaths)
        assertNull(result.fence.partialFolder)
        assertEquals(listOf("$DB/orphan_a/part-0.parquet" to false, "$DB/orphan_a" to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertNull(InMemoryS3FileSystem.listFilesCalls["$DB/orphan_b"])
        assertTrue(exists("$DB/orphan_b/part-0.parquet") && exists("$DB/orphan_c/part-0.parquet"))
        verify(exactly = 1) { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") }
    }

    @Test
    fun `a first read that takes longer than the limit stops`() {
        put("orphan_a")
        otherStorage = {
            clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
            OtherStorageRecheck(emptyList(), emptyList())
        }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a"), emptyList(), fence())

        assertEquals(FenceStopCause.RECHECK_TOO_SLOW, result.fence.stop?.cause)
        assertTrue(exists("$DB/orphan_a/part-0.parquet"))
    }

    @Test
    fun `within the time limit ownership is read once for all folders`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach { put(it) }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b", "$DB/orphan_c"), emptyList(), fence())

        assertEquals(3, result.deletedPaths.size)
        assertEquals(1, otherStorageReads)
        verify(exactly = 1) { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") }
    }

    @Test
    fun `the snapshot age counts from the start of the read, so a slow read is followed by an earlier read again`() {
        put("orphan_a")
        put("orphan_b")
        // Measured from its end, the read would be 150 s old before orphan_b, too young to be read again.
        otherStorage = {
            clock.advance(Duration.ofMinutes(2))
            OtherStorageRecheck(emptyList(), emptyList())
        }
        expireAfterFolder("$DB/orphan_a", Duration.ofSeconds(150))

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        assertEquals(listOf("$DB/orphan_a", "$DB/orphan_b"), result.deletedPaths)
        assertEquals(2, otherStorageReads)
    }

    @Test
    fun `an owner found at the first read keeps its folder after a later read no longer shows it`() {
        put("orphan_a")
        put("orphan_b")
        otherStorage = {
            if (otherStorageReads == 1) OtherStorageRecheck(listOf("$DB/orphan_b"), emptyList()) else OtherStorageRecheck(emptyList(), emptyList())
        }
        expireAfterFolder("$DB/orphan_a", OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        assertEquals(2, otherStorageReads)
        assertEquals(listOf("$DB/orphan_a"), result.deletedPaths)
        assertEquals(mapOf(FenceKeepReason.CLAIMED to listOf("$DB/orphan_b")), result.fence.keptPaths)
        assertTrue(exists("$DB/orphan_b/part-0.parquet"))
    }

    @Test
    fun `an object newer than the cutoff in the listing before deletion keeps the folder even when it matches inspection`() {
        InMemoryS3FileSystem.putFile(COMPAT, "$DB/young/part-0.parquet", 5, modificationTimeMillis = 2_000)
        put("orphan")

        val result =
            gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/young", "$DB/orphan"), emptyList(), fence(cutoffTimeMillis = 1_999))

        assertEquals(mapOf(FenceKeepReason.TOO_NEW to listOf("$DB/young")), result.fence.keptPaths)
        assertEquals(listOf("$DB/orphan"), result.deletedPaths)
        assertTrue(exists("$DB/young/part-0.parquet"))
    }

    @Test
    fun `an object with no modification time keeps the folder even when it matches inspection`() {
        InMemoryS3FileSystem.putFile(COMPAT, "$DB/timeless/part-0.parquet", 5, modificationTimeMillis = 0)

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/timeless"), emptyList(), fence())

        assertEquals(mapOf(FenceKeepReason.UNKNOWN_AGE to listOf("$DB/timeless")), result.fence.keptPaths)
        assertTrue(exists("$DB/timeless/part-0.parquet"))
    }

    @Test
    fun `a folder with no inspection fingerprint is kept as changed`() {
        put("uninspected")

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/uninspected"), emptyList(), fence(fingerprints = emptyMap()))

        assertEquals(mapOf(FenceKeepReason.CHANGED_SINCE_INSPECTION to listOf("$DB/uninspected")), result.fence.keptPaths)
        assertTrue(exists("$DB/uninspected/part-0.parquet"))
    }

    @Test
    fun `a table named like the folder and stored in it keeps it, and a table of that name stored elsewhere protects only its own folder`() {
        listOf("orders", "events", "events_v2").forEach { put(it) }
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orders") } returns NamedTableStorage.Found("$DB/orders/")
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "events") } returns NamedTableStorage.Found("$DB/events_v2")

        val result =
            gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orders", "$DB/events", "$DB/events_v2"), emptyList(), fence())

        assertEquals(listOf("$DB/events"), result.deletedPaths)
        assertEquals(
            mapOf(
                FenceKeepReason.TABLE_NAMED_LIKE_FOLDER to listOf("$DB/orders"),
                FenceKeepReason.CLAIMED to listOf("$DB/events_v2"),
            ),
            result.fence.keptPaths,
        )
        assertTrue(exists("$DB/orders/part-0.parquet") && exists("$DB/events_v2/part-0.parquet"))
    }

    @Test
    fun `a table named like the folder and stored elsewhere keeps it when a write path of the table is in it`() {
        listOf("orders", "events").forEach { put(it) }
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orders") } returns
            NamedTableStorage.Found("$DB/orders_v2", listOf("s3a://example-bucket/elsewhere/orders_meta", "$DB/orders/data"))

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orders", "$DB/events"), emptyList(), fence())

        assertEquals(listOf("$DB/events"), result.deletedPaths)
        assertEquals(mapOf(FenceKeepReason.TABLE_NAMED_LIKE_FOLDER to listOf("$DB/orders")), result.fence.keptPaths)
        assertTrue(exists("$DB/orders/part-0.parquet"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first.startsWith("$DB/orders") }, InMemoryS3FileSystem.deleteCalls.toString())
    }

    @Test
    fun `a table named like the folder whose storage cannot be established stops the remaining folders`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach { put(it) }
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orphan_b") } returns
            NamedTableStorage.Unresolved("gc.enabled is not true, so the table may use files outside its location")

        val result =
            gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b", "$DB/orphan_c"), emptyList(), fence())

        val stop = result.fence.stop!!
        assertEquals(FenceStopCause.NAME_PROBE_FAILED, stop.cause)
        assertEquals("the storage of the table named like folder=$DB/orphan_b could not be established", stop.message)
        assertEquals(listOf("$DB/orphan_a"), result.deletedPaths)
        assertEquals(listOf("$DB/orphan_b", "$DB/orphan_c"), result.fence.notAttemptedPaths)
        assertNull(InMemoryS3FileSystem.listFilesCalls["$DB/orphan_b"])
        assertTrue(exists("$DB/orphan_b/part-0.parquet") && exists("$DB/orphan_c/part-0.parquet"))
    }

    @Test
    fun `a table named like the folder with a write path that has no scheme and bucket stops the remaining folders`() {
        listOf("orphan_a", "orphan_b").forEach { put(it) }
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orphan_a") } returns
            NamedTableStorage.Found("s3a://example-bucket/elsewhere/orphan_a", listOf("/warehouse/orphan_a/data"))

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        assertEquals(FenceStopCause.NAME_PROBE_FAILED, result.fence.stop?.cause)
        assertEquals(emptyList<String>(), result.deletedPaths)
        assertEquals(listOf("$DB/orphan_a", "$DB/orphan_b"), result.fence.notAttemptedPaths)
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    @Test
    fun `a read again whose read of this database ends after the limit stops before the folder is listed`() {
        listOf("orphan_a", "orphan_b").forEach { put(it) }
        var reads = 0
        every { catalogDiscoveryService.discoverDatabase(CATALOG, "analytics") } answers {
            if (++reads == 2) clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
            DiscoveredDatabase(CATALOG, "analytics", DB, emptyList())
        }
        expireAfterFolder("$DB/orphan_a", OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        val stop = result.fence.stop!!
        assertEquals(FenceStopCause.RECHECK_TOO_SLOW, stop.cause)
        assertTrue(stop.message.contains("more than the 150 s a read may use of the 300 s limit"), stop.message)
        assertEquals(2, reads)
        assertEquals(listOf("$DB/orphan_a"), result.deletedPaths)
        assertEquals(listOf("$DB/orphan_b"), result.fence.notAttemptedPaths)
        assertNull(InMemoryS3FileSystem.listFilesCalls["$DB/orphan_b"])
        assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first.startsWith("$DB/orphan_b") })
        assertTrue(exists("$DB/orphan_b/part-0.parquet"))
    }

    @Test
    fun `a name probe that fails or returns an unqualified location stops the remaining folders`() {
        put("orphan_a")
        put("orphan_b")
        put("orphan_c")
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orphan_b") } throws IllegalStateException("metastore down")
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orphan_c") } returns NamedTableStorage.Found("/warehouse/orphan_c")

        val first = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b", "$DB/orphan_c"), emptyList(), fence())
        val second = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_c"), emptyList(), fence())

        assertEquals(FenceStopCause.NAME_PROBE_FAILED, first.fence.stop?.cause)
        assertEquals(listOf("$DB/orphan_a"), first.deletedPaths)
        assertEquals(listOf("$DB/orphan_b", "$DB/orphan_c"), first.fence.notAttemptedPaths)
        assertEquals(FenceStopCause.NAME_PROBE_FAILED, second.fence.stop?.cause)
        assertTrue(exists("$DB/orphan_b/part-0.parquet") && exists("$DB/orphan_c/part-0.parquet"))
    }

    @Test
    fun `a snapshot that grows too old during deletion stops before the next object and reports the partial folder`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        put("orphan_b")
        InMemoryS3FileSystem.beforeDelete = { path ->
            if (path == "$DB/orphan_a/part-0.parquet") clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
        }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        assertEquals(FenceStopCause.SNAPSHOT_EXPIRED, result.fence.stop?.cause)
        assertEquals(emptyList<String>(), result.deletedPaths)
        assertTrue(result.deletedAnything)
        val partial = result.fence.partialFolder!!
        assertEquals("$DB/orphan_a", partial.folderPath)
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(listOf("part-1.parquet"), partial.remainingKeys)
        assertEquals(listOf("$DB/orphan_b"), result.fence.notAttemptedPaths)
        assertTrue(exists("$DB/orphan_a/part-1.parquet") && exists("$DB/orphan_b/part-0.parquet"))
        assertEquals(listOf("$DB/orphan_a/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    // With 5 minutes and a 30 s margin, a delete starts only while at least 30 s of the snapshot's limit are left.
    @Test
    fun `no delete starts with less than the start margin of the snapshot left, and its key stays remaining`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        put("orphan_b")
        // The status read right before part-1's delete, after the point where ownership would be read again.
        InMemoryS3FileSystem.beforeGetFileStatus = { path ->
            if (path == "$DB/orphan_a/part-1.parquet" && InMemoryS3FileSystem.getFileStatusCalls[path] == 2) {
                clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.minusSeconds(29))
            }
        }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        val stop = result.fence.stop!!
        assertEquals(FenceStopCause.SNAPSHOT_EXPIRED, stop.cause)
        assertEquals("the ownership snapshot was within 30 s of its 300 s limit during deletion, so no further delete was started", stop.message)
        val partial = result.fence.partialFolder!!
        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, partial.outcome)
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(listOf("part-1.parquet"), partial.remainingKeys)
        assertEquals(emptyList<String>(), partial.unknownKeys + partial.vanishedKeys)
        assertEquals(DirectoryCleanupOutcome.NOT_RUN, partial.directoryCleanup)
        assertEquals(2, InMemoryS3FileSystem.getFileStatusCalls["$DB/orphan_a/part-1.parquet"], "read before the first delete and before its own")
        assertEquals(listOf("$DB/orphan_b"), result.fence.notAttemptedPaths)
        assertEquals(listOf("$DB/orphan_a/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists("$DB/orphan_a/part-1.parquet") && exists("$DB/orphan_b/part-0.parquet"))
    }

    @Test
    fun `a delete still starts with exactly the start margin left`() {
        put("orphan_a", "part-0.parquet", "part-1.parquet")
        put("orphan_b")
        // The status read right before part-1's delete, after the point where ownership would be read again.
        InMemoryS3FileSystem.beforeGetFileStatus = { path ->
            if (path == "$DB/orphan_a/part-1.parquet" && InMemoryS3FileSystem.getFileStatusCalls[path] == 2) {
                clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.minusSeconds(30))
            }
        }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        assertNull(result.fence.stop)
        assertEquals(listOf("$DB/orphan_a", "$DB/orphan_b"), result.deletedPaths)
        assertTrue(InMemoryS3FileSystem.deleteCalls.contains("$DB/orphan_a/part-1.parquet" to false))
        // The directory step that follows is past the point where ownership is read again.
        assertEquals(2, otherStorageReads)
    }

    @Test
    fun `no directory delete starts with less than the start margin left, so the folder is partly deleted with its directory kept`() {
        put("orphan_a")
        put("orphan_b")
        InMemoryS3FileSystem.putDirectory(COMPAT, "$DB/orphan_a")
        // The status read of the directory itself, after the point where ownership would be read again.
        InMemoryS3FileSystem.beforeGetFileStatus = { path ->
            if (path == "$DB/orphan_a") clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.minusSeconds(29))
        }

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        assertEquals(FenceStopCause.SNAPSHOT_EXPIRED, result.fence.stop?.cause)
        assertEquals(1, otherStorageReads)
        val partial = result.fence.partialFolder!!
        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, partial.outcome)
        assertEquals(listOf("part-0.parquet"), partial.deletedKeys)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, partial.directoryCleanup)
        assertEquals(listOf("$DB/orphan_a"), partial.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), partial.unknownDirectoryPaths)
        assertEquals(emptyList<String>(), result.deletedPaths)
        assertEquals(listOf("$DB/orphan_b"), result.fence.notAttemptedPaths)
        assertEquals(listOf("$DB/orphan_a/part-0.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists("$DB/orphan_a") && exists("$DB/orphan_b/part-0.parquet"))
    }

    // The table named like the first folder is stored elsewhere, so it does not claim that folder. Its write path is
    // in the second folder, and the protection from that probe stays for every later folder.
    @Test
    fun `a write path found by the name probe of one folder keeps a later folder`() {
        listOf("orphan_a", "orphan_b").forEach { put(it) }
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orphan_a") } returns
            NamedTableStorage.Found("s3a://example-bucket/elsewhere/orphan_a", listOf("$DB/orphan_b/data"))

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        assertEquals(listOf("$DB/orphan_a"), result.deletedPaths)
        assertEquals(mapOf(FenceKeepReason.CLAIMED to listOf("$DB/orphan_b")), result.fence.keptPaths)
        assertNull(result.fence.stop)
        assertTrue(exists("$DB/orphan_b/part-0.parquet"))
        assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first.startsWith("$DB/orphan_b") }, InMemoryS3FileSystem.deleteCalls.toString())
    }

    @Test
    fun `a name probe location that cannot be parsed as a path stops the remaining folders`() {
        listOf("orphan_a", "orphan_b").forEach { put(it) }
        every { catalogDiscoveryService.tableStorageIfExists(CATALOG, "analytics", "orphan_a") } returns NamedTableStorage.Found(" ")

        val result = gate().deleteCandidates(CATALOG, "analytics", eligible("$DB/orphan_a", "$DB/orphan_b"), emptyList(), fence())

        assertEquals(FenceStopCause.NAME_PROBE_FAILED, result.fence.stop?.cause)
        assertEquals("the table named like folder=$DB/orphan_a has a location with no scheme and bucket", result.fence.stop?.message)
        assertEquals(emptyList<String>(), result.deletedPaths)
        assertEquals(listOf("$DB/orphan_a", "$DB/orphan_b"), result.fence.notAttemptedPaths)
        assertTrue(InMemoryS3FileSystem.deleteCalls.isEmpty())
    }

    private fun gate(
        dryRun: Boolean = false,
        deleteEnabled: Boolean = true,
    ): CandidateDeletionGate =
        CandidateDeletionGate().apply {
            config = ApplicationConfig(catalog = CATALOG, databases = listOf("analytics"), dryRun = dryRun, deleteEnabled = deleteEnabled)
            catalogDiscoveryService = this@CandidateDeletionGateTest.catalogDiscoveryService
            objectStorageDeletionService = harness.deletionService()
            ticker = this@CandidateDeletionGateTest.clock
        }

    // Inspection fingerprints are taken from the folders as they are now, so only a later change can differ from them.
    private fun fence(
        fingerprints: Map<String, FolderFingerprint>? = null,
        cutoffTimeMillis: Long = Long.MAX_VALUE,
    ): DeletionFence {
        val inspected =
            fingerprints ?: InMemoryS3FileSystem.paths(COMPAT)
                .filter { it.count { char -> char == '/' } >= 4 }
                .map { it.substringBeforeLast('/') }
                .distinct()
                .associateWith { folder -> listed(folder).fingerprint }
        InMemoryS3FileSystem.listFilesCalls.clear()
        InMemoryS3FileSystem.listStatusCalls.clear()
        InMemoryS3FileSystem.listFilesReturned.clear()
        return DeletionFence(inspected, cutoffTimeMillis) {
            otherStorageReads += 1
            otherStorage()
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

    // The clock moves inside the delete of the folder's own directory, its last delete, so the folder is deleted in
    // full and the next folder sees the older snapshot.
    private fun expireAfterFolder(
        folder: String,
        by: Duration,
    ) {
        InMemoryS3FileSystem.putDirectory(COMPAT, folder)
        InMemoryS3FileSystem.beforeDelete = { path -> if (path == folder) clock.advance(by) }
    }

    private fun eligible(vararg paths: String): StorageFolderReconciliation.DeletionEligible =
        StorageFolderReconciliation.DeletionEligible(paths.map { StorageFolder(path = it) })

    private fun discoveredDatabase(vararg activeTableLocations: String): DiscoveredDatabase =
        DiscoveredDatabase(
            catalog = CATALOG,
            database = "analytics",
            location = DB,
            tables =
                activeTableLocations.mapIndexed { index, location ->
                    DiscoveredTable(catalog = CATALOG, database = "analytics", table = "table_$index", isTemporary = false, location = location)
                },
        )
}
