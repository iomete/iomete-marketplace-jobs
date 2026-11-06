package com.iomete.cleanup.untrackedtablefolders.storage

import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val COMPAT = CatalogStorageTestHarness.COMPAT_ENDPOINT

class CatalogScopedStorageOperationsTest {

    private lateinit var harness: CatalogStorageTestHarness

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        harness =
            CatalogStorageTestHarness(
                catalogProperties = mapOf("example_catalog" to CatalogStorageTestHarness.s3CompatibleCatalogProperties()),
            )
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `lists only immediate child directories, sorted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orders")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/customers")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orders/nested")
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/loose_file.parquet", 10)

        val folders = harness.discoveryService().listImmediateChildFolders("example_catalog", "s3a://example-bucket/db")

        assertEquals(
            listOf("s3a://example-bucket/db/customers", "s3a://example-bucket/db/orders"),
            folders.map { it.path },
        )
    }

    @Test
    fun `uppercase and lowercase prefixes stay distinct folders`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/ORDERS")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orders")
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/ORDERS/a.parquet", 7)
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/orders/b.parquet", 11)

        val discovery = harness.discoveryService()
        val folders = discovery.listImmediateChildFolders("example_catalog", "s3a://example-bucket/db").map { it.path }

        assertEquals(
            listOf("s3a://example-bucket/db/ORDERS", "s3a://example-bucket/db/orders"),
            folders,
        )

        assertEquals(7L, discovery.collectSizeStats("example_catalog", listOf("s3a://example-bucket/db/ORDERS")).totalSizeBytes)
        assertEquals(11L, discovery.collectSizeStats("example_catalog", listOf("s3a://example-bucket/db/orders")).totalSizeBytes)

        harness.deletionService().deleteFoldersRecursively("example_catalog", listOf("s3a://example-bucket/db/ORDERS"))

        assertFalse(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/ORDERS"))
        assertTrue(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orders"))
        assertTrue(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orders/b.parquet"))
    }

    @Test
    fun `collects size statistics recursively across folders`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_a")
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/orphan_a/part-0.parquet", 100)
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_a/data")
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/orphan_a/data/part-1.parquet", 250)
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_b")
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/orphan_b/part-0.parquet", 40)

        val stats =
            harness.discoveryService().collectSizeStats(
                "example_catalog",
                listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/orphan_b"),
            )

        assertEquals(3L, stats.objectCount)
        assertEquals(390L, stats.totalSizeBytes)
    }

    @Test
    fun `size collection fails closed when the catalog is not registered`() {
        assertThrows(CatalogStorageConfigurationException::class.java) {
            harness.discoveryService().collectSizeStats("unknown_catalog", listOf("s3a://example-bucket/db/orphan_a"))
        }
    }

    @Test
    fun `deletes a folder and everything under it`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan")
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/orphan/part-0.parquet", 5)
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/keep")

        val result = harness.deletionService().deleteFoldersRecursively("example_catalog", listOf("s3a://example-bucket/db/orphan")).single()

        assertTrue(result.deleted)
        assertEquals("s3a://example-bucket/db/orphan", result.path)
        assertEquals(setOf("s3a://example-bucket/db/keep"), InMemoryS3FileSystem.paths(COMPAT))
    }

    @Test
    fun `reports not deleted when the folder is already gone`() {
        val result = harness.deletionService().deleteFoldersRecursively("example_catalog", listOf("s3a://example-bucket/db/vanished")).single()

        assertFalse(result.deleted)
    }

    @Test
    fun `deletion fails closed when the catalog is not registered`() {
        assertThrows(CatalogStorageConfigurationException::class.java) {
            harness.deletionService().deleteFoldersRecursively("unknown_catalog", listOf("s3a://example-bucket/db/orphan"))
        }
    }

    @Test
    fun `one filesystem serves every folder in a size collection batch`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
            InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/$it/part-0.parquet", 10)
        }

        harness.discoveryService().collectSizeStats(
            "example_catalog",
            listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/orphan_b", "s3a://example-bucket/db/orphan_c"),
        )

        assertEquals(1, InMemoryS3FileSystem.opened.size)
        assertEquals(1, InMemoryS3FileSystem.closedEndpoints.size)
    }

    @Test
    fun `one filesystem serves every folder in a deletion batch`() {
        listOf("orphan_a", "orphan_b").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
        }

        harness.deletionService().deleteFoldersRecursively(
            "example_catalog",
            listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/orphan_b"),
        )

        assertEquals(1, InMemoryS3FileSystem.opened.size)
        assertEquals(1, InMemoryS3FileSystem.closedEndpoints.size)
        assertTrue(InMemoryS3FileSystem.paths(COMPAT).isEmpty())
    }

    @Test
    fun `the batch filesystem is closed when an operation fails partway`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_a")

        assertThrows(IllegalStateException::class.java) {
            harness.deletionService().deleteFoldersRecursively(
                "example_catalog",
                listOf("s3a://example-bucket/db/orphan_a", "s3a://other-bucket/db/orphan_b"),
            )
        }

        assertEquals(InMemoryS3FileSystem.opened.size, InMemoryS3FileSystem.closedEndpoints.size)
    }

    @Test
    fun `a batch spanning two storage targets is refused rather than sharing one filesystem`() {
        val error =
            assertThrows(CatalogStorageConfigurationException::class.java) {
                harness.discoveryService().collectSizeStats(
                    "example_catalog",
                    listOf("s3a://example-bucket/db/a", "s3a://another-bucket/db/b"),
                )
            }

        assertEquals("example_catalog", error.catalog)
        assertTrue(error.message!!.contains("more than one storage target"))
        assertTrue(InMemoryS3FileSystem.opened.isEmpty())
    }

    @Test
    fun `deletion stops at the first failing folder`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
        }
        InMemoryS3FileSystem.failDeleteFor = "s3a://example-bucket/db/orphan_b"

        assertThrows(IllegalStateException::class.java) {
            harness.deletionService().deleteFoldersRecursively(
                "example_catalog",
                listOf(
                    "s3a://example-bucket/db/orphan_a",
                    "s3a://example-bucket/db/orphan_b",
                    "s3a://example-bucket/db/orphan_c",
                ),
            )
        }

        assertTrue(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orphan_c"))
    }

    @Test
    fun `deletion stops when an existing folder is not deleted`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
        }
        InMemoryS3FileSystem.refuseDeleteFor = "s3a://example-bucket/db/orphan_b"

        val error =
            assertThrows(IllegalStateException::class.java) {
                harness.deletionService().deleteFoldersRecursively(
                    "example_catalog",
                    listOf(
                        "s3a://example-bucket/db/orphan_a",
                        "s3a://example-bucket/db/orphan_b",
                        "s3a://example-bucket/db/orphan_c",
                    ),
                )
            }

        assertTrue(error.message!!.contains("s3a://example-bucket/db/orphan_b"))
        assertFalse(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orphan_a"))
        assertTrue(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orphan_c"))
    }

    @Test
    fun `a failing folder keeps its own path in the error context`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_a")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_b")
        InMemoryS3FileSystem.failDeleteFor = "s3a://example-bucket/db/orphan_b"

        val error =
            assertThrows(IllegalStateException::class.java) {
                harness.deletionService().deleteFoldersRecursively(
                    "example_catalog",
                    listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/orphan_b"),
                )
            }

        assertTrue(error.message!!.contains("delete storage folder recursively"))
        assertTrue(error.message!!.contains("catalog=example_catalog"))
        assertTrue(error.message!!.contains("s3a://example-bucket/db/orphan_b"))
    }

    @Test
    fun `discovery failures are not swallowed`() {
        val error =
            assertThrows(IllegalStateException::class.java) {
                harness.discoveryService().listImmediateChildFolders("example_catalog", "s3a://example-bucket/never-created")
            }

        assertTrue(error.message!!.contains("Failed to list immediate child folders"))
        assertTrue(error.message!!.contains("catalog=example_catalog"))
    }

    @Test
    fun `size collection reports the newest object time from the same traversal`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan", modificationTimeMillis = 9_999)
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/orphan/part-0.parquet", 5, modificationTimeMillis = 100)
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan/nested", modificationTimeMillis = 9_999)
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/orphan/nested/part-1.parquet", 7, modificationTimeMillis = 300)

        val stats =
            harness.discoveryService()
                .collectSizeStatsPerFolder("example_catalog", listOf("s3a://example-bucket/db/orphan"))
                .statsByFolder.getValue("s3a://example-bucket/db/orphan")

        assertEquals(StorageSizeStats(objectCount = 2, totalSizeBytes = 12, newestObjectModificationTimeMillis = 300), stats)
        assertEquals(1, InMemoryS3FileSystem.listStatusCalls["s3a://example-bucket/db/orphan"])
    }

    @Test
    fun `size collection reports no object time for an empty folder`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/empty", modificationTimeMillis = 9_999)

        val stats =
            harness.discoveryService()
                .collectSizeStatsPerFolder("example_catalog", listOf("s3a://example-bucket/db/empty"))
                .statsByFolder.getValue("s3a://example-bucket/db/empty")

        assertEquals(StorageSizeStats(objectCount = 0, totalSizeBytes = 0, newestObjectModificationTimeMillis = null), stats)
    }

    @Test
    fun `size collection reports no object time when any object time is invalid`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/mixed", modificationTimeMillis = 9_999)
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/mixed/part-0.parquet", 5, modificationTimeMillis = 100)
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/mixed/part-1.parquet", 7, modificationTimeMillis = 0)

        val stats =
            harness.discoveryService()
                .collectSizeStatsPerFolder("example_catalog", listOf("s3a://example-bucket/db/mixed"))
                .statsByFolder.getValue("s3a://example-bucket/db/mixed")

        assertEquals(StorageSizeStats(objectCount = 2, totalSizeBytes = 12, newestObjectModificationTimeMillis = null), stats)
    }

    @Test
    fun `a folder whose listing fails is reported as a failure, not as stats`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/unreadable")
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/unreadable/part-0.parquet", 5)
        InMemoryS3FileSystem.failListFor = "s3a://example-bucket/db/unreadable"

        val batch = harness.discoveryService().collectSizeStatsPerFolder("example_catalog", listOf("s3a://example-bucket/db/unreadable"))

        assertTrue(batch.statsByFolder.isEmpty())
        assertEquals(setOf("s3a://example-bucket/db/unreadable"), batch.failures.keys)
        assertTrue(batch.absentFolderPaths.isEmpty())
    }

    @Test
    fun `a folder that is gone on inspection and on recheck is confirmed absent, not a failure`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan")
        InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/orphan/part-0.parquet", 5)

        val batch =
            harness.discoveryService().collectSizeStatsPerFolder(
                "example_catalog",
                listOf("s3a://example-bucket/db/gone", "s3a://example-bucket/db/orphan"),
            )

        assertEquals(setOf("s3a://example-bucket/db/gone"), batch.absentFolderPaths)
        assertTrue(batch.failures.isEmpty())
        assertEquals(setOf("s3a://example-bucket/db/orphan"), batch.statsByFolder.keys)
        assertEquals(1, InMemoryS3FileSystem.getFileStatusCalls["s3a://example-bucket/db/gone"])
    }

    @Test
    fun `folders holding only directory markers are empty, not absent`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/marker_only")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/nested_markers")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/nested_markers/data")

        val batch =
            harness.discoveryService().collectSizeStatsPerFolder(
                "example_catalog",
                listOf("s3a://example-bucket/db/marker_only", "s3a://example-bucket/db/nested_markers"),
            )

        assertTrue(batch.absentFolderPaths.isEmpty())
        assertTrue(batch.failures.isEmpty())
        batch.statsByFolder.values.forEach { assertEquals(0L, it.objectCount) }
        assertEquals(2, batch.statsByFolder.size)
    }

    @Test
    fun `access denied while collecting statistics is a failure and is not rechecked`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/denied")
        InMemoryS3FileSystem.listFailures["s3a://example-bucket/db/denied"] =
            java.nio.file.AccessDeniedException("s3a://example-bucket/db/denied")

        val batch = harness.discoveryService().collectSizeStatsPerFolder("example_catalog", listOf("s3a://example-bucket/db/denied"))

        assertTrue(batch.absentFolderPaths.isEmpty())
        assertTrue(batch.failures.getValue("s3a://example-bucket/db/denied").cause is java.nio.file.AccessDeniedException)
        assertEquals(null, InMemoryS3FileSystem.getFileStatusCalls["s3a://example-bucket/db/denied"])
    }

    @Test
    fun `a folder not found on inspection but present on recheck stays a failure`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/racing")
        InMemoryS3FileSystem.listFailures["s3a://example-bucket/db/racing"] =
            java.io.FileNotFoundException("No such file or directory: s3a://example-bucket/db/racing")

        val batch = harness.discoveryService().collectSizeStatsPerFolder("example_catalog", listOf("s3a://example-bucket/db/racing"))

        assertTrue(batch.absentFolderPaths.isEmpty())
        val failure = batch.failures.getValue("s3a://example-bucket/db/racing")
        assertTrue(failure.message!!.contains("exists on recheck"))
        assertTrue(failure.cause!!.cause is java.io.FileNotFoundException)
    }

    @Test
    fun `a folder not found on inspection whose recheck fails stays a failure`() {
        InMemoryS3FileSystem.statusFailures["s3a://example-bucket/db/flaky"] = java.io.IOException("simulated timeout")

        val batch = harness.discoveryService().collectSizeStatsPerFolder("example_catalog", listOf("s3a://example-bucket/db/flaky"))

        assertTrue(batch.absentFolderPaths.isEmpty())
        val failure = batch.failures.getValue("s3a://example-bucket/db/flaky")
        assertTrue(failure is CatalogStorageOperationException)
        assertTrue(failure.cause is java.io.IOException)
        assertFalse(failure.cause is java.io.FileNotFoundException)
    }
    @Test
    fun `a failed folder delete reports the confirmed deletions and the failed folder`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
        }
        InMemoryS3FileSystem.failDeleteFor = "s3a://example-bucket/db/orphan_b"

        val error =
            assertThrows(PartialDeletionException::class.java) {
                harness.deletionService().deleteFoldersRecursively(
                    "example_catalog",
                    listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/orphan_b", "s3a://example-bucket/db/orphan_c"),
                )
            }

        assertEquals(listOf("s3a://example-bucket/db/orphan_a"), error.deletedPaths)
        assertEquals("s3a://example-bucket/db/orphan_b", error.failedPath)
        assertTrue(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orphan_c"))
    }

    @Test
    fun `an already absent folder is not counted as deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_a")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_b")
        InMemoryS3FileSystem.failDeleteFor = "s3a://example-bucket/db/orphan_b"

        val error =
            assertThrows(PartialDeletionException::class.java) {
                harness.deletionService().deleteFoldersRecursively(
                    "example_catalog",
                    listOf("s3a://example-bucket/db/vanished", "s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/orphan_b"),
                )
            }

        assertEquals(listOf("s3a://example-bucket/db/orphan_a"), error.deletedPaths)
        assertEquals(listOf("s3a://example-bucket/db/vanished"), error.notFoundPaths)
    }

    @Test
    fun `a refused delete of an existing folder stops deletion with that folder's state unknown`() {
        listOf("orphan_a", "orphan_b", "orphan_c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
        }
        InMemoryS3FileSystem.refuseDeleteFor = "s3a://example-bucket/db/orphan_b"

        val error =
            assertThrows(PartialDeletionException::class.java) {
                harness.deletionService().deleteFoldersRecursively(
                    "example_catalog",
                    listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/orphan_b", "s3a://example-bucket/db/orphan_c"),
                )
            }

        assertEquals(listOf("s3a://example-bucket/db/orphan_a"), error.deletedPaths)
        assertEquals("s3a://example-bucket/db/orphan_b", error.failedPath)
        assertTrue(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orphan_c"))
    }

    @Test
    fun `a first folder that fails is still reported, with no confirmed deletions`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_a")
        InMemoryS3FileSystem.failDeleteFor = "s3a://example-bucket/db/orphan_a"

        val error =
            assertThrows(PartialDeletionException::class.java) {
                harness.deletionService().deleteFoldersRecursively("example_catalog", listOf("s3a://example-bucket/db/orphan_a"))
            }

        assertEquals(emptyList<String>(), error.deletedPaths)
        assertEquals("s3a://example-bucket/db/orphan_a", error.failedPath)
    }

    @Test
    fun `a close failure after confirmed deletions keeps them and names no failed folder`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/orphan_a")
        InMemoryS3FileSystem.failCloseFor = COMPAT

        val error =
            assertThrows(PartialDeletionException::class.java) {
                harness.deletionService().deleteFoldersRecursively("example_catalog", listOf("s3a://example-bucket/db/orphan_a"))
            }

        assertEquals(listOf("s3a://example-bucket/db/orphan_a"), error.deletedPaths)
        assertEquals(null, error.failedPath)
    }

    @Test
    fun `a folder that vanishes between the existence check and delete is not found, and deletion continues`() {
        listOf("orphan_a", "racing", "orphan_c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
            InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/$it/part-0.parquet", 5)
        }
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "s3a://example-bucket/db/racing") removeTree(target)
        }

        val results =
            harness.deletionService().deleteFoldersRecursively(
                "example_catalog",
                listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/racing", "s3a://example-bucket/db/orphan_c"),
            )

        assertEquals(
            listOf(
                DeletedStorageFolder("s3a://example-bucket/db/orphan_a", deleted = true),
                DeletedStorageFolder("s3a://example-bucket/db/racing", deleted = false),
                DeletedStorageFolder("s3a://example-bucket/db/orphan_c", deleted = true),
            ),
            results,
        )
        assertEquals(2, InMemoryS3FileSystem.getFileStatusCalls["s3a://example-bucket/db/racing"])
    }

    @Test
    fun `a refused delete whose recheck fails stays a deletion failure`() {
        listOf("orphan_a", "racing", "orphan_c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
        }
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "s3a://example-bucket/db/racing") {
                removeTree(target)
                InMemoryS3FileSystem.statusFailures[target] = java.io.IOException("simulated timeout")
            }
        }

        val error =
            assertThrows(PartialDeletionException::class.java) {
                harness.deletionService().deleteFoldersRecursively(
                    "example_catalog",
                    listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/racing", "s3a://example-bucket/db/orphan_c"),
                )
            }

        assertEquals(listOf("s3a://example-bucket/db/orphan_a"), error.deletedPaths)
        assertEquals("s3a://example-bucket/db/racing", error.failedPath)
        assertEquals(emptyList<String>(), error.notFoundPaths)
        assertTrue(error.cause!!.cause is java.io.IOException)
        assertTrue(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orphan_c"))
    }

    @Test
    fun `a delete that throws after an earlier folder vanished is a failure that keeps the not-found folder separate`() {
        listOf("racing", "orphan_b").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
        }
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "s3a://example-bucket/db/racing") removeTree(target)
        }
        InMemoryS3FileSystem.failDeleteFor = "s3a://example-bucket/db/orphan_b"

        val error =
            assertThrows(PartialDeletionException::class.java) {
                harness.deletionService().deleteFoldersRecursively(
                    "example_catalog",
                    listOf("s3a://example-bucket/db/racing", "s3a://example-bucket/db/orphan_b"),
                )
            }

        assertEquals(emptyList<String>(), error.deletedPaths)
        assertEquals(listOf("s3a://example-bucket/db/racing"), error.notFoundPaths)
        assertEquals("s3a://example-bucket/db/orphan_b", error.failedPath)
    }

    @Test
    fun `outside S3A a refused delete stays a deletion failure even if the folder is gone on recheck`() {
        InMemoryS3FileSystem.reportedScheme = "gs"
        listOf("orphan_a", "racing", "orphan_c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/$it")
        }
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "s3a://example-bucket/db/racing") removeTree(target)
        }

        val error =
            assertThrows(PartialDeletionException::class.java) {
                harness.deletionService().deleteFoldersRecursively(
                    "example_catalog",
                    listOf("s3a://example-bucket/db/orphan_a", "s3a://example-bucket/db/racing", "s3a://example-bucket/db/orphan_c"),
                )
            }

        assertEquals(listOf("s3a://example-bucket/db/orphan_a"), error.deletedPaths)
        assertEquals("s3a://example-bucket/db/racing", error.failedPath)
        assertEquals(emptyList<String>(), error.notFoundPaths)
        assertEquals(1, InMemoryS3FileSystem.getFileStatusCalls["s3a://example-bucket/db/racing"])
        assertTrue(InMemoryS3FileSystem.paths(COMPAT).contains("s3a://example-bucket/db/orphan_c"))
    }

    private fun removeTree(path: String) {
        InMemoryS3FileSystem.store(COMPAT).keys.removeIf { it == path || it.startsWith("$path/") }
    }
}
