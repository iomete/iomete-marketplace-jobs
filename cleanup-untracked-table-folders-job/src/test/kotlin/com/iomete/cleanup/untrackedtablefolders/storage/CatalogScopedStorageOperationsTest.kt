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
    }
}
