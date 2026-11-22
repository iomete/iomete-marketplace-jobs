package com.iomete.cleanup.untrackedtablefolders.storage

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.FileNotFoundException

private const val COMPAT = CatalogStorageTestHarness.COMPAT_ENDPOINT
private const val FOLDER = "s3a://example-bucket/db/t"

class FolderObjectListingTest {

    private lateinit var harness: CatalogStorageTestHarness

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        InMemoryS3FileSystem.impliedDirectories = true
        harness = CatalogStorageTestHarness(mapOf("example_catalog" to CatalogStorageTestHarness.s3CompatibleCatalogProperties()))
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `lists every object below the folder and nothing beside it`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/a.parquet", 3, modificationTimeMillis = 30, etag = "e-a")
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/sub/b.parquet", 5, modificationTimeMillis = 50)
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/sub/deeper/c.parquet", 7, modificationTimeMillis = 70)
        InMemoryS3FileSystem.putDirectory(COMPAT, "$FOLDER/empty")
        siblings().forEach { InMemoryS3FileSystem.putFile(COMPAT, it, 1) }

        val listing = list() as FolderListing.Listed

        assertEquals(
            listOf(
                ListedObject("a.parquet", 3, 30, "e-a"),
                ListedObject("sub/b.parquet", 5, 50, null),
                ListedObject("sub/deeper/c.parquet", 7, 70, null),
            ),
            listing.objects,
        )
        assertEquals(FolderFingerprint.of(listing.objects), listing.fingerprint)
    }

    @Test
    fun `listing ETags lose their quotes`() {
        InMemoryS3FileSystem.quoteListingEtags = true
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/a.parquet", 3, etag = "0cc175b9c0f1b6a831c399e269772661")

        val listing = list() as FolderListing.Listed

        assertEquals("0cc175b9c0f1b6a831c399e269772661", listing.objects.single().etag)
    }

    @Test
    fun `a folder above the budget is reported as too many objects, and the listing stops there`() {
        (1..5).forEach { InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/part-$it", 1) }

        assertEquals(FolderListing.TooManyObjects(FOLDER, 3), list(maxObjects = 3))
        assertEquals(4, InMemoryS3FileSystem.listFilesReturned[FOLDER])
        assertEquals(5, (list(maxObjects = 5) as FolderListing.Listed).objects.size)
        assertEquals(FolderListing.TooManyObjects(FOLDER, 0), list(maxObjects = 0))
    }

    @Test
    fun `an empty folder is listed as empty, whatever the budget`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, "$FOLDER/marker_only")

        assertEquals(FolderListing.Listed(FOLDER, emptyList()), list(maxObjects = 0))
    }

    @Test
    fun `a missing folder is not found`() {
        assertThrows(FileNotFoundException::class.java) { list() }
    }

    @Test
    fun `a key that resolves outside the folder fails the listing`() {
        // Hadoop's Path resolves the dot segments, as S3A does for a key listed under the folder's prefix.
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/a.parquet", 1)
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/../t2/x.parquet", 1)

        val error = assertThrows(IllegalStateException::class.java) { list() }

        assertTrue(error.message!!.contains("outside"), error.message)
    }

    @Test
    fun `an object listed from another folder fails the listing`() {
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/a.parquet", 1)
        siblings().forEach { InMemoryS3FileSystem.putFile(COMPAT, it, 1) }
        siblings().forEach { sibling ->
            InMemoryS3FileSystem.foreignListingEntries[FOLDER] = listOf(sibling)

            assertThrows(IllegalStateException::class.java, { list() }, sibling)
        }
    }

    @Test
    fun `a directory entry in the listing fails it`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/a.parquet", 1)
        InMemoryS3FileSystem.putDirectory(COMPAT, "$FOLDER/sub")
        InMemoryS3FileSystem.foreignListingEntries[FOLDER] = listOf("$FOLDER/sub")

        val error = assertThrows(IllegalStateException::class.java) { list() }

        assertTrue(error.message!!.contains("directory entry"), error.message)
    }

    @Test
    fun `an object named exactly like the folder fails the listing`() {
        // S3A finds the object before the directory, so listing the folder returns the object itself.
        InMemoryS3FileSystem.putFile(COMPAT, FOLDER, 1)
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/a.parquet", 1)

        assertThrows(IllegalStateException::class.java) { list() }
    }

    @ParameterizedTest
    @ValueSource(strings = ["a/../b", "./a", "a//b", "", "a/", "/a", ".."])
    fun `object paths below the folder must be plain`(key: String) {
        assertThrows(IllegalStateException::class.java) { FolderObjectListing.objectPath(FOLDER, key) }
    }

    @Test
    fun `plain keys with unusual characters map to their own path`() {
        listOf("a b", "caf\u00e9", "x=1/y", "a:b", "%41", "tab\there", "line\nbreak", "\u202Ename").forEach { key ->
            assertEquals("$FOLDER/$key", FolderObjectListing.objectPath(FOLDER, key).toString(), key)
        }
    }

    @Test
    fun `inspection keeps a folder above the budget as too many objects, with no statistics or fingerprint`() {
        (1..MAX_OBJECTS_PER_FOLDER + 1).forEach { InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/part-$it", 1) }
        (1..MAX_OBJECTS_PER_FOLDER).forEach { InMemoryS3FileSystem.putFile(COMPAT, "s3a://example-bucket/db/u/part-$it", 1) }

        val batch = harness.discoveryService().collectSizeStatsPerFolder("example_catalog", listOf(FOLDER, "s3a://example-bucket/db/u"))

        assertEquals(setOf(FOLDER), batch.tooManyObjectsFolderPaths)
        assertNull(batch.statsByFolder[FOLDER])
        assertNull(batch.fingerprintsByFolder[FOLDER])
        assertTrue(batch.failures.isEmpty())
        assertTrue(batch.absentFolderPaths.isEmpty())
        assertEquals(MAX_OBJECTS_PER_FOLDER + 1, InMemoryS3FileSystem.listFilesReturned[FOLDER])
        assertEquals(MAX_OBJECTS_PER_FOLDER.toLong(), batch.statsByFolder.getValue("s3a://example-bucket/db/u").objectCount)
        assertEquals(MAX_OBJECTS_PER_FOLDER.toLong(), batch.fingerprintsByFolder.getValue("s3a://example-bucket/db/u").objectCount)

        val statistics = CandidateFolderStatistics(batch.statsByFolder, batch.absentFolderPaths, batch.tooManyObjectsFolderPaths)
        assertEquals(CandidateInspectionOutcome.TOO_MANY_OBJECTS, statistics.outcome(FOLDER))
    }

    @Test
    fun `inspection records the fingerprint of each listed folder`() {
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/a.parquet", 3, modificationTimeMillis = 30, etag = "e-a")

        val batch = harness.discoveryService().collectSizeStatsPerFolder("example_catalog", listOf(FOLDER))

        assertEquals(FolderFingerprint.of(listOf(ListedObject("a.parquet", 3, 30, "e-a"))), batch.fingerprintsByFolder.getValue(FOLDER))
    }

    @Test
    fun `an object outside the folder makes inspection fail for that folder, not report statistics`() {
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/a.parquet", 1)
        InMemoryS3FileSystem.putFile(COMPAT, "$FOLDER/../t2/x.parquet", 1)

        val batch = harness.discoveryService().collectSizeStatsPerFolder("example_catalog", listOf(FOLDER))

        assertEquals(setOf(FOLDER), batch.failures.keys)
        assertTrue(batch.statsByFolder.isEmpty())
    }

    private fun siblings(): List<String> =
        listOf(
            "s3a://example-bucket/db/t2/a.parquet",
            "s3a://example-bucket/db/t_x",
            "s3a://example-bucket/db/t-1/a.parquet",
            "s3a://example-bucket/db/t.bak/a.parquet",
        )

    private fun list(maxObjects: Int = MAX_OBJECTS_PER_FOLDER): FolderListing =
        FileSystem.newInstance(Path(FOLDER).toUri(), harness.fileSystemProvider.buildConfiguration("example_catalog", Path(FOLDER))).use {
            FolderObjectListing.list(it, FOLDER, maxObjects)
        }
}
