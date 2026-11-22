package com.iomete.cleanup.untrackedtablefolders.storage

import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FilterFileSystem
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.IOException

private const val CATALOG = "example_catalog"
private const val COMPAT = CatalogStorageTestHarness.COMPAT_ENDPOINT
private const val FOLDER = "s3a://example-bucket/db/t"

class ExactKeyDeletionTest {

    private lateinit var harness: CatalogStorageTestHarness
    private lateinit var service: ObjectStorageDeletionService

    private val siblings =
        listOf(
            "s3a://example-bucket/db/t2/a.parquet",
            "s3a://example-bucket/db/t_x",
            "s3a://example-bucket/db/t-1/a.parquet",
            "s3a://example-bucket/db/t.bak/a.parquet",
        )

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        InMemoryS3FileSystem.impliedDirectories = true
        harness = CatalogStorageTestHarness(mapOf(CATALOG to CatalogStorageTestHarness.s3CompatibleCatalogProperties()))
        service = harness.deletionService()
    }

    @AfterEach
    fun tearDown() {
        val recursive = InMemoryS3FileSystem.deleteCalls.filter { it.second }
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
        assertEquals(emptyList<Pair<String, Boolean>>(), recursive, "no delete may be recursive")
    }

    @Test
    fun `deletes exactly the listed objects, then the empty directories bottom-up without recursion`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub/empty"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("empty2"))
        siblings.forEach { InMemoryS3FileSystem.putFile(COMPAT, it, 1) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(listOf("a.parquet", "sub/b.parquet"), result.deletedKeys)
        assertEquals(emptyList<String>(), result.remainingKeys + result.unknownKeys + result.vanishedKeys)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths + result.leftoverDirectoryPaths)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
        assertEquals(
            listOf(at("a.parquet"), at("sub/b.parquet"), at("empty2"), at("sub/empty"), at("sub"), FOLDER).map { it to false },
            InMemoryS3FileSystem.deleteCalls.toList(),
        )
        assertEquals(siblings.toSet(), InMemoryS3FileSystem.paths(COMPAT))
    }

    @Test
    fun `S3A-style markers left by each delete are removed bottom-up without recursion`() {
        InMemoryS3FileSystem.createParentMarkerOnDelete = true
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        put("a.parquet")
        put("x/y/z.parquet")
        siblings.forEach { InMemoryS3FileSystem.putFile(COMPAT, it, 1) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(listOf("a.parquet", "x/y/z.parquet"), result.deletedKeys)
        assertEquals(emptyList<String>(), result.leftoverDirectoryPaths)
        assertEquals(
            listOf(at("a.parquet"), at("x/y/z.parquet"), at("x/y"), at("x"), FOLDER).map { it to false },
            InMemoryS3FileSystem.deleteCalls.toList(),
        )
        assertEquals(siblings.toSet(), InMemoryS3FileSystem.paths(COMPAT))
    }

    @ParameterizedTest
    @ValueSource(strings = ["c.parquet", "late/c.parquet", "sub/c.parquet"])
    fun `an object written after the final listing survives and the folder is reported changed`(newKey: String) {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        put("sub/b.parquet")
        val listing = listed()
        InMemoryS3FileSystem.putDirectory(COMPAT, at(newKey.substringBeforeLast('/', "")))
        put(newKey)

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("a.parquet", "sub/b.parquet"), result.deletedKeys)
        assertEquals(listOf(at(newKey)), result.leftoverObjectPaths)
        assertTrue(FOLDER in result.leftoverDirectoryPaths)
        assertTrue(exists(at(newKey)))
        assertFalse(InMemoryS3FileSystem.deleteCalls.any { it.first == at(newKey) || it.first == FOLDER })
    }

    @Test
    fun `an object written between the deletion listing and the first delete stops the batch with the folder changed`() {
        put("a.parquet")
        put("b.parquet")
        InMemoryS3FileSystem.afterListFiles = { root, call -> if (root == FOLDER && call == 1) put("c.parquet") }

        val error = assertThrows(PartialDeletionException::class.java) { service.deleteFolders(CATALOG, listOf(FOLDER), fence = FolderDeletionFence.NONE) }

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, error.folderResult?.outcome)
        assertEquals(emptyList<String>(), error.deletedPaths)
        assertEquals(FOLDER, error.failedPath)
        assertEquals(
            "Folder deletion stopped after 0 confirmed deletion(s); folder $FOLDER changed during deletion: 2 of 2 listed " +
                "object(s) deleted, 0 kept, 0 in an unknown state, 0 already gone, 1 unlisted object(s) found",
            error.message,
        )
        assertTrue(exists(at("c.parquet")))
    }

    @ParameterizedTest
    @ValueSource(strings = ["length", "time", "etag", "etag removed"])
    fun `an object overwritten after the listing and before its status check is kept and reported`(change: String) {
        val etag = if (change == "time") null else "e-b"
        put("a.parquet")
        InMemoryS3FileSystem.putFile(COMPAT, at("b.parquet"), 5, modificationTimeMillis = 1_700_000_000_123, etag = etag)
        put("c.parquet")
        val listing = listed()
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == at("a.parquet")) {
                when (change) {
                    "length" -> InMemoryS3FileSystem.overwriteFile(COMPAT, at("b.parquet"), 6, 1_700_000_000_123, etag)
                    "time" -> InMemoryS3FileSystem.overwriteFile(COMPAT, at("b.parquet"), 5, 1_700_000_001_000, etag)
                    "etag" -> InMemoryS3FileSystem.overwriteFile(COMPAT, at("b.parquet"), 5, 1_700_000_000_123, "e-new")
                    "etag removed" -> InMemoryS3FileSystem.overwriteFile(COMPAT, at("b.parquet"), 5, 1_700_000_000_123, null)
                }
            }
        }

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf("b.parquet", "c.parquet"), result.remainingKeys)
        assertEquals(listOf(at("a.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("b.parquet")))
        assertTrue(exists(at("c.parquet")))
    }

    // Residual race: nothing between the status check and the delete call can see a new write to the same key, so the
    // new content is deleted. Exact-key deletion only protects keys written, or rewritten, before their status check.
    @Test
    fun `residual race - an object overwritten between its status check and its delete is deleted with its new content`() {
        put("a.parquet")
        InMemoryS3FileSystem.putFile(COMPAT, at("b.parquet"), 5, modificationTimeMillis = 1_700_000_000_123, etag = "e-b")
        put("c.parquet")
        val listing = listed()
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == at("b.parquet")) InMemoryS3FileSystem.overwriteFile(COMPAT, target, 9, 1_700_000_005_000, "e-new")
        }

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(listOf("a.parquet", "b.parquet", "c.parquet"), result.deletedKeys)
        assertFalse(exists(at("b.parquet")))
    }

    @Test
    fun `a delete that throws stops the folder with that object in an unknown state and later objects untouched`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        InMemoryS3FileSystem.failDeleteFor = at("b.parquet")

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf("b.parquet"), result.unknownKeys)
        assertEquals(listOf("c.parquet"), result.remainingKeys)
        assertTrue(result.failure is CatalogStorageOperationException)
        assertEquals(listOf(at("a.parquet"), at("b.parquet")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        // Only the read of every key before the first delete; c is never read again.
        assertEquals(1, InMemoryS3FileSystem.getFileStatusCalls[at("c.parquet")])
        assertTrue(exists(at("c.parquet")))
    }

    @Test
    fun `a refused delete stops the folder with that object kept and later objects untouched`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = at("b.parquet")

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf("b.parquet", "c.parquet"), result.remainingKeys)
        assertEquals(emptyList<String>(), result.unknownKeys + result.vanishedKeys)
        assertEquals(listOf(at("a.parquet"), at("b.parquet")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("b.parquet")))
        assertTrue(exists(at("c.parquet")))
    }

    @Test
    fun `a refused delete of an object that is gone on recheck stops the folder as changed`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = at("b.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("b.parquet")) InMemoryS3FileSystem.store(COMPAT).remove(target) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf("b.parquet"), result.vanishedKeys)
        assertEquals(listOf("c.parquet"), result.remainingKeys)
        assertTrue(exists(at("c.parquet")))
    }

    @Test
    fun `an object gone before its status check stops the folder as changed`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("a.parquet")) InMemoryS3FileSystem.store(COMPAT).remove(at("b.parquet")) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf("b.parquet"), result.vanishedKeys)
        assertEquals(listOf("c.parquet"), result.remainingKeys)
        assertEquals(listOf(at("a.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a status read that fails stops the folder before that object is deleted`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == at("a.parquet")) InMemoryS3FileSystem.statusFailures[at("b.parquet")] = IOException("simulated status failure")
        }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf("b.parquet", "c.parquet"), result.remainingKeys)
        assertEquals(listOf(at("a.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `the batch error names the folder state but carries no raw cause text`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        InMemoryS3FileSystem.failDeleteFor = at("b.parquet")

        val error = assertThrows(PartialDeletionException::class.java) { service.deleteFolders(CATALOG, listOf(FOLDER), fence = FolderDeletionFence.NONE) }

        assertEquals(
            "Folder deletion stopped after 0 confirmed deletion(s); deleting an object of folder $FOLDER failed: 1 of 3 " +
                "listed object(s) deleted, 1 kept, 1 in an unknown state (a delete may still complete), 0 already gone, " +
                "0 unlisted object(s) found",
            error.message,
        )
        assertFalse(error.message!!.contains("simulated"))
        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, error.folderResult?.outcome)
    }

    @Test
    fun `listing times in milliseconds and quoted listing ETags still match a HEAD that reports seconds`() {
        InMemoryS3FileSystem.statusTimesInSeconds = true
        InMemoryS3FileSystem.quoteListingEtags = true
        InMemoryS3FileSystem.putFile(COMPAT, at("a.parquet"), 5, modificationTimeMillis = 1_700_000_000_999, etag = "e-a")
        InMemoryS3FileSystem.putFile(COMPAT, at("b.parquet"), 5, modificationTimeMillis = 1_700_000_001_001, etag = "e-b")

        val result = delete()

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(listOf("a.parquet", "b.parquet"), result.deletedKeys)
    }

    @Test
    fun `deleting a folder never touches folders that share its name as a prefix`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        put("sub/b.parquet")
        siblings.forEach { InMemoryS3FileSystem.putFile(COMPAT, it, 1) }
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/t2")
        InMemoryS3FileSystem.putDirectory(COMPAT, "s3a://example-bucket/db/t-1")

        val results = service.deleteFolders(CATALOG, listOf(FOLDER), fence = FolderDeletionFence.NONE)

        assertEquals(listOf(DeletedStorageFolder(FOLDER, deleted = true)), results)
        assertTrue(InMemoryS3FileSystem.deleteCalls.all { it.first == FOLDER || it.first.startsWith("$FOLDER/") })
        assertEquals(
            siblings.toSet() + setOf("s3a://example-bucket/db/t2", "s3a://example-bucket/db/t-1"),
            InMemoryS3FileSystem.paths(COMPAT),
        )
    }

    // The listing of the folder's directories returns that object, so what is below the folder is not known and the
    // step cannot finish. S3A returns it only when nothing is listed below the folder; this store always does.
    @Test
    fun `an object named exactly like the folder is never deleted`() {
        InMemoryS3FileSystem.putFile(COMPAT, FOLDER, 3)
        put("a.parquet")
        val listing = FolderListing.Listed(FOLDER, listOf(ListedObject("a.parquet", 5, 1, null)))

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("the listing of storage directory=$FOLDER returned an object of the same name", result.failure?.message)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths + result.unknownDirectoryPaths)
        assertEquals(listOf(at("a.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(FOLDER))
    }

    @Test
    fun `an object written at the folder's own name before its directories are listed ends the step, and the folder is partly deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == at("a.parquet")) {
                InMemoryS3FileSystem.putFile(COMPAT, FOLDER, 7)
                put("sub/new.parquet")
            }
        }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("the listing of storage directory=$FOLDER returned an object of the same name", result.failure?.message)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(emptyList<String>(), result.remainingKeys + result.unknownKeys + result.leftoverObjectPaths + result.unknownDirectoryPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertTrue(result.describe().endsWith("removing its empty directories did not finish"), result.describe())
        assertEquals(listOf(at("a.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(FOLDER) && exists(at("sub/new.parquet")))
    }

    @Test
    fun `an object written at a directory's name before that directory is listed is reported, and the step ends there`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("zzz"))
        put("sub/b.parquet")
        InMemoryS3FileSystem.beforeListStatus = { target -> if (target == at("sub")) InMemoryS3FileSystem.putFile(COMPAT, target, 7) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("the listing of storage directory=${at("sub")} returned an object of the same name", result.failure?.message)
        assertEquals(listOf("sub/b.parquet"), result.deletedKeys)
        assertEquals(listOf(at("sub")), result.leftoverObjectPaths)
        assertEquals(listOf(at("sub"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(listOf(at("sub/b.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("sub")) && exists(at("zzz")))
    }

    @Test
    fun `an object written at a directory's name during cleanup is kept, not deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        InMemoryS3FileSystem.beforeGetFileStatus = { target -> if (target == at("sub")) InMemoryS3FileSystem.putFile(COMPAT, target, 7) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("sub/b.parquet"), result.deletedKeys)
        assertEquals(listOf(at("sub")), result.leftoverObjectPaths)
        assertEquals(listOf(at("sub"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(listOf(at("sub/b.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("sub")))
    }

    // The folder is kept with no delete, so the last listing looks at it, and gets the object back as the first
    // listing would. An object below a directory removed earlier is not reported deleted with the folder.
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `an object written at the folder's own name during cleanup is not deleted, not named, and ends the step`(newObjectBelow: Boolean) {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("a"))
        put("a/part-0.parquet")
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == FOLDER) {
                InMemoryS3FileSystem.putFile(COMPAT, target, 7)
                if (newObjectBelow) put("a/new.parquet")
            }
        }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("the listing of storage directory=$FOLDER returned an object of the same name", result.failure?.message)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths)
        assertEquals(listOf(at("a/part-0.parquet"), at("a")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(FOLDER) && (!newObjectBelow || exists(at("a/new.parquet"))))
    }

    // The kept directory was counted when the step first listed it, so the last listing does not count it again.
    @Test
    fun `the last listing does not count a kept directory again, so a folder at the entry limit is still deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        listOf("a", "b", "c").forEach {
            InMemoryS3FileSystem.putDirectory(COMPAT, at(it))
            put("$it/part-0.parquet")
        }
        InMemoryS3FileSystem.refuseDeleteFor = at("c")

        val result = delete(maxCleanupEntries = 3)

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
        assertNull(result.failure)
        assertEquals(listOf(at("c"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths)
    }

    @Test
    fun `a listed object that is now a directory is not deleted`() {
        InMemoryS3FileSystem.putFile(COMPAT, at("a"), 0, modificationTimeMillis = 1_000)
        val listing = listed()
        InMemoryS3FileSystem.putDirectory(COMPAT, at("a"), modificationTimeMillis = 1_000)

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("a"), result.remainingKeys)
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("a")))
    }

    @Test
    fun `a folder whose listing returns an object of the same name is not deleted at all`() {
        InMemoryS3FileSystem.putFile(COMPAT, FOLDER, 3)
        put("a.parquet")

        val error = assertThrows(PartialDeletionException::class.java) { service.deleteFolders(CATALOG, listOf(FOLDER), fence = FolderDeletionFence.NONE) }

        assertEquals("Folder deletion stopped after 0 confirmed deletion(s); no object of $FOLDER was deleted", error.message)
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(FOLDER))
        assertTrue(exists(at("a.parquet")))
    }

    @ParameterizedTest
    @ValueSource(strings = ["../t2/a.parquet", "sub/../../t2/a.parquet", "./a.parquet", "a//b", "/a", ""])
    fun `a listed key that is not a plain path below the folder is refused before any call`(key: String) {
        siblings.forEach { InMemoryS3FileSystem.putFile(COMPAT, it, 1) }
        put("a.parquet")
        val listing = FolderListing.Listed(FOLDER, listOf(ListedObject(key, 1, 1, null)))

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(listOf(key), result.remainingKeys)
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(InMemoryS3FileSystem.getFileStatusCalls.isEmpty())
        assertEquals(siblings.toSet() + at("a.parquet"), InMemoryS3FileSystem.paths(COMPAT))
    }

    @Test
    fun `a stop request ends the folder before the next object`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        val stop = FenceStop(FenceStopCause.SNAPSHOT_EXPIRED, "stop")
        val stopAfterFirstDelete =
            object : FolderDeletionFence {
                override fun stopBeforeNextObject(): FenceStop? = stop.takeIf { InMemoryS3FileSystem.deleteCalls.isNotEmpty() }
            }

        val result = delete(fence = stopAfterFirstDelete)

        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, result.outcome)
        assertEquals(stop, result.stop)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf("b.parquet", "c.parquet"), result.remainingKeys)
        assertEquals(listOf(at("a.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("b.parquet")))
    }

    @Test
    fun `an empty listing deletes nothing and is not found`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("marker"))

        val result = delete()

        assertEquals(FolderDeletionOutcome.NOT_FOUND, result.outcome)
        assertEquals(0, result.listedObjectCount)
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(InMemoryS3FileSystem.getFileStatusCalls.isEmpty())
        assertEquals(setOf(FOLDER, at("marker")), InMemoryS3FileSystem.paths(COMPAT))
    }

    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `an object written into a directory during cleanup is kept and the folder is reported changed`(deleteThrows: Boolean) {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = deleteThrows
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("sub")) put("sub/new.parquet") }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("sub/b.parquet"), result.deletedKeys)
        assertEquals(listOf(at("sub/new.parquet")), result.leftoverObjectPaths)
        assertEquals(listOf(at("sub"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(listOf(at("sub/b.parquet"), at("sub")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("sub/new.parquet")))
    }

    @Test
    fun `a directory delete that is refused keeps the directory and the folder still counts as deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("a.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = at("sub")

        val result = delete()

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(listOf(at("sub"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths + result.unknownDirectoryPaths)
        assertEquals(setOf(FOLDER, at("sub")), InMemoryS3FileSystem.paths(COMPAT))
    }

    // A delete that threw may still be carried out by the store, so the directory is not reported as kept.
    @Test
    fun `a directory delete that fails leaves that directory unknown, ends the step and the folder is partly deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("zzz"))
        put("a.parquet")
        InMemoryS3FileSystem.failDeleteFor = at("sub")

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertTrue(result.failure is CatalogStorageOperationException, result.failure.toString())
        assertNull(result.stop)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(emptyList<String>(), result.remainingKeys + result.unknownKeys + result.vanishedKeys + result.leftoverObjectPaths)
        assertEquals(listOf(at("sub")), result.unknownDirectoryPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertTrue(result.mayHaveDeleted())
        assertTrue(result.describe().endsWith("removing its empty directories did not finish, 1 directory delete(s) may still complete"), result.describe())
        assertEquals(listOf(at("a.parquet"), at("sub")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertEquals(setOf(FOLDER, at("sub"), at("zzz")), InMemoryS3FileSystem.paths(COMPAT))
    }

    @Test
    fun `a directory delete that fails but whose directory is gone at the recheck counts as done`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("a.parquet")
        InMemoryS3FileSystem.failDeleteFor = at("sub")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("sub")) InMemoryS3FileSystem.store(COMPAT).remove(at("sub")) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
        assertEquals(emptyList<String>(), result.unknownDirectoryPaths + result.leftoverDirectoryPaths)
        assertEquals(emptySet<String>(), InMemoryS3FileSystem.paths(COMPAT))
    }

    // A read that fails is not a refusal: what is in the directory is not known, so no later directory is deleted.
    @ParameterizedTest
    @ValueSource(strings = ["listing", "status read", "recheck after a refused delete"])
    fun `a directory read that fails ends the step at that directory and the folder is partly deleted`(read: String) {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("zzz"))
        put("a.parquet")
        val simulated = IOException("simulated read failure")
        val operation =
            when (read) {
                "listing" -> {
                    InMemoryS3FileSystem.beforeDelete = { if (it == at("a.parquet")) InMemoryS3FileSystem.listFailures[at("sub")] = simulated }
                    "list storage directory after deletion"
                }
                "status read" -> {
                    InMemoryS3FileSystem.beforeDelete = { if (it == at("a.parquet")) InMemoryS3FileSystem.statusFailures[at("sub")] = simulated }
                    "read storage object status"
                }
                else -> {
                    InMemoryS3FileSystem.refuseDeleteFor = at("sub")
                    InMemoryS3FileSystem.beforeDelete = { if (it == at("sub")) InMemoryS3FileSystem.listFailures[at("sub")] = simulated }
                    "list storage directory after deletion"
                }
            }
        val sent = if (read == "listing" || read == "status read") listOf(at("a.parquet")) else listOf(at("a.parquet"), at("sub"))

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("Failed to $operation for catalog=$CATALOG, location=${at("sub")}", result.failure?.message)
        assertTrue(result.failure?.cause === simulated)
        assertNull(result.stop)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(emptyList<String>(), result.remainingKeys + result.unknownKeys + result.vanishedKeys + result.leftoverObjectPaths)
        assertEquals(listOf(at("sub"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), result.unknownDirectoryPaths)
        assertTrue(result.describe().endsWith("removing its empty directories did not finish"), result.describe())
        assertEquals(sent.map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertEquals(setOf(FOLDER, at("sub"), at("zzz")), InMemoryS3FileSystem.paths(COMPAT))
    }

    @Test
    fun `a directory that is not empty at its delete is rechecked, and the object found there is reported`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("sub")) put("sub/new.parquet") }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf(at("sub/new.parquet")), result.leftoverObjectPaths)
        assertEquals(emptyList<String>(), result.unknownDirectoryPaths)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
    }

    // Every directory below was empty or removed before the refused delete, so an object in one found by the recheck is new.
    @ParameterizedTest
    @ValueSource(booleans = [false, true])
    fun `an object written below an already removed directory before the folder delete is found by the recheck and reported`(deleteThrows: Boolean) {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = deleteThrows
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("data"))
        put("data/part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == FOLDER) put("data/new.parquet") }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("data/part-0.parquet"), result.deletedKeys)
        assertEquals(listOf(at("data/new.parquet")), result.leftoverObjectPaths)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
        assertEquals(listOf(at("data/part-0.parquet"), at("data"), FOLDER).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("data/new.parquet")))
    }

    @Test
    fun `an object written two levels below a directory at its own delete is found by the recheck and reported`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("sub")) put("sub/deeper/new.parquet") }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf("sub/b.parquet"), result.deletedKeys)
        assertEquals(listOf(at("sub/deeper/new.parquet")), result.leftoverObjectPaths)
        assertEquals(listOf(at("sub"), FOLDER), result.leftoverDirectoryPaths)
        assertTrue(exists(at("sub/deeper/new.parquet")))
    }

    @Test
    fun `objects several directories below a removed directory are all found by the recheck`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("data"))
        put("data/part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == FOLDER) {
                put("data/x/y/new.parquet")
                put("data/x/z.parquet")
            }
        }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf(at("data/x/z.parquet"), at("data/x/y/new.parquet")), result.leftoverObjectPaths)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
    }

    @Test
    fun `a new empty directory found by the recheck is reported as unlisted`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("sub")) InMemoryS3FileSystem.putDirectory(COMPAT, at("sub/newdir")) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf(at("sub/newdir")), result.leftoverObjectPaths)
        assertTrue(exists(at("sub/newdir")))
    }

    @Test
    fun `a read that fails while the recheck walks a new directory ends the step and the folder is partly deleted`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("data"))
        put("data/part-0.parquet")
        val simulated = IOException("simulated read failure")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == FOLDER) {
                put("data/new.parquet")
                InMemoryS3FileSystem.listFailures[at("data")] = simulated
            }
        }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertTrue(result.failure?.cause === simulated)
        assertEquals(listOf("data/part-0.parquet"), result.deletedKeys)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertTrue(exists(at("data/new.parquet")))
    }

    @Test
    fun `the recheck walk counts toward the entry limit and the folder is partly deleted when it stops there`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("data"))
        put("data/part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == FOLDER) (1..3).forEach { put("data/new-$it.parquet") } }

        val result = delete(maxCleanupEntries = 3)

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("more than 3 entries were found below folder=$FOLDER after its objects were deleted", result.failure?.message)
        assertEquals((1..3).map { at("data/new-$it.parquet") }, result.leftoverObjectPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        (1..3).forEach { assertTrue(exists(at("data/new-$it.parquet"))) }
    }

    // The directories the recheck finds count too, so a folder full of new empty directories cannot run on.
    @Test
    fun `the directories the recheck finds count toward the entry limit before any of them is listed`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == FOLDER) (1..5).forEach { InMemoryS3FileSystem.putDirectory(COMPAT, at("n$it")) } }

        val result = delete(maxCleanupEntries = 2)

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("more than 2 entries were found below folder=$FOLDER after its objects were deleted", result.failure?.message)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths)
        assertEquals(emptyList<String>(), (1..5).map { at("n$it") }.filter { InMemoryS3FileSystem.listStatusCalls.containsKey(it) })
        assertEquals(listOf(at("a.parquet"), FOLDER).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a read that fails two levels down in the recheck walk ends the step and the folder is partly deleted`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("data"))
        put("data/part-0.parquet")
        val simulated = IOException("simulated read failure")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == FOLDER) {
                put("data/x/new.parquet")
                InMemoryS3FileSystem.listFailures[at("data/x")] = simulated
            }
        }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("Failed to list storage directory after deletion for catalog=$CATALOG, location=${at("data/x")}", result.failure?.message)
        assertTrue(result.failure?.cause === simulated)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertTrue(exists(at("data/x/new.parquet")))
    }

    // Objects are recorded before any directory is walked, so a walk that fails still leaves each new object named.
    @Test
    fun `a new object beside a new directory whose listing fails is still named`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("old.parquet")
        val simulated = IOException("simulated read failure")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == FOLDER) {
                put("a/new.parquet")
                put("b.parquet")
                InMemoryS3FileSystem.listFailures[at("a")] = simulated
            }
        }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertTrue(result.failure?.cause === simulated)
        assertEquals(listOf(at("b.parquet")), result.leftoverObjectPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertTrue(exists(at("a/new.parquet")) && exists(at("b.parquet")))
    }

    // What is below a directory whose listing returns an object of its name is not known, as in the main walk.
    @Test
    fun `an object written at a new directory's name while the recheck walks it ends the step and is reported`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("data"))
        put("data/part-0.parquet")
        var folderDeleted = false
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == FOLDER) {
                put("data/new.parquet")
                folderDeleted = true
            }
        }
        InMemoryS3FileSystem.beforeListStatus = { target -> if (folderDeleted && target == at("data")) InMemoryS3FileSystem.putFile(COMPAT, target, 7) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("the listing of storage directory=${at("data")} returned an object of the same name", result.failure?.message)
        assertEquals(listOf(at("data")), result.leftoverObjectPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertTrue(exists(at("data")) && exists(at("data/new.parquet")))
    }

    // An object at the folder's own name is not the folder's, so it is not named as unlisted, and what is below is not known.
    @Test
    fun `an object written at the folder's own name before its recheck is not named, and the step ends`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = FOLDER
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == FOLDER) InMemoryS3FileSystem.putFile(COMPAT, FOLDER, 7) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("the listing of storage directory=$FOLDER returned an object of the same name", result.failure?.message)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertTrue(exists(FOLDER))
    }

    // A kept directory keeps the folder, and the folder is never rechecked at its own delete. So the step lists the
    // folder once more, and an object written below a directory it already removed is found there.
    @ParameterizedTest
    @ValueSource(strings = ["returns false", "throws because a short-lived object is there"])
    fun `an object written below a removed directory while a sibling is kept is found by a last listing of the folder`(keptBecause: String) {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("a"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("b"))
        put("a/part-0.parquet")
        put("b/part-1.parquet")
        var bDeleted = false
        if (keptBecause == "returns false") {
            InMemoryS3FileSystem.refuseDeleteFor = at("b")
        } else {
            InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
            InMemoryS3FileSystem.beforeListStatus = { target -> if (bDeleted && target == at("b")) InMemoryS3FileSystem.store(COMPAT).remove(at("b/_tmp")) }
        }
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == at("b")) {
                put("a/new.parquet")
                if (keptBecause != "returns false") put("b/_tmp")
                bDeleted = true
            }
        }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
        assertEquals(listOf("a/part-0.parquet", "b/part-1.parquet"), result.deletedKeys)
        assertEquals(listOf(at("a/new.parquet")), result.leftoverObjectPaths)
        assertEquals(listOf(at("b"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(listOf(at("a/part-0.parquet"), at("b/part-1.parquet"), at("a"), at("b")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("a/new.parquet")) && exists(at("b")))
    }

    @Test
    fun `a last listing of the folder that fails ends the step and the folder is partly deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("a"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("b"))
        put("a/part-0.parquet")
        put("b/part-1.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = at("b")
        val simulated = IOException("simulated read failure")
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("b")) InMemoryS3FileSystem.listFailures[FOLDER] = simulated }

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("Failed to list storage directory after deletion for catalog=$CATALOG, location=$FOLDER", result.failure?.message)
        assertTrue(result.failure?.cause === simulated)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths)
        assertEquals(listOf(at("b"), FOLDER), result.leftoverDirectoryPaths)
    }

    @Test
    fun `a kept sibling with nothing new leaves the folder deleted, and the last listing names no kept directory`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("a"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("b"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("b/c"))
        put("a/part-0.parquet")
        put("b/c/part-1.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = at("b/c")
        val listing = listed()
        InMemoryS3FileSystem.listStatusCalls.clear()

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
        assertEquals(emptyList<String>(), result.leftoverObjectPaths)
        assertEquals(listOf(at("b/c"), at("b"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(mapOf(FOLDER to 2, at("a") to 1, at("b") to 2, at("b/c") to 3), InMemoryS3FileSystem.listStatusCalls.toMap())
        assertEquals(listOf(at("a/part-0.parquet"), at("b/c/part-1.parquet"), at("a"), at("b/c")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a folder delete that returns false is still deleted when its objects are gone`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        InMemoryS3FileSystem.refuseDeleteFor = FOLDER

        val result = delete()

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertEquals(listOf(at("a.parquet"), FOLDER).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
    }

    // The entries past the limit were never looked at, so an object written there after the listing could be unreported.
    @Test
    fun `directory cleanup visits a bounded number of entries and the folder is partly deleted when it stops there`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        (1..5).forEach { InMemoryS3FileSystem.putDirectory(COMPAT, at("m$it")) }

        val result = delete(maxCleanupEntries = 3)

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals("more than 3 entries were found below folder=$FOLDER after its objects were deleted", result.failure?.message)
        assertEquals(listOf("a.parquet"), result.deletedKeys)
        assertEquals(emptyList<String>(), result.unknownDirectoryPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertEquals(listOf(at("a.parquet"), at("m1"), at("m2"), at("m3")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertEquals(setOf(FOLDER, at("m4"), at("m5")), InMemoryS3FileSystem.paths(COMPAT))
    }

    @Test
    fun `a folder above the budget at the deletion listing is held, nothing in it is deleted, and the next folder is deleted`() {
        (1..MAX_OBJECTS_PER_FOLDER + 1).forEach { put("part-$it") }
        val next = "s3a://example-bucket/db/u"
        InMemoryS3FileSystem.putFile(COMPAT, "$next/a.parquet", 5)

        val results = service.deleteFolders(CATALOG, listOf(FOLDER, next), fence = FolderDeletionFence.NONE)

        assertEquals(
            listOf(
                DeletedStorageFolder(FOLDER, deleted = false, keptReason = FenceKeepReason.TOO_MANY_OBJECTS),
                DeletedStorageFolder(next, deleted = true),
            ),
            results,
        )
        assertEquals(listOf("$next/a.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertEquals(MAX_OBJECTS_PER_FOLDER + 1, InMemoryS3FileSystem.listFilesReturned[FOLDER])
        assertTrue(InMemoryS3FileSystem.getFileStatusCalls.keys.none { it == FOLDER || it.startsWith("$FOLDER/") })
        assertEquals(MAX_OBJECTS_PER_FOLDER + 1, InMemoryS3FileSystem.paths(COMPAT).count { it.startsWith("$FOLDER/") })
    }

    @Test
    fun `a folder at the budget is deleted`() {
        (1..MAX_OBJECTS_PER_FOLDER).forEach { put("part-$it") }

        assertEquals(listOf(DeletedStorageFolder(FOLDER, deleted = true)), service.deleteFolders(CATALOG, listOf(FOLDER), fence = FolderDeletionFence.NONE))
        assertEquals(MAX_OBJECTS_PER_FOLDER, InMemoryS3FileSystem.deleteCalls.size)
    }

    @Test
    fun `a stop that comes during the status read right before a delete keeps that delete from being sent`() {
        put("a.parquet")
        put("b.parquet")
        val expired = StopSwitch()
        // The first read of a is the check of every key before the first delete; the second is the one right before its delete.
        InMemoryS3FileSystem.beforeGetFileStatus = { target -> if (target == at("a.parquet") && InMemoryS3FileSystem.getFileStatusCalls[target] == 2) expired.on() }

        val result = delete(fence = expired)

        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, result.outcome)
        assertEquals(expired.stop, result.stop)
        assertEquals(emptyList<String>(), result.deletedKeys + result.unknownKeys + result.vanishedKeys)
        assertEquals(listOf("a.parquet", "b.parquet"), result.remainingKeys)
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a stop during the check of every key ends the folder before the next read`() {
        put("a.parquet")
        put("b.parquet")
        val expired = StopSwitch()
        InMemoryS3FileSystem.beforeGetFileStatus = { target -> if (target == at("a.parquet")) expired.on() }

        val result = delete(fence = expired)

        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, result.outcome)
        assertEquals(listOf("a.parquet", "b.parquet"), result.remainingKeys)
        assertNull(InMemoryS3FileSystem.getFileStatusCalls[at("b.parquet")])
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a status read that fails before the first delete stops the folder with nothing deleted`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        InMemoryS3FileSystem.statusFailures[at("c.parquet")] = IOException("simulated status failure")

        val result = delete()

        assertEquals(FolderDeletionOutcome.PARTIAL_FAILED, result.outcome)
        assertEquals(emptyList<String>(), result.deletedKeys + result.unknownKeys + result.vanishedKeys)
        assertEquals(listOf("a.parquet", "b.parquet", "c.parquet"), result.remainingKeys)
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `an object changed between the listing and the first delete stops the folder with nothing deleted`() {
        put("a.parquet")
        put("b.parquet")
        put("c.parquet")
        val listing = listed()
        InMemoryS3FileSystem.overwriteFile(COMPAT, at("c.parquet"), 6, 1)

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(emptyList<String>(), result.deletedKeys)
        assertEquals(listOf("a.parquet", "b.parquet", "c.parquet"), result.remainingKeys)
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
    }

    // Hadoop's Path turns the stored key b//c.parquet into b/c.parquet and x/../a.parquet into a.parquet, so the listed
    // key names another object or none. The listing check refuses only keys that do not map back to their own path.
    @ParameterizedTest
    @ValueSource(strings = ["b//c.parquet", "b//c.parquet + b/c.parquet", "x/../a.parquet + a.parquet", "x/./b.parquet + x/b.parquet"])
    fun `a folder with a stored key that its listed name does not address is held before any delete`(case: String) {
        siblings.forEach { InMemoryS3FileSystem.putFile(COMPAT, it, 1) }
        val stored = (listOf("a.parquet") + case.split(" + ")).distinct()
        stored.forEach { put(it) }

        val result = delete()

        assertEquals(FolderDeletionOutcome.KEPT, result.outcome)
        assertEquals(FenceKeepReason.KEY_NOT_ADDRESSABLE, result.keptReason)
        assertEquals(emptyList<String>(), result.deletedKeys + result.unknownKeys + result.vanishedKeys)
        assertFalse(result.touchedObjects())
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
        assertEquals(siblings.toSet() + stored.map { at(it) }, InMemoryS3FileSystem.paths(COMPAT))
        assertTrue(result.describe().startsWith("folder $FOLDER was kept before any delete: 0 of ${stored.size} listed object(s) deleted"), result.describe())
        assertTrue(result.describe().contains(", 0 already gone,"), result.describe())
    }

    @Test
    fun `a held folder is skipped and the next folder is deleted`() {
        put("a.parquet")
        put("b//c.parquet")
        val next = "s3a://example-bucket/db/u"
        InMemoryS3FileSystem.putFile(COMPAT, "$next/a.parquet", 5)

        val results = service.deleteFolders(CATALOG, listOf(FOLDER, next), fence = FolderDeletionFence.NONE)

        assertEquals(
            listOf(
                DeletedStorageFolder(FOLDER, deleted = false, keptReason = FenceKeepReason.KEY_NOT_ADDRESSABLE),
                DeletedStorageFolder(next, deleted = true),
            ),
            results,
        )
        assertEquals(listOf("$next/a.parquet" to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("a.parquet")) && exists(at("b//c.parquet")))
    }

    @Test
    fun `an object gone between the listing and the first delete holds the folder with nothing deleted`() {
        put("a.parquet")
        put("b.parquet")
        val listing = listed()
        InMemoryS3FileSystem.store(COMPAT).remove(at("b.parquet"))

        val result = delete(listing)

        assertEquals(FolderDeletionOutcome.KEPT, result.outcome)
        assertEquals(FenceKeepReason.KEY_NOT_ADDRESSABLE, result.keptReason)
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("a.parquet")))
    }

    @Test
    fun `a snapshot that grows too old during the last delete keeps every directory and the folder counts as partly deleted`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("a.parquet")
        put("sub/b.parquet")
        val expired = StopSwitch()
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("sub/b.parquet")) expired.on() }

        val result = delete(fence = expired)

        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, result.outcome)
        assertEquals(listOf("a.parquet", "sub/b.parquet"), result.deletedKeys)
        assertEquals(emptyList<String>(), result.remainingKeys + result.unknownKeys + result.unknownDirectoryPaths)
        assertEquals(expired.stop, result.stop)
        assertNull(result.failure)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals(listOf(at("sub"), FOLDER), result.leftoverDirectoryPaths)
        assertEquals(listOf(at("a.parquet"), at("sub/b.parquet")).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertEquals(setOf(FOLDER, at("sub")), InMemoryS3FileSystem.paths(COMPAT))
    }

    // Inside a directory's delete, S3A deletes an object written at the directory's name since the job's status read,
    // in place of the marker, and returns true. The store stands in for that by keeping the marker. The marker left
    // behind keeps the folder, and the recheck names it, so the folder is reported partly deleted, not deleted.
    @Test
    fun `a marker left by a directory delete that removed an object at its name leaves the folder partly deleted`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        val listing = listed()

        val result = withFileSystem { fileSystem -> service.deleteListedObjects(CATALOG, deleteKeepsMarker(fileSystem, at("sub")), listing, FolderDeletionFence.NONE) }

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(DirectoryCleanupOutcome.FINISHED, result.directoryCleanup)
        assertEquals(listOf("sub/b.parquet"), result.deletedKeys)
        assertEquals(listOf(at("sub")), result.leftoverObjectPaths)
        assertEquals(listOf(FOLDER), result.leftoverDirectoryPaths)
        assertEquals(listOf(at("sub/b.parquet"), at("sub"), FOLDER).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
    }

    // The same at the folder's own name hits an object beside the folder, outside it. Nothing is left inside the
    // folder to show it, and a status read after the delete would not undo it, so the row reports the folder
    // deleted. This pins the known gap described in the README; bucket versioning is the way back.
    @Test
    fun `known gap - a folder delete that removed an object at the folder's own name reports the folder deleted`() {
        InMemoryS3FileSystem.nonEmptyDirectoryDeleteThrows = true
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        val listing = listed()

        val result = withFileSystem { fileSystem -> service.deleteListedObjects(CATALOG, deleteKeepsMarker(fileSystem, FOLDER), listing, FolderDeletionFence.NONE) }

        assertEquals(FolderDeletionOutcome.DELETED, result.outcome)
        assertEquals(listOf(at("a.parquet"), FOLDER).map { it to false }, InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(FOLDER))
    }

    // S3A lists an object and a directory prefix of the same name as two entries, the object first, and lists the
    // object alone when nothing is below the prefix. The object is reported once.
    @Test
    fun `an object beside a directory of the same name, listed as S3A lists it, is reported once`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        val listing = listed()
        val sub = FileStatus(7, false, 1, 1, 1, Path(at("sub")))

        val result =
            withFileSystem { fileSystem ->
                val s3aLike =
                    object : FilterFileSystem(fileSystem) {
                        override fun listStatus(f: Path): Array<FileStatus> =
                            when (f.toString()) {
                                FOLDER -> arrayOf(sub) + super.listStatus(f)
                                at("sub") -> arrayOf(sub)
                                else -> super.listStatus(f)
                            }
                    }
                service.deleteListedObjects(CATALOG, s3aLike, listing, FolderDeletionFence.NONE)
            }

        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertEquals(listOf(at("sub")), result.leftoverObjectPaths)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals(listOf("sub/b.parquet"), result.deletedKeys)
        assertEquals(listOf(at("sub/b.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    // The unlisted object makes the folder changed, which the row reports as a failure, so the stop of the step is not
    // carried as a fence stop. Its message is still on the folder's description and on the batch error.
    @Test
    fun `a stop after an unlisted object was found leaves the folder changed, not stopped, and names the stop`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        InMemoryS3FileSystem.putDirectory(COMPAT, at("aaa"))
        InMemoryS3FileSystem.putDirectory(COMPAT, at("sub"))
        put("sub/b.parquet")
        InMemoryS3FileSystem.afterListFiles = { root, _ -> if (root == FOLDER) put("aaa/new.parquet") }
        val expired = StopSwitch()
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("sub/b.parquet")) expired.on() }

        val error = assertThrows(PartialDeletionException::class.java) { service.deleteFolders(CATALOG, listOf(FOLDER), fence = expired) }

        val result = error.folderResult!!
        assertEquals(FolderDeletionOutcome.PARTIAL_CHANGED, result.outcome)
        assertNull(result.stop)
        assertNull(error.stop)
        assertEquals(expired.stop, result.cleanupStop)
        assertNull(result.failure)
        assertEquals(DirectoryCleanupOutcome.NOT_FINISHED, result.directoryCleanup)
        assertEquals(listOf("sub/b.parquet"), result.deletedKeys)
        assertEquals(listOf(at("aaa/new.parquet")), result.leftoverObjectPaths)
        assertEquals(listOf(at("aaa"), at("sub"), FOLDER), result.leftoverDirectoryPaths)
        assertTrue(
            result.describe().endsWith("removing its empty directories did not finish (${expired.stop.message})"),
            result.describe(),
        )
        assertTrue(error.message!!.endsWith("(${expired.stop.message})"), error.message)
        assertEquals(FOLDER, error.failedPath)
        assertEquals(listOf(at("sub/b.parquet") to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue(exists(at("aaa/new.parquet")) && exists(at("sub")))
    }

    @Test
    fun `a stop of the step on a folder with no unlisted object is carried once, as the fence stop`() {
        InMemoryS3FileSystem.putDirectory(COMPAT, FOLDER)
        put("a.parquet")
        val expired = StopSwitch()
        InMemoryS3FileSystem.beforeDelete = { target -> if (target == at("a.parquet")) expired.on() }

        val error = assertThrows(PartialDeletionException::class.java) { service.deleteFolders(CATALOG, listOf(FOLDER), fence = expired) }

        val result = error.folderResult!!
        assertEquals(FolderDeletionOutcome.PARTIAL_STOPPED, result.outcome)
        assertEquals(expired.stop, result.stop)
        assertEquals(expired.stop, error.stop)
        assertTrue(result.describe().endsWith("removing its empty directories did not finish"), result.describe())
        assertEquals(1, error.message!!.split(expired.stop.message).size - 1, error.message)
    }

    /** A fence that lets deletion go on until [on], then stops it before the next object, as an expired snapshot does. */
    private class StopSwitch : FolderDeletionFence {
        val stop = FenceStop(FenceStopCause.SNAPSHOT_EXPIRED, "the ownership snapshot became older than 300 s during deletion")

        @Volatile
        private var stopped = false

        fun on() {
            stopped = true
        }

        override fun stopBeforeNextObject(): FenceStop? = stop.takeIf { stopped }
    }

    private fun at(key: String): String = if (key.isEmpty()) FOLDER else "$FOLDER/$key"

    private fun put(key: String) = InMemoryS3FileSystem.putFile(COMPAT, at(key), 5)

    private fun exists(path: String): Boolean = path in InMemoryS3FileSystem.paths(COMPAT)

    private fun <T> withFileSystem(block: (FileSystem) -> T): T =
        FileSystem.newInstance(Path(FOLDER).toUri(), harness.fileSystemProvider.buildConfiguration(CATALOG, Path(FOLDER))).use(block)

    private fun listed(): FolderListing.Listed = withFileSystem { FolderObjectListing.list(it, FOLDER) as FolderListing.Listed }

    // A non-recursive delete of [target] that returns true and keeps its marker, as S3A does when it deletes an object
    // written at the directory's name in place of the marker.
    private fun deleteKeepsMarker(
        fileSystem: FileSystem,
        target: String,
    ): FileSystem =
        object : FilterFileSystem(fileSystem) {
            override fun delete(
                f: Path,
                recursive: Boolean,
            ): Boolean {
                if (recursive || f.toString() != target) return super.delete(f, recursive)
                InMemoryS3FileSystem.deleteCalls += target to false
                return true
            }
        }

    private fun delete(
        listing: FolderListing.Listed = listed(),
        fence: FolderDeletionFence = FolderDeletionFence.NONE,
        maxCleanupEntries: Int = MAX_OBJECTS_PER_FOLDER,
    ): FolderDeletionResult = withFileSystem { service.deleteListedObjects(CATALOG, it, listing, fence, maxCleanupEntries) }
}
