package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class UntrackedFolderCandidateDetectorTest {
    private val detector = UntrackedFolderCandidateDetector()

    @Test
    fun `excludes active table locations from candidates`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/active_table"),
                storageFolder("s3a://bucket/db/deleted_table"),
            ),
            activeTableLocations = listOf("s3a://bucket/db/active_table"),
            excludedPaths = emptyList(),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/deleted_table"),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `excludes configured paths from candidates`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/excluded_table"),
                storageFolder("s3a://bucket/db/deleted_table"),
            ),
            activeTableLocations = emptyList(),
            excludedPaths = listOf("s3a://bucket/db/excluded_table"),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/deleted_table"),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `excludes resolved database folder paths from candidates`() {
        val resolvedDatabaseFolderExclusion = "s3a://bucket/db/customer_events"

        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/customer_events"),
                storageFolder("s3a://bucket/db/deleted_table"),
            ),
            activeTableLocations = emptyList(),
            excludedPaths = listOf(resolvedDatabaseFolderExclusion),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/deleted_table"),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `normalizes trailing slashes before comparing paths`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/active_table/"),
                storageFolder("s3a://bucket/db/excluded_table/"),
                storageFolder("s3a://bucket/db/deleted_table/"),
            ),
            activeTableLocations = listOf("s3a://bucket/db/active_table"),
            excludedPaths = listOf("s3a://bucket/db/excluded_table"),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/deleted_table/"),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `throws when candidate count exceeds configured maximum`() {
        assertThrows(TooManyCandidateFoldersException::class.java) {
            detector.detectCandidates(
                storageFolders = listOf(
                    storageFolder("s3a://bucket/db/deleted_table_1"),
                    storageFolder("s3a://bucket/db/deleted_table_2"),
                ),
                activeTableLocations = emptyList(),
                excludedPaths = emptyList(),
                unresolvedTableCount = 0,
            maxCandidateFolders = 1,
            )
        }
    }

    @Test
    fun `skips Hadoop temporary sentinel folder from candidates`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/_temporary"),
                storageFolder("s3a://bucket/db/orphan_table"),
            ),
            activeTableLocations = emptyList(),
            excludedPaths = emptyList(),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/orphan_table"),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `skips Spark staging sentinel folder from candidates`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/.spark-staging-application_123"),
                storageFolder("s3a://bucket/db/orphan_table"),
            ),
            activeTableLocations = emptyList(),
            excludedPaths = emptyList(),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/orphan_table"),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `skips Hive staging sentinel folder from candidates`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/.hive-staging_hive_2026-05-23_10-30-00"),
                storageFolder("s3a://bucket/db/orphan_table"),
            ),
            activeTableLocations = emptyList(),
            excludedPaths = emptyList(),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/orphan_table"),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `zero unresolved tables yields a deletion-eligible reconciliation`() {
        val result =
            detector.detectCandidates(
                storageFolders = listOf(storageFolder("s3a://bucket/db/old_1")),
                activeTableLocations = emptyList(),
                excludedPaths = emptyList(),
                unresolvedTableCount = 0,
                maxCandidateFolders = 10,
            )

        assertTrue(result is StorageFolderReconciliation.DeletionEligible)
        assertEquals(listOf("s3a://bucket/db/old_1"), result.folders.map { it.path })
    }

    @Test
    fun `any unresolved table yields an ownership-unverified reconciliation for every folder`() {
        val result =
            detector.detectCandidates(
                storageFolders =
                    listOf(storageFolder("s3a://bucket/db/old_1"), storageFolder("s3a://bucket/db/old_2")),
                activeTableLocations = emptyList(),
                excludedPaths = emptyList(),
                unresolvedTableCount = 1,
                maxCandidateFolders = 10,
            )

        assertTrue(result is StorageFolderReconciliation.OwnershipUnverified)
        assertEquals(1, (result as StorageFolderReconciliation.OwnershipUnverified).unresolvedTableCount)
        assertEquals(
            listOf("s3a://bucket/db/old_1", "s3a://bucket/db/old_2"),
            result.folders.map { it.path },
        )
    }

    @Test
    fun `the candidate limit is enforced even when ownership is unverified`() {
        val error =
            assertThrows(TooManyCandidateFoldersException::class.java) {
                detector.detectCandidates(
                    storageFolders =
                        listOf(storageFolder("s3a://bucket/db/old_1"), storageFolder("s3a://bucket/db/old_2")),
                    activeTableLocations = emptyList(),
                    excludedPaths = emptyList(),
                    unresolvedTableCount = 1,
                    maxCandidateFolders = 1,
                )
            }

        assertEquals(2, error.candidateCount)
    }

    private fun storageFolder(path: String): StorageFolder = StorageFolder(path = path)

    @Test
    fun `protects candidate that contains an active table location nested below it`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/team_a"),
                storageFolder("s3a://bucket/db/team_b"),
                storageFolder("s3a://bucket/db/abandoned"),
            ),
            activeTableLocations = listOf(
                "s3a://bucket/db/team_a/orders",
                "s3a://bucket/db/team_b/sales",
            ),
            excludedPaths = emptyList(),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/abandoned"),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `protects candidate when active table location has trailing slash`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/team_a"),
            ),
            activeTableLocations = listOf("s3a://bucket/db/team_a/orders/"),
            excludedPaths = emptyList(),
            unresolvedTableCount = 0,
            maxCandidateFolders = 10,
        )

        assertEquals(
            emptyList<String>(),
            candidates.folders.map { it.path },
        )
    }

    @Test
    fun `age check keeps folders whose newest object is at or before the cutoff`() {
        val result =
            detector.applyAgeCutoff(
                reconciliation = deletionEligibleFolders("s3a://bucket/db/older", "s3a://bucket/db/at_cutoff"),
                folderStats =
                    mapOf(
                        "s3a://bucket/db/older" to stats(newestObjectModificationTimeMillis = 100),
                        "s3a://bucket/db/at_cutoff" to stats(newestObjectModificationTimeMillis = 200),
                    ),
                cutoffTimeMillis = 200,
            )

        assertEquals(listOf("s3a://bucket/db/older", "s3a://bucket/db/at_cutoff"), result.folders.map { it.path })
    }

    @Test
    fun `age check skips folders whose newest object is after the cutoff`() {
        val result =
            detector.applyAgeCutoff(
                reconciliation = deletionEligibleFolders("s3a://bucket/db/fresh"),
                folderStats = mapOf("s3a://bucket/db/fresh" to stats(newestObjectModificationTimeMillis = 201)),
                cutoffTimeMillis = 200,
            )

        assertEquals(emptyList<String>(), result.folders.map { it.path })
    }

    @Test
    fun `age check skips folders whose age cannot be established`() {
        val result =
            detector.applyAgeCutoff(
                reconciliation =
                    deletionEligibleFolders(
                        "s3a://bucket/db/unreadable",
                        "s3a://bucket/db/empty",
                        "s3a://bucket/db/zero_mtime",
                        "s3a://bucket/db/negative_mtime",
                    ),
                folderStats =
                    mapOf(
                        "s3a://bucket/db/empty" to StorageSizeStats(objectCount = 0, totalSizeBytes = 0),
                        "s3a://bucket/db/zero_mtime" to stats(newestObjectModificationTimeMillis = 0),
                        "s3a://bucket/db/negative_mtime" to stats(newestObjectModificationTimeMillis = -1),
                    ),
                cutoffTimeMillis = 200,
            )

        assertEquals(emptyList<String>(), result.folders.map { it.path })
    }

    @Test
    fun `age check keeps ownership unverified and its unresolved table count`() {
        val result =
            detector.applyAgeCutoff(
                reconciliation =
                    StorageFolderReconciliation.OwnershipUnverified(
                        folders = listOf(storageFolder("s3a://bucket/db/old"), storageFolder("s3a://bucket/db/fresh")),
                        unresolvedTableCount = 2,
                    ),
                folderStats =
                    mapOf(
                        "s3a://bucket/db/old" to stats(newestObjectModificationTimeMillis = 100),
                        "s3a://bucket/db/fresh" to stats(newestObjectModificationTimeMillis = 300),
                    ),
                cutoffTimeMillis = 200,
            )

        assertTrue(result is StorageFolderReconciliation.OwnershipUnverified)
        assertEquals(2, (result as StorageFolderReconciliation.OwnershipUnverified).unresolvedTableCount)
        assertEquals(listOf("s3a://bucket/db/old"), result.folders.map { it.path })
    }

    private fun deletionEligibleFolders(vararg paths: String): StorageFolderReconciliation =
        StorageFolderReconciliation.DeletionEligible(paths.map { storageFolder(it) })

    private fun stats(newestObjectModificationTimeMillis: Long): StorageSizeStats =
        StorageSizeStats(objectCount = 1, totalSizeBytes = 1, newestObjectModificationTimeMillis = newestObjectModificationTimeMillis)
}
