package com.iomete.cleanup.untrackedtablefolders.candidate

import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class UntrackedFolderCandidateDetectorTest {
    private val detector = UntrackedFolderCandidateDetector()

    @Test
    fun `excludes active table locations from candidates`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/active_table", modificationTimeMillis = 100),
                storageFolder("s3a://bucket/db/deleted_table", modificationTimeMillis = 100),
            ),
            activeTableLocations = listOf("s3a://bucket/db/active_table"),
            excludedPaths = emptyList(),
            cutoffTimeMillis = 200,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/deleted_table"),
            candidates.map { it.path },
        )
    }

    @Test
    fun `excludes configured paths from candidates`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/excluded_table", modificationTimeMillis = 100),
                storageFolder("s3a://bucket/db/deleted_table", modificationTimeMillis = 100),
            ),
            activeTableLocations = emptyList(),
            excludedPaths = listOf("s3a://bucket/db/excluded_table"),
            cutoffTimeMillis = 200,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/deleted_table"),
            candidates.map { it.path },
        )
    }

    @Test
    fun `excludes resolved database folder paths from candidates`() {
        val resolvedDatabaseFolderExclusion = "s3a://bucket/db/customer_events"

        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/customer_events", modificationTimeMillis = 100),
                storageFolder("s3a://bucket/db/deleted_table", modificationTimeMillis = 100),
            ),
            activeTableLocations = emptyList(),
            excludedPaths = listOf(resolvedDatabaseFolderExclusion),
            cutoffTimeMillis = 200,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/deleted_table"),
            candidates.map { it.path },
        )
    }

    @Test
    fun `normalizes trailing slashes before comparing paths`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/active_table/", modificationTimeMillis = 100),
                storageFolder("s3a://bucket/db/excluded_table/", modificationTimeMillis = 100),
                storageFolder("s3a://bucket/db/deleted_table/", modificationTimeMillis = 100),
            ),
            activeTableLocations = listOf("s3a://bucket/db/active_table"),
            excludedPaths = listOf("s3a://bucket/db/excluded_table"),
            cutoffTimeMillis = 200,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/deleted_table/"),
            candidates.map { it.path },
        )
    }

    @Test
    fun `filters out folders newer than cutoff time`() {
        val candidates = detector.detectCandidates(
            storageFolders = listOf(
                storageFolder("s3a://bucket/db/old_deleted_table", modificationTimeMillis = 100),
                storageFolder("s3a://bucket/db/new_deleted_table", modificationTimeMillis = 300),
            ),
            activeTableLocations = emptyList(),
            excludedPaths = emptyList(),
            cutoffTimeMillis = 200,
            maxCandidateFolders = 10,
        )

        assertEquals(
            listOf("s3a://bucket/db/old_deleted_table"),
            candidates.map { it.path },
        )
    }

    @Test
    fun `throws when candidate count exceeds configured maximum`() {
        assertThrows(TooManyCandidateFoldersException::class.java) {
            detector.detectCandidates(
                storageFolders = listOf(
                    storageFolder("s3a://bucket/db/deleted_table_1", modificationTimeMillis = 100),
                    storageFolder("s3a://bucket/db/deleted_table_2", modificationTimeMillis = 100),
                ),
                activeTableLocations = emptyList(),
                excludedPaths = emptyList(),
                cutoffTimeMillis = 200,
                maxCandidateFolders = 1,
            )
        }
    }

    private fun storageFolder(
        path: String,
        modificationTimeMillis: Long,
    ): StorageFolder =
        StorageFolder(
            path = path,
            modificationTimeMillis = modificationTimeMillis,
        )
}
