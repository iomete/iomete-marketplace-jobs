package com.iomete.cleanup.untrackedtablefolders.storage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class CandidateFolderStatisticsTest {

    private val statistics =
        CandidateFolderStatistics(
            statsByFolder =
                mapOf(
                    "s3a://b/db/dated" to StorageSizeStats(objectCount = 2, totalSizeBytes = 4, newestObjectModificationTimeMillis = 100),
                    "s3a://b/db/empty" to StorageSizeStats(objectCount = 0, totalSizeBytes = 0),
                    "s3a://b/db/undated" to StorageSizeStats(objectCount = 2, totalSizeBytes = 4),
                    "s3a://b/db/zero_mtime" to StorageSizeStats(objectCount = 1, totalSizeBytes = 1, newestObjectModificationTimeMillis = 0),
                ),
            absentFolderPaths = setOf("s3a://b/db/gone"),
        )

    @Test
    fun `classifies each folder by what inspection found`() {
        assertEquals(CandidateInspectionOutcome.FOLDER_NOT_FOUND, statistics.outcome("s3a://b/db/gone"))
        assertEquals(CandidateInspectionOutcome.STATISTICS_UNAVAILABLE, statistics.outcome("s3a://b/db/unreadable"))
        assertEquals(CandidateInspectionOutcome.EMPTY_FOLDER, statistics.outcome("s3a://b/db/empty"))
        assertEquals(CandidateInspectionOutcome.UNKNOWN_AGE, statistics.outcome("s3a://b/db/undated"))
        assertEquals(CandidateInspectionOutcome.UNKNOWN_AGE, statistics.outcome("s3a://b/db/zero_mtime"))
        assertNull(statistics.outcome("s3a://b/db/dated"))
    }

    @Test
    fun `groups only the given folders, sorted, and leaves out folders of known age`() {
        assertEquals(
            mapOf(
                CandidateInspectionOutcome.FOLDER_NOT_FOUND to listOf("s3a://b/db/gone"),
                CandidateInspectionOutcome.STATISTICS_UNAVAILABLE to listOf("s3a://b/db/unreadable"),
                CandidateInspectionOutcome.UNKNOWN_AGE to listOf("s3a://b/db/undated", "s3a://b/db/zero_mtime"),
            ),
            statistics.folderPathsByOutcome(
                listOf("s3a://b/db/zero_mtime", "s3a://b/db/undated", "s3a://b/db/unreadable", "s3a://b/db/gone", "s3a://b/db/dated"),
            ),
        )
    }
}
