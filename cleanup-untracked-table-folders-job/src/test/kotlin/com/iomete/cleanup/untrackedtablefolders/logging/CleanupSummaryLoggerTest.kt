package com.iomete.cleanup.untrackedtablefolders.logging

import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CleanupSummaryLoggerTest {

    private val logger = CleanupSummaryLogger()

    private fun summary(
        unresolvedTableNames: List<String> = emptyList(),
        deletionEligible: Boolean = true,
        unmatchedFolderPaths: List<String> = listOf("s3a://bucket/db/old_1"),
        deletedFolderPaths: List<String> = emptyList(),
    ) = CleanupSummary(
        catalog = "spark_catalog",
        database = "analytics",
        discoveredDatabaseLocation = "s3a://bucket/db",
        storageScanLocation = "s3a://bucket/db",
        catalogTableCount = 3,
        unresolvedTableNames = unresolvedTableNames,
        activeTableLocations = listOf("s3a://bucket/db/table_a"),
        storageFolderPaths = listOf("s3a://bucket/db/table_a", "s3a://bucket/db/old_1"),
        excludedPaths = emptyList(),
        unmatchedFolderPaths = unmatchedFolderPaths,
        unmatchedSizeStats = StorageSizeStats(objectCount = 2, totalSizeBytes = 2048),
        deletionEligible = deletionEligible,
        deletedFolderPaths = deletedFolderPaths,
        deletedSizeStats = StorageSizeStats.ZERO,
    )

    @Test
    fun `report is surrounded by three blank lines`() {
        val lines = logger.buildReportLines(summary())

        assertEquals(listOf("", "", ""), lines.take(3))
        assertEquals(listOf("", "", ""), lines.takeLast(3))
        assertTrue(lines[3].startsWith("========"))
        assertTrue(lines[lines.size - 4].startsWith("========"))
    }

    @Test
    fun `ownership-unverified report names the unresolved tables and explains the block`() {
        val lines =
            logger.buildReportLines(
                summary(
                    unresolvedTableNames = listOf("spark_catalog.analytics.table_b", "spark_catalog.analytics.table_c"),
                    deletionEligible = false,
                    unmatchedFolderPaths = listOf("s3a://bucket/db/old_1", "s3a://bucket/db/table_b"),
                ),
            )
        val report = lines.joinToString("\n")

        assertTrue(report.contains("TABLES WHOSE STORAGE LOCATION COULD NOT BE RESOLVED"))
        assertTrue(report.contains("- spark_catalog.analytics.table_b"))
        assertTrue(report.contains("- spark_catalog.analytics.table_c"))
        assertTrue(report.contains("POTENTIALLY UNTRACKED STORAGE FOLDERS"))
        assertTrue(report.contains("  Because 2 catalog table(s) have unknown storage locations,"))
        assertTrue(report.contains("  these 2 unmatched storage folders cannot be proven safe to delete."))
        assertTrue(lines.none { it.length > 100 })
        assertTrue(report.contains("- s3a://bucket/db/old_1"))
        assertTrue(report.contains("  BLOCKED"))
        assertTrue(report.contains("2 catalog table(s) have unresolved storage ownership."))
    }

    @Test
    fun `deletion-eligible report does not claim folders are only potentially untracked`() {
        val report = logger.buildReportLines(summary(deletedFolderPaths = listOf("s3a://bucket/db/old_1"))).joinToString("\n")

        assertTrue(report.contains("UNTRACKED STORAGE FOLDERS"))
        assertTrue(report.contains("not referenced by the discovered catalog tables"))
        assertFalse(report.contains("POTENTIALLY UNTRACKED"))
        assertFalse(report.contains("BLOCKED"))
        assertTrue(report.contains("Folders deleted"))
        assertFalse(report.contains("TABLES WHOSE STORAGE LOCATION COULD NOT BE RESOLVED"))
    }

    @Test
    fun `Tables found counts resolved and unresolved catalog tables together`() {
        val lines =
            logger.buildReportLines(
                summary(
                    unresolvedTableNames = listOf("spark_catalog.analytics.c", "spark_catalog.analytics.d"),
                    deletionEligible = false,
                ).copy(catalogTableCount = 4, activeTableLocations = listOf("s3a://bucket/db/a", "s3a://bucket/db/b")),
            )
        val report = lines.joinToString("\n")

        assertTrue(report.contains("Tables found                                     4"))
        assertTrue(report.contains("Tables with verified storage locations           2"))
        assertTrue(report.contains("Tables whose storage location could not be read  2"))
    }

    @Test
    fun `report distinguishes folders owned by verified tables from unmatched folders`() {
        val report = logger.buildReportLines(summary()).joinToString("\n")

        assertTrue(report.contains("Folders discovered                               2"))
        assertTrue(report.contains("Folders belonging to verified catalog tables     1"))
        assertTrue(report.contains("Folders with no verified catalog owner           1"))
    }
}
