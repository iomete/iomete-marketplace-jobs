package com.iomete.cleanup.untrackedtablefolders.logging

import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.PlannedScanRoot
import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootResult
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootRejection
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootSource
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import io.mockk.verify
import org.jboss.logging.Logger
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
        dryRun: Boolean = false,
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
        dryRun = dryRun,
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
    fun `no verified location report names the tables and does not imply a storage scan`() {
        val lines =
            logger.buildNoVerifiedLocationReportLines(
                NoVerifiedLocationSummary(
                    catalog = "spark_catalog",
                    database = "analytics",
                    discoveredDatabaseLocation = "s3a://bucket/db",
                    catalogTableCount = 2,
                    unresolvedTableNames = listOf("spark_catalog.analytics.table_a", "spark_catalog.analytics.table_b"),
                    dryRun = false,
                ),
            )
        val report = lines.joinToString("\n")

        assertTrue(report.contains("Tables with verified storage locations           0"))
        assertTrue(report.contains("- spark_catalog.analytics.table_a"))
        assertTrue(report.contains("Storage was not scanned."))
        assertTrue(report.contains("  BLOCKED"))
        assertTrue(report.contains("2 catalog table(s) have unresolved storage ownership."))
        assertFalse(report.contains("Folders discovered"))
        assertFalse(report.contains("Folders scanned under"))
        assertTrue(lines.none { it.length > 100 })
    }

    @Test
    fun `too many folders report states both conditions and does not say blocked`() {
        val lines =
            logger.buildTooManyFoldersReportLines(
                TooManyFoldersSummary(
                    catalog = "spark_catalog",
                    database = "analytics",
                    discoveredDatabaseLocation = "s3a://bucket/db",
                    storageScanLocation = "s3a://bucket/db",
                    catalogTableCount = 3,
                    verifiedTableCount = 1,
                    unresolvedTableNames = listOf("spark_catalog.analytics.table_b", "spark_catalog.analytics.table_c"),
                    storageFolderCount = 13,
                    unmatchedFolderPaths = (1..12).map { "s3a://bucket/db/orphan_$it" },
                    maxCandidateFolders = 10,
                    dryRun = false,
                ),
            )
        val report = lines.joinToString("\n")

        assertTrue(report.contains("- spark_catalog.analytics.table_b"))
        assertTrue(report.contains("Folder contents were not inspected"))
        assertTrue(report.contains("  SKIPPED"))
        assertTrue(report.contains("Reason: too_many_candidate_folders"))
        assertTrue(report.contains("12 folder(s) exceed max_candidate_folders_per_database=10."))
        assertTrue(report.contains("2 catalog table(s) also have unresolved storage ownership"))
        assertFalse(report.contains("BLOCKED"))
        assertFalse(report.contains("Size of folders"))
        assertTrue(lines.none { it.length > 100 })
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

    @Test
    fun `only folders at visible table locations count as belonging to catalog tables`() {
        val report =
            logger.buildReportLines(
                summary().copy(
                    activeTableLocations = listOf("s3a://bucket/db/table_a", "s3a://bucket/db/nested/table_b"),
                    storageFolderPaths =
                        listOf(
                            "s3a://bucket/db/table_a", "s3a://bucket/db/nested", "s3a://bucket/db/excluded",
                            "s3a://bucket/db/too_new", "s3a://bucket/db/old_1",
                        ),
                    excludedPaths = listOf("s3a://bucket/db/excluded"),
                ),
            ).joinToString("\n")

        assertTrue(report.contains("Folders discovered                               5"))
        assertTrue(report.contains("Folders belonging to verified catalog tables     2"))
        assertTrue(report.contains("Folders protected or skipped for other reasons   2"))
        assertTrue(report.contains("Folders with no verified catalog owner           1"))
        assertTrue(report.contains("STORAGE FOLDERS PROTECTED OR SKIPPED FOR OTHER REASONS"))
        assertTrue(report.contains("- s3a://bucket/db/excluded\n- s3a://bucket/db/too_new"))
    }

    @Test
    fun `a folder path that cannot be compared is reported, not thrown`() {
        val lines = logger.buildReportLines(summary().copy(storageFolderPaths = listOf("s3a://bucket/db/table_a", " ")))

        assertTrue("  Folders protected or skipped for other reasons   1" in lines)
    }

    @Test
    fun `a dry run report says so and does not report deletions`() {
        val dryRun = logger.buildReportLines(summary(dryRun = true))
        val deletion = logger.buildReportLines(summary(deletedFolderPaths = listOf("s3a://bucket/db/old_1")))

        assertTrue(" Mode: dry run, nothing is deleted" in dryRun)
        assertTrue("  Dry run. No folder was deleted." in dryRun)
        assertTrue(dryRun.none { "Folders deleted" in it })
        assertTrue(" Mode: deletion enabled" in deletion)
        assertTrue(deletion.any { it.startsWith("  Folders deleted") })
    }

    @Test
    fun `report lists table locations outside the scanned root`() {
        val report =
            logger.buildReportLines(
                summary().copy(unscannedTableLocations = listOf("s3a://bucket/other/events")),
            ).joinToString("\n")

        assertTrue(report.contains("Table locations outside the scanned root         1"))
        assertTrue(report.contains("TABLE LOCATIONS OUTSIDE THE SCANNED ROOT"))
        assertTrue(report.contains("- s3a://bucket/other/events"))
    }

    @Test
    fun `report omits the outside-root section when coverage is complete`() {
        val report = logger.buildReportLines(summary()).joinToString("\n")

        assertTrue(report.contains("Table locations outside the scanned root         0"))
        assertFalse(report.contains("TABLE LOCATIONS OUTSIDE THE SCANNED ROOT"))
    }

    @Test
    fun `storage roots section appears only with a plan and never calls report-only folders deletable`() {
        assertFalse(logger.buildReportLines(summary()).contains("STORAGE ROOTS"))

        val plan =
            ScanPlanReport(
                roots =
                    listOf(
                        ScanRootResult(PlannedScanRoot("s3a://bucket/db", setOf(ScanRootSource.DATABASE_LOCATION), cleanupRoot = true), ScanRootOutcome.SCANNED, 1),
                        ScanRootResult(
                            PlannedScanRoot("s3a://bucket/rest/db", setOf(ScanRootSource.REST_NAMESPACE), cleanupRoot = false),
                            ScanRootOutcome.SCANNED,
                            0,
                            listOf("s3a://bucket/rest/db/x"),
                        ),
                    ),
                uncoveredTableLocations = emptyList(),
            )

        val lines = logger.buildReportLines(summary().copy(scanPlan = plan))

        assertTrue(lines.contains("STORAGE ROOTS"))
        assertTrue(lines.contains("- s3a://bucket/rest/db  report_only  scanned  visible tables at any depth: 0"))
        assertTrue(lines.contains("  These folders are not deleted. A table this job cannot see may still own them."))
        assertTrue(lines.contains("- s3a://bucket/rest/db/x"))
    }

    @Test
    fun `folders skipped after inspection are counted and listed by outcome, not as other reasons`() {
        val report =
            logger.buildReportLines(
                summary().copy(
                    storageFolderPaths =
                        listOf(
                            "s3a://bucket/db/table_a", "s3a://bucket/db/old_1", "s3a://bucket/db/gone",
                            "s3a://bucket/db/empty", "s3a://bucket/db/unreadable", "s3a://bucket/db/too_new",
                        ),
                    inspectionOutcomes =
                        mapOf(
                            CandidateInspectionOutcome.EMPTY_FOLDER to listOf("s3a://bucket/db/empty"),
                            CandidateInspectionOutcome.FOLDER_NOT_FOUND to listOf("s3a://bucket/db/gone"),
                            CandidateInspectionOutcome.STATISTICS_UNAVAILABLE to listOf("s3a://bucket/db/unreadable"),
                        ),
                ),
            ).joinToString("\n")

        assertTrue(report.contains("Folders discovered                               6"))
        assertTrue(report.contains("Folders protected or skipped for other reasons   1"))
        assertTrue(report.contains("Folders no longer present when inspected         1"))
        assertTrue(report.contains("Folders whose contents could not be listed       1"))
        assertTrue(report.contains("Empty folders                                    1"))
        assertFalse(report.contains("Folders of unknown age"))
        assertTrue(report.contains("STORAGE FOLDERS SKIPPED AFTER INSPECTION"))
        assertTrue(report.contains("  folder_not_found: listed under the cleanup root, but not found when inspected.\n\n- s3a://bucket/db/gone"))
        assertTrue(report.contains("  empty_folder: no objects. Empty folders, including their directory markers, are not deleted.\n\n- s3a://bucket/db/empty"))
        assertTrue(
            report.indexOf("folder_not_found:") < report.indexOf("statistics_unavailable:") &&
                report.indexOf("statistics_unavailable:") < report.indexOf("empty_folder:"),
        )
        val otherReasons = report.substringAfter("STORAGE FOLDERS PROTECTED OR SKIPPED FOR OTHER REASONS").substringBefore("STORAGE FOLDERS SKIPPED AFTER INSPECTION")
        assertTrue(otherReasons.contains("- s3a://bucket/db/too_new"))
        assertFalse(otherReasons.contains("s3a://bucket/db/gone"))
    }

    @Test
    fun `report omits the inspection section when every candidate was readable`() {
        val report = logger.buildReportLines(summary()).joinToString("\n")

        assertFalse(report.contains("STORAGE FOLDERS SKIPPED AFTER INSPECTION"))
        assertFalse(report.contains("Empty folders"))
    }

    @Test
    fun `a folder gone at deletion is reported separately and is not counted as deleted`() {
        val report =
            logger.buildReportLines(
                summary(
                    unmatchedFolderPaths = listOf("s3a://bucket/db/gone", "s3a://bucket/db/old_1"),
                    deletedFolderPaths = listOf("s3a://bucket/db/old_1"),
                ).copy(
                    storageFolderPaths = listOf("s3a://bucket/db/table_a", "s3a://bucket/db/old_1", "s3a://bucket/db/gone"),
                    notFoundAtDeletionPaths = listOf("s3a://bucket/db/gone"),
                ),
            ).joinToString("\n")

        assertTrue(report.contains("Folders with no verified catalog owner           2"))
        assertTrue(report.contains("Folders deleted                                  1"))
        assertTrue(report.contains("Folders not found at deletion                    1"))
        val deletion = report.substringAfter("\nDELETION\n")
        assertTrue(deletion.contains("- s3a://bucket/db/old_1\n\n  folder_not_found_at_deletion:"))
        assertTrue(deletion.substringAfter("folder_not_found_at_deletion:").contains("- s3a://bucket/db/gone"))
        assertFalse(report.contains("STORAGE FOLDERS SKIPPED AFTER INSPECTION"))
    }

    @Test
    fun `report omits deletion-time not-found lines when every folder was deleted`() {
        val report = logger.buildReportLines(summary(deletedFolderPaths = listOf("s3a://bucket/db/old_1"))).joinToString("\n")

        assertFalse(report.contains("not found at deletion"))
        assertFalse(report.contains("folder_not_found_at_deletion"))
    }

    @Test
    fun `every report states what ownership was and was not checked`() {
        val reports =
            listOf(
                logger.buildReportLines(summary()),
                logger.buildReportLines(summary(dryRun = true)),
                logger.buildNoVerifiedLocationReportLines(
                    NoVerifiedLocationSummary("spark_catalog", "analytics", "s3a://bucket/db", 1, listOf("spark_catalog.analytics.t"), dryRun = true),
                ),
                logger.buildTooManyFoldersReportLines(tooManySummary(unresolvedTableNames = emptyList())),
            )

        reports.forEach { lines ->
            val checks = lines.joinToString("\n").substringAfter("OWNERSHIP CHECKS").substringBefore("CATALOG")
            assertTrue(checks.contains("  Checked      tables of this database the job can see, their Iceberg write paths,"))
            assertTrue(checks.contains("  Not checked  tables of other databases, tables of other catalogs, tables the job"))
            assertTrue(checks.contains("files a table uses outside its own folder"))
            assertTrue(lines.none { it.length > 100 })
        }
    }

    @Test
    fun `unresolved tables are listed with their reason and any known folder`() {
        val report =
            logger.buildReportLines(
                summary(
                    unresolvedTableNames = listOf("spark_catalog.analytics.table_b", "spark_catalog.analytics.table_c"),
                    deletionEligible = false,
                ).copy(
                    unresolvedTableDetails =
                        mapOf(
                            "spark_catalog.analytics.table_b" to
                                UnresolvedTableDetail(
                                    "Location does not exist: s3a://bucket/db/table_b/metadata/1.metadata.json\n\tat stack frame",
                                    "s3a://bucket/db/table_b",
                                ),
                            "spark_catalog.analytics.table_c" to UnresolvedTableDetail("gc.enabled is not true", null),
                        ),
                ),
            ).joinToString("\n")

        assertTrue(
            report.contains(
                "- spark_catalog.analytics.table_b\n" +
                    "    reason: Location does not exist: s3a://bucket/db/table_b/metadata/1.metadata.json\n" +
                    "    known folder: s3a://bucket/db/table_b\n" +
                    "- spark_catalog.analytics.table_c\n" +
                    "    reason: gc.enabled is not true\n",
            ),
        )
        assertFalse(report.contains("stack frame"))
    }

    @Test
    fun `too many folders without unresolved tables lists the folders and does not mention unresolved tables`() {
        val lines = logger.buildTooManyFoldersReportLines(tooManySummary(unresolvedTableNames = emptyList()))
        val report = lines.joinToString("\n")

        assertTrue(report.contains("FOLDERS WITH NO VERIFIED CATALOG OWNER (NOT INSPECTED)"))
        assertTrue(report.contains("- s3a://bucket/db/orphan_1\n- s3a://bucket/db/orphan_10"))
        assertTrue(report.contains("Reason: too_many_candidate_folders"))
        assertTrue(report.contains("12 folder(s) exceed max_candidate_folders_per_database=10."))
        assertTrue(report.contains("Protect folders to keep with exclude_paths, or raise the limit."))
        assertFalse(report.contains("unresolved"))
        assertTrue(report.contains("No folder in this database was deleted."))
    }

    @Test
    fun `a location shared by several tables is listed once`() {
        val report =
            logger.buildReportLines(
                summary().copy(
                    activeTableLocations = listOf("s3a://bucket/db/table_a", "s3a://bucket/db/table_a"),
                    excludedPaths = listOf("s3a://bucket/db/keep", "s3a://bucket/db/keep"),
                    unscannedTableLocations = listOf("s3a://bucket/other/events", "s3a://bucket/other/events"),
                ),
            )

        assertEquals(1, report.count { it == "- s3a://bucket/db/table_a" })
        assertEquals(1, report.count { it == "- s3a://bucket/db/keep" })
        assertEquals(1, report.count { it == "- s3a://bucket/other/events" })
        assertTrue("  Table locations outside the scanned root         1" in report)
        assertTrue("  Tables with verified storage locations           2" in report)
    }

    @Test
    fun `a listed root with no child folders says so and a rejected root shows no table count`() {
        val plan =
            ScanPlanReport(
                roots =
                    listOf(
                        ScanRootResult(cleanupRoot("s3a://bucket/data/db"), ScanRootOutcome.SCANNED, 1, listedFolderCount = 3),
                        ScanRootResult(
                            PlannedScanRoot("s3a://bucket/db.db", setOf(ScanRootSource.DATABASE_LOCATION), cleanupRoot = false),
                            ScanRootOutcome.SCANNED,
                            0,
                            listedFolderCount = 0,
                        ),
                        ScanRootResult(
                            PlannedScanRoot("s3a://bucket", setOf(ScanRootSource.TABLE_PARENT), cleanupRoot = false, rejection = ScanRootRejection.BUCKET_ROOT),
                            ScanRootOutcome.NOT_SCANNED,
                            70,
                        ),
                    ),
                uncoveredTableLocations = listOf("s3a://bucket/t", "s3a://bucket/t"),
                visibleTablesOutsideCleanupRoot = 1,
            )

        val lines = logger.buildReportLines(summary().copy(scanPlan = plan))

        assertTrue("- s3a://bucket/data/db  cleanup  scanned  visible tables at any depth: 1" in lines)
        assertTrue("- s3a://bucket/db.db  report_only  scanned, no folders  visible tables at any depth: 0" in lines)
        assertTrue("- s3a://bucket  not_scanned  bucket_root" in lines)
        assertEquals(1, lines.count { it == "- s3a://bucket/t" })
    }

    @Test
    fun `coverage is complete only when every visible table is under the cleanup root`() {
        fun coverageLines(outsideCleanupRoot: Int, unresolvedTables: Int = 0) =
            logger.buildReportLines(
                summary().copy(
                    scanPlan =
                        ScanPlanReport(
                            roots = listOf(ScanRootResult(cleanupRoot("s3a://bucket/db"), ScanRootOutcome.SCANNED, 1)),
                            uncoveredTableLocations = emptyList(),
                            visibleTablesOutsideCleanupRoot = outsideCleanupRoot,
                            unresolvedTableCount = unresolvedTables,
                        ),
                ),
            )
        val partial = "  Coverage is partial: a table this job can see is outside the cleanup root or has an"

        assertTrue("  Coverage is complete: every table this job can see is under the cleanup root." in coverageLines(0))
        assertTrue(partial in coverageLines(1))
        assertTrue(partial in coverageLines(0, unresolvedTables = 1))
        assertTrue("  Coverage counts only tables this job can see. It is not an ownership check." in coverageLines(0))
    }

    @Test
    fun `each report is written as one log statement`() {
        val jbossLogger = mockk<Logger>(relaxed = true)
        mockkStatic(Logger::class)
        try {
            every { Logger.getLogger(CleanupSummaryLogger::class.java) } returns jbossLogger
            val reportLogger = CleanupSummaryLogger()

            reportLogger.logCleanupSummary(summary())
            reportLogger.logTooManyFoldersReport(tooManySummary())
            reportLogger.logNoVerifiedLocationReport(
                NoVerifiedLocationSummary("spark_catalog", "analytics", "s3a://bucket/db", 1, listOf("spark_catalog.analytics.t"), dryRun = true),
            )
            reportLogger.logScanPlan("spark_catalog", "analytics", ScanPlanReport(roots = emptyList(), uncoveredTableLocations = emptyList()))
        } finally {
            unmockkStatic(Logger::class)
        }

        verify(exactly = 1) { jbossLogger.info(logger.buildReportLines(summary()).joinToString("\n")) }
        verify(exactly = 4) { jbossLogger.info(any<Any>()) }
    }

    private fun cleanupRoot(location: String) = PlannedScanRoot(location, setOf(ScanRootSource.DATABASE_LOCATION), cleanupRoot = true)

    private fun tooManySummary(unresolvedTableNames: List<String> = listOf("spark_catalog.analytics.table_b")) =
        TooManyFoldersSummary(
            catalog = "spark_catalog",
            database = "analytics",
            discoveredDatabaseLocation = "s3a://bucket/db",
            storageScanLocation = "s3a://bucket/db",
            catalogTableCount = 3,
            verifiedTableCount = 1,
            unresolvedTableNames = unresolvedTableNames,
            storageFolderCount = 13,
            unmatchedFolderPaths = (1..12).map { "s3a://bucket/db/orphan_$it" }.sorted(),
            maxCandidateFolders = 10,
            dryRun = false,
        )
}
