package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.PlannedScanRoot
import com.iomete.cleanup.untrackedtablefolders.storage.ScanPlanReport
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootRejection
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootResult
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootSource
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CleanupAuditDiagnosticDetailsBuilderTest {

    private val builder = CleanupAuditDiagnosticDetailsBuilder()

    @Test
    fun `each check of other storage is listed as checked only when its read finished`() {
        val checks = OwnershipChecks(otherDatabaseFolders = true, otherTables = false, finalRecheck = false)

        val details = builder.ownershipCheckDetails(checks)

        assertEquals(
            "visible_tables_of_this_database,write_paths_of_visible_tables_of_this_database," +
                "default_folders_of_visible_other_databases,configured_exclusions",
            details["ownership_checks"],
        )
        assertEquals(
            "other_database_tables,same_bucket_session_catalogs,final_recheck_before_deletion,other_bucket_session_catalogs," +
                "catalogs_outside_session,hidden_tables,nested_namespace_tables,files_outside_table_folders",
            details["ownership_not_checked"],
        )
        val listed = details.getValue("ownership_checks").split(",") + details.getValue("ownership_not_checked").split(",")
        assertEquals(listed.distinct(), listed)
    }

    @Test
    fun `always emits active table location sample and truncation flag`() {
        val details = builder.build(activeTableLocations = emptyList())

        assertEquals("", details["active_table_locations_sample"])
        assertEquals("false", details["active_table_locations_truncated"])
        assertEquals(setOf("active_table_locations_sample", "active_table_locations_truncated"), details.keys)
    }

    @Test
    fun `adds storage folder section only when storage folder paths are present`() {
        val withStorage = builder.build(
            activeTableLocations = listOf("s3a://bucket/db/active_table"),
            storageFolderPaths = listOf("s3a://bucket/db/orphan"),
        )

        assertTrue(withStorage.containsKey("storage_folder_paths_sample"))
        assertTrue(withStorage.containsKey("storage_folder_paths_truncated"))

        val withoutStorage = builder.build(
            activeTableLocations = listOf("s3a://bucket/db/active_table"),
        )

        assertFalse(withoutStorage.containsKey("storage_folder_paths_sample"))
        assertFalse(withoutStorage.containsKey("storage_folder_paths_truncated"))
    }

    @Test
    fun `adds candidate folder section only when candidate paths are present`() {
        val withCandidates = builder.build(
            activeTableLocations = emptyList(),
            candidateFolderPaths = listOf("s3a://bucket/db/orphan"),
        )

        assertTrue(withCandidates.containsKey("candidate_folder_paths_sample"))
        assertTrue(withCandidates.containsKey("candidate_folder_paths_truncated"))

        val withoutCandidates = builder.build(activeTableLocations = emptyList())

        assertFalse(withoutCandidates.containsKey("candidate_folder_paths_sample"))
        assertFalse(withoutCandidates.containsKey("candidate_folder_paths_truncated"))
    }

    @Test
    fun `adds non candidate storage folder section only when paths are present`() {
        val withNonCandidates = builder.build(
            activeTableLocations = emptyList(),
            nonCandidateStorageFolderPaths = listOf("s3a://bucket/db/active_table"),
        )

        assertTrue(withNonCandidates.containsKey("non_candidate_storage_folder_paths_sample"))
        assertTrue(withNonCandidates.containsKey("non_candidate_storage_folder_paths_truncated"))

        val withoutNonCandidates = builder.build(activeTableLocations = emptyList())

        assertFalse(withoutNonCandidates.containsKey("non_candidate_storage_folder_paths_sample"))
        assertFalse(withoutNonCandidates.containsKey("non_candidate_storage_folder_paths_truncated"))
    }

    @Test
    fun `truncates samples to first 100 paths and flags truncation`() {
        val manyPaths = (1..150).map { "s3a://bucket/db/folder_${"%03d".format(it)}" }

        val details = builder.build(activeTableLocations = manyPaths)

        val sample = details["active_table_locations_sample"]!!.split("\n")
        assertEquals(100, sample.size)
        assertEquals("s3a://bucket/db/folder_001", sample.first())
        assertEquals("s3a://bucket/db/folder_100", sample.last())
        assertEquals("true", details["active_table_locations_truncated"])
    }

    @Test
    fun `does not flag truncation when sample size exactly matches limit`() {
        val exactlyHundredPaths = (1..100).map { "s3a://bucket/db/folder_${"%03d".format(it)}" }

        val details = builder.build(activeTableLocations = exactlyHundredPaths)

        assertEquals("false", details["active_table_locations_truncated"])
    }

    @Test
    fun `emits all four sections when every input is populated`() {
        val details = builder.build(
            activeTableLocations = listOf("s3a://bucket/db/active"),
            storageFolderPaths = listOf("s3a://bucket/db/active", "s3a://bucket/db/orphan"),
            candidateFolderPaths = listOf("s3a://bucket/db/orphan"),
            nonCandidateStorageFolderPaths = listOf("s3a://bucket/db/active"),
        )

        assertEquals(
            setOf(
                "active_table_locations_sample",
                "active_table_locations_truncated",
                "storage_folder_paths_sample",
                "storage_folder_paths_truncated",
                "candidate_folder_paths_sample",
                "candidate_folder_paths_truncated",
                "non_candidate_storage_folder_paths_sample",
                "non_candidate_storage_folder_paths_truncated",
            ),
            details.keys,
        )
    }

    @Test
    fun `adds unscanned table location count and sample only when present`() {
        val withUnscanned =
            builder.build(
                activeTableLocations = listOf("s3a://bucket/db/orders"),
                unscannedTableLocations = listOf("s3a://bucket/other/events"),
            )

        assertEquals("1", withUnscanned["unscanned_table_location_count"])
        assertEquals("s3a://bucket/other/events", withUnscanned["unscanned_table_locations_sample"])
        assertEquals("false", withUnscanned["unscanned_table_locations_truncated"])
        assertFalse(builder.build(activeTableLocations = emptyList()).containsKey("unscanned_table_location_count"))
    }

    @Test
    fun `adds the failed deletion path only when present`() {
        assertEquals(
            "s3a://bucket/db/b",
            builder.build(activeTableLocations = emptyList(), deletionFailedFolderPath = "s3a://bucket/db/b")["deletion_failed_folder_path"],
        )
        assertFalse(builder.build(activeTableLocations = emptyList()).containsKey("deletion_failed_folder_path"))
    }

    @Test
    fun `scan coverage reports a zero count only when asked to report complete coverage`() {
        assertEquals(emptyMap<String, String>(), builder.scanCoverageDetails(emptyList()))
        assertEquals(mapOf("unscanned_table_location_count" to "0"), builder.scanCoverageDetails(emptyList(), reportCompleteScanCoverage = true))
        assertEquals(
            mapOf(
                "unscanned_table_location_count" to "1",
                "unscanned_table_locations_sample" to "s3a://bucket/other/events",
                "unscanned_table_locations_truncated" to "false",
            ),
            builder.scanCoverageDetails(listOf("s3a://bucket/other/events"), reportCompleteScanCoverage = true),
        )
    }

    @Test
    fun `scan plan keys are written only when a plan exists`() {
        assertEquals(emptyMap<String, String>(), builder.scanPlanDetails(null))

        val plan =
            ScanPlanReport(
                roots =
                    listOf(
                        ScanRootResult(PlannedScanRoot("s3a://b/lake/sales.db", setOf(ScanRootSource.DATABASE_LOCATION), cleanupRoot = true), ScanRootOutcome.SCANNED, 1),
                        ScanRootResult(
                            PlannedScanRoot("s3a://b/lake/sales", setOf(ScanRootSource.REST_NAMESPACE), cleanupRoot = false),
                            ScanRootOutcome.SCANNED,
                            0,
                            listOf("s3a://b/lake/sales/x"),
                        ),
                        ScanRootResult(
                            PlannedScanRoot("s3a://o/t", setOf(ScanRootSource.TABLE_PARENT), cleanupRoot = false, rejection = ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY),
                            ScanRootOutcome.NOT_SCANNED,
                            1,
                        ),
                    ),
                uncoveredTableLocations = listOf("s3a://o/t/t2"),
            )

        val details = builder.scanPlanDetails(plan)

        assertEquals("visible_catalog_tables", details["ownership_basis"])
        assertEquals("3", details["scan_root_count"])
        assertEquals(
            "s3a://b/lake/sales.db=cleanup:scanned:visible_tables=1\n" +
                "s3a://b/lake/sales=report_only:scanned:visible_tables=0\n" +
                "s3a://o/t=not_scanned:outside_database_boundary:visible_tables=1",
            details["scan_roots_sample"],
        )
        assertEquals("partial", details["scan_plan_coverage"])
        assertEquals("1", details["scan_plan_uncovered_table_location_count"])
        assertEquals("s3a://o/t/t2", details["scan_plan_uncovered_table_locations_sample"])
        assertEquals("1", details["report_only_folder_count"])
        assertEquals("s3a://b/lake/sales/x", details["report_only_folders_sample"])
    }

    @Test
    fun `inspection outcome details carry a count and a bounded sample per outcome`() {
        val emptyFolders = (1..101).map { "s3a://b/db/empty_$it" }

        val details =
            builder.inspectionOutcomeDetails(
                mapOf(
                    CandidateInspectionOutcome.FOLDER_NOT_FOUND to listOf("s3a://b/db/gone"),
                    CandidateInspectionOutcome.EMPTY_FOLDER to emptyFolders,
                    CandidateInspectionOutcome.UNKNOWN_AGE to emptyList(),
                ),
            )

        assertEquals("1", details["folder_not_found_count"])
        assertEquals("s3a://b/db/gone", details["folder_not_found_paths_sample"])
        assertEquals("false", details["folder_not_found_paths_truncated"])
        assertEquals("101", details["empty_folder_count"])
        assertEquals(emptyFolders.take(100).joinToString("\n"), details["empty_folder_paths_sample"])
        assertEquals("true", details["empty_folder_paths_truncated"])
        assertFalse(details.keys.any { it.startsWith("unknown_age") || it.startsWith("statistics_unavailable") })
    }

    @Test
    fun `deletion-time not-found details are separate from inspection outcomes and appear only when present`() {
        val details = builder.notFoundAtDeletionDetails(listOf("s3a://b/db/gone"))

        assertEquals("1", details["folder_not_found_at_deletion_count"])
        assertEquals("s3a://b/db/gone", details["folder_not_found_at_deletion_paths_sample"])
        assertEquals("false", details["folder_not_found_at_deletion_paths_truncated"])
        assertFalse(details.containsKey("folder_not_found_count"))
        assertEquals(emptyMap<String, String>(), builder.notFoundAtDeletionDetails(emptyList()))
    }
}
