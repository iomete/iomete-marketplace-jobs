package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.candidate.HeldFolders
import com.iomete.cleanup.untrackedtablefolders.logging.SafeText
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.DeletionFenceReport
import com.iomete.cleanup.untrackedtablefolders.storage.DirectoryCleanupOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.FenceKeepReason
import com.iomete.cleanup.untrackedtablefolders.storage.FenceStop
import com.iomete.cleanup.untrackedtablefolders.storage.FenceStopCause
import com.iomete.cleanup.untrackedtablefolders.storage.FolderDeletionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.FolderDeletionResult
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
            "other_database_tables,other_session_catalogs,final_recheck_before_deletion," +
                "catalogs_outside_session,hidden_tables,nested_namespace_tables,files_outside_table_folders",
            details["ownership_not_checked"],
        )
        val listed = details.getValue("ownership_checks").split(",") + details.getValue("ownership_not_checked").split(",")
        assertEquals(listed.distinct(), listed)
        assertFalse("ownership_unreadable_sample" in details)
        assertFalse("ownership_unreadable_truncated" in details)
    }

    @Test
    fun `an empty fence report adds no key`() {
        assertEquals(emptyMap<String, String>(), builder.deletionFenceDetails(DeletionFenceReport()))
    }

    @Test
    fun `the fence report states kept folders by reason, the partly deleted folder, what was not attempted and why it stopped`() {
        val folder = "s3a://bucket/db/orphan_a"
        val report =
            DeletionFenceReport(
                keptPaths =
                    mapOf(
                        FenceKeepReason.CHANGED_SINCE_INSPECTION to listOf("s3a://bucket/db/changed"),
                        FenceKeepReason.CLAIMED to listOf("s3a://bucket/db/claimed"),
                    ),
                partialFolder =
                    FolderDeletionResult(
                        folderPath = folder,
                        outcome = FolderDeletionOutcome.PARTIAL_FAILED,
                        listedObjectCount = 4,
                        deletedKeys = listOf("a.parquet"),
                        remainingKeys = listOf("c.parquet"),
                        unknownKeys = listOf("b.parquet"),
                        vanishedKeys = listOf("d.parquet"),
                        leftoverObjectPaths = listOf("$folder/new.parquet"),
                    ),
                notAttemptedPaths = listOf("s3a://bucket/db/orphan_b"),
                stop = FenceStop(FenceStopCause.SNAPSHOT_EXPIRED, "expired"),
                ownershipRefreshCount = 2,
            )

        val details = builder.deletionFenceDetails(report)

        assertEquals(
            mapOf(
                "folder_kept_changed_since_inspection_count" to "1",
                "folder_kept_changed_since_inspection_paths_sample" to "s3a://bucket/db/changed",
                "folder_kept_changed_since_inspection_paths_truncated" to "false",
                "folder_kept_at_final_check_count" to "1",
                "folder_kept_at_final_check_paths_sample" to "s3a://bucket/db/claimed",
                "folder_kept_at_final_check_paths_truncated" to "false",
                "partially_deleted_folder_path" to folder,
                "partially_deleted_folder_listed_object_count" to "4",
                "partially_deleted_folder_deleted_object_count" to "1",
                "partially_deleted_folder_deleted_keys_sample" to "a.parquet",
                "partially_deleted_folder_deleted_keys_truncated" to "false",
                "partially_deleted_folder_remaining_object_count" to "1",
                "partially_deleted_folder_remaining_keys_sample" to "c.parquet",
                "partially_deleted_folder_remaining_keys_truncated" to "false",
                "partially_deleted_folder_unknown_state_object_count" to "1",
                "partially_deleted_folder_unknown_state_keys_sample" to "b.parquet",
                "partially_deleted_folder_unknown_state_keys_truncated" to "false",
                "partially_deleted_folder_vanished_object_count" to "1",
                "partially_deleted_folder_vanished_keys_sample" to "d.parquet",
                "partially_deleted_folder_vanished_keys_truncated" to "false",
                "partially_deleted_folder_unlisted_object_count" to "1",
                "partially_deleted_folder_unlisted_keys_sample" to "new.parquet",
                "partially_deleted_folder_unlisted_keys_truncated" to "false",
                "partially_deleted_folder_directory_cleanup" to "not_run",
                "folder_not_attempted_count" to "1",
                "folder_not_attempted_paths_sample" to "s3a://bucket/db/orphan_b",
                "folder_not_attempted_paths_truncated" to "false",
                "deletion_stop_cause" to "snapshot_expired_during_deletion",
                "ownership_snapshot_max_age_seconds" to "300",
                "ownership_refresh_count" to "2",
            ),
            details,
        )
    }

    @Test
    fun `directories are stated for the partly deleted folder, with the delete that may still complete, and for deleted folders`() {
        val folder = "s3a://bucket/db/orphan_a"
        val report =
            DeletionFenceReport(
                partialFolder =
                    FolderDeletionResult(
                        folderPath = folder,
                        outcome = FolderDeletionOutcome.PARTIAL_FAILED,
                        listedObjectCount = 1,
                        deletedKeys = listOf("a.parquet"),
                        leftoverDirectoryPaths = listOf("$folder/sub", folder),
                        unknownDirectoryPaths = listOf("$folder/sub/x\nFORGED=1"),
                        directoryCleanup = DirectoryCleanupOutcome.NOT_FINISHED,
                    ),
                keptDirectoryPaths = listOf("s3a://bucket/db/orphan_0/refused"),
            )

        val details = builder.deletionFenceDetails(report)

        assertEquals("not_finished", details["partially_deleted_folder_directory_cleanup"])
        assertEquals("2", details["partially_deleted_folder_kept_directory_count"])
        assertEquals("$folder/sub\n$folder", details["partially_deleted_folder_kept_directory_paths_sample"])
        assertEquals("false", details["partially_deleted_folder_kept_directory_paths_truncated"])
        assertEquals("1", details["partially_deleted_folder_unknown_state_directory_count"])
        assertEquals("$folder/sub/x\\u000AFORGED=1", details["partially_deleted_folder_unknown_state_directory_paths_sample"])
        assertEquals("false", details["partially_deleted_folder_unknown_state_directory_paths_truncated"])
        assertEquals("1", details["deleted_folder_kept_directory_count"])
        assertEquals("s3a://bucket/db/orphan_0/refused", details["deleted_folder_kept_directory_paths_sample"])
        assertEquals("false", details["deleted_folder_kept_directory_paths_truncated"])
    }

    @Test
    fun `the partly deleted folder path and every key sample are escaped, one entry per line`() {
        val folder = "s3a://bucket/db/orphan\u001B[2J\nFORGED=1"
        val report =
            DeletionFenceReport(
                partialFolder =
                    FolderDeletionResult(
                        folderPath = folder,
                        outcome = FolderDeletionOutcome.PARTIAL_FAILED,
                        listedObjectCount = 4,
                        deletedKeys = listOf("d\nFORGED=1.parquet"),
                        remainingKeys = listOf("r\u001B.parquet"),
                        unknownKeys = listOf("u\r.parquet"),
                        vanishedKeys = listOf("v\u202E.parquet"),
                        leftoverObjectPaths = listOf("$folder/n\u0000.parquet"),
                    ),
            )

        val details = builder.deletionFenceDetails(report)

        assertEquals("s3a://bucket/db/orphan\\u001B[2J\\u000AFORGED=1", details["partially_deleted_folder_path"])
        assertEquals("d\\u000AFORGED=1.parquet", details["partially_deleted_folder_deleted_keys_sample"])
        assertEquals("r\\u001B.parquet", details["partially_deleted_folder_remaining_keys_sample"])
        assertEquals("u\\u000D.parquet", details["partially_deleted_folder_unknown_state_keys_sample"])
        assertEquals("v\\u202E.parquet", details["partially_deleted_folder_vanished_keys_sample"])
        assertEquals("n\\u0000.parquet", details["partially_deleted_folder_unlisted_keys_sample"])
        details.forEach { (key, value) -> assertTrue(value.none { it.isISOControl() || it == '\u202E' }, "$key=$value") }
    }

    @Test
    fun `remaining keys and kept folders are bounded samples whose entries are made safe`() {
        val keys = (0..MAX_AUDIT_PATH_SAMPLE_SIZE).map { "part-$it\nforged=1.parquet" }
        val report =
            DeletionFenceReport(
                keptPaths = mapOf(FenceKeepReason.TOO_NEW to keys.map { "s3a://bucket/db/$it" }),
                partialFolder =
                    FolderDeletionResult("s3a://bucket/db/t", FolderDeletionOutcome.PARTIAL_STOPPED, keys.size + 1, deletedKeys = listOf("x"), remainingKeys = keys),
            )

        val details = builder.deletionFenceDetails(report)

        val sample = details.getValue("partially_deleted_folder_remaining_keys_sample").split("\n")
        assertEquals(MAX_AUDIT_PATH_SAMPLE_SIZE, sample.size)
        assertTrue(sample.all { it.startsWith("part-") && "forged" in it }, sample.first())
        assertEquals("true", details["partially_deleted_folder_remaining_keys_truncated"])
        assertEquals((MAX_AUDIT_PATH_SAMPLE_SIZE + 1).toString(), details["folder_kept_too_new_count"])
        assertEquals(MAX_AUDIT_PATH_SAMPLE_SIZE, details.getValue("folder_kept_too_new_paths_sample").split("\n").size)
        assertEquals("true", details["folder_kept_too_new_paths_truncated"])
        assertFalse(details.containsKey("deletion_stop_cause"))
    }

    @Test
    fun `what could not be read is a bounded sample with a truncation flag`() {
        val unreadable = (0..MAX_AUDIT_PATH_SAMPLE_SIZE).map { "catalog=c$it: the database list could not be read (ConnectException)" }

        val details = builder.ownershipCheckDetails(OwnershipChecks(unreadableStorage = unreadable))

        assertEquals(unreadable.take(MAX_AUDIT_PATH_SAMPLE_SIZE).joinToString("\n"), details["ownership_unreadable_sample"])
        assertEquals("true", details["ownership_unreadable_truncated"])
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

    @Test
    fun `cleanup roots are written in order with their folder counts and kept when planning fails`() {
        val planned =
            ScanPlanReport(
                roots =
                    listOf(
                        ScanRootResult(PlannedScanRoot("s3a://b/lake/sales", setOf(ScanRootSource.REST_NAMESPACE), cleanupRoot = true), ScanRootOutcome.SCANNED, 1, listedFolderCount = 3),
                        ScanRootResult(PlannedScanRoot("s3a://b/lake/sales.db", setOf(ScanRootSource.DATABASE_LOCATION), cleanupRoot = true), ScanRootOutcome.SCANNED, 1, listedFolderCount = 2),
                    ),
                uncoveredTableLocations = emptyList(),
                cleanupRoots = listOf("s3a://b/lake/sales.db", "s3a://b/lake/sales"),
                managedRootStatus = "used",
            )

        val details = builder.scanPlanDetails(planned)

        assertEquals("s3a://b/lake/sales.db\ns3a://b/lake/sales", details["cleanup_roots"])
        assertEquals("s3a://b/lake/sales.db=2\ns3a://b/lake/sales=3", details["cleanup_root_folder_counts"])
        assertEquals("used", details["managed_root_status"])

        val failed =
            builder.scanPlanDetails(
                ScanPlanReport(
                    roots = emptyList(),
                    uncoveredTableLocations = emptyList(),
                    planningError = "boom",
                    cleanupRoots = listOf("s3a://b/lake/sales.db"),
                    managedRootStatus = "not_applicable",
                ),
            )
        assertEquals(
            mapOf(
                "scan_plan_status" to "failed",
                "scan_plan_error" to "boom",
                "cleanup_roots" to "s3a://b/lake/sales.db",
                "managed_root_status" to "not_applicable",
            ),
            failed,
        )

        val refusedDatabase = builder.scanPlanDetails(ScanPlanReport(roots = emptyList(), uncoveredTableLocations = emptyList()))
        assertFalse("cleanup_roots" in refusedDatabase)
        assertFalse("managed_root_status" in refusedDatabase)
        assertFalse("cleanup_root_folder_counts" in refusedDatabase)
    }

    @Test
    fun `a newline or control character in a path stays inside one escaped sample entry`() {
        val details =
            builder.build(
                activeTableLocations = listOf("s3a://b/db/a\nFORGED=1", "s3a://b/db/b\u001B[2J"),
                deletionFailedFolderPath = "s3a://b/db/c\r\nFORGED",
            )

        assertEquals("s3a://b/db/a\\u000AFORGED=1\ns3a://b/db/b\\u001B[2J", details["active_table_locations_sample"])
        assertEquals(2, details.getValue("active_table_locations_sample").lines().size)
        assertEquals("s3a://b/db/c\\u000D\\u000AFORGED", details["deletion_failed_folder_path"])

        val plan =
            builder.scanPlanDetails(
                ScanPlanReport(
                    roots = emptyList(),
                    uncoveredTableLocations = emptyList(),
                    planningError = "boom",
                    cleanupRoots = listOf("s3a://b/lake/x\ny"),
                ),
            )
        assertEquals("s3a://b/lake/x\\u000Ay", plan["cleanup_roots"])
    }

    @Test
    fun `what could not be read is free text, so credential-like parts are redacted`() {
        val details =
            builder.ownershipCheckDetails(
                OwnershipChecks(unreadableStorage = listOf("catalog=c: listing failed (secret=CANARY_UNREADABLE)\nnext")),
            )

        assertEquals("catalog=c: listing failed (secret=***)\\u000Anext", details["ownership_unreadable_sample"])
    }

    @Test
    fun `a scan plan error is sanitized and bounded`() {
        val error = "java.lang.IllegalStateException: token=CANARY_PLAN \u001B[31m" + "x".repeat(1_000)

        val details =
            builder.scanPlanDetails(ScanPlanReport(roots = emptyList(), uncoveredTableLocations = emptyList(), planningError = error))

        val written = details.getValue("scan_plan_error")
        assertTrue(written.startsWith("java.lang.IllegalStateException: token=*** \\u001B[31mxxx"), written)
        assertTrue(written.endsWith(SafeText.TRUNCATION_MARKER), written)
        assertTrue(written.length <= 500, written)
        assertFalse(written.contains("CANARY"))
    }

    @Test
    fun `age details name the configured age and the deletion-mode minimum`() {
        assertEquals(mapOf("configured_older_than_hours" to "0", "deletion_min_age_hours" to "72"), builder.ageDetails(0))
        assertEquals(mapOf("configured_older_than_hours" to "100", "deletion_min_age_hours" to "72"), builder.ageDetails(100))
    }

    @Test
    fun `folders deletion mode would hold are counted with a bounded escaped sample`() {
        val paths = (0 until 101).map { "s3a://b/db/young_%03d".format(it) } + "s3a://b/db/a\nFORGED"

        val details = builder.wouldBeHeldByDeletionFloorDetails(paths)

        assertEquals("102", details["would_be_held_by_deletion_floor_count"])
        assertEquals(paths.take(100), details.getValue("would_be_held_by_deletion_floor_paths_sample").split("\n"))
        assertEquals("true", details["would_be_held_by_deletion_floor_paths_truncated"])
        assertEquals(3, details.size)

        val escaped = builder.wouldBeHeldByDeletionFloorDetails(listOf("s3a://b/db/a\nFORGED"))
        assertEquals("s3a://b/db/a\\u000AFORGED", escaped["would_be_held_by_deletion_floor_paths_sample"])
        assertEquals("false", escaped["would_be_held_by_deletion_floor_paths_truncated"])
        assertEquals(emptyMap<String, String>(), builder.wouldBeHeldByDeletionFloorDetails(emptyList()))
    }

    @Test
    fun `too new folders are counted with a bounded escaped sample`() {
        val paths = (0 until 101).map { "s3a://b/db/young_%03d".format(it) } + "s3a://b/db/a\nFORGED"

        val details = builder.tooNewDetails(paths)

        assertEquals("102", details["too_new_count"])
        assertEquals(paths.take(100), details.getValue("too_new_paths_sample").split("\n"))
        assertEquals("true", details["too_new_paths_truncated"])
        assertEquals(3, details.size)
        assertEquals(
            mapOf("too_new_count" to "1", "too_new_paths_sample" to "s3a://b/db/a\\u000AFORGED", "too_new_paths_truncated" to "false"),
            builder.tooNewDetails(listOf("s3a://b/db/a\nFORGED")),
        )
        assertEquals(emptyMap<String, String>(), builder.tooNewDetails(emptyList()))
    }

    @Test
    fun `held folders are one total and one count per reason, and nothing when none is held`() {
        val held =
            HeldFolders.of(
                inspectionOutcomes = mapOf(CandidateInspectionOutcome.EMPTY_FOLDER to listOf("s3a://b/db/empty")),
                tooNewPaths = listOf("s3a://b/db/young_a", "s3a://b/db/young_b"),
                fence = DeletionFenceReport(keptPaths = mapOf(FenceKeepReason.CHANGED_SINCE_INSPECTION to listOf("s3a://b/db/moved"))),
            )

        assertEquals(
            mapOf(
                "held_folder_count" to "4",
                "held_folder_counts" to "empty_folder=1\ntoo_new=2\nfolder_kept_changed_since_inspection=1",
            ),
            builder.heldFolderDetails(held),
        )
        assertEquals(emptyMap<String, String>(), builder.heldFolderDetails(HeldFolders.of(emptyMap(), emptyList(), DeletionFenceReport())))
    }
}
