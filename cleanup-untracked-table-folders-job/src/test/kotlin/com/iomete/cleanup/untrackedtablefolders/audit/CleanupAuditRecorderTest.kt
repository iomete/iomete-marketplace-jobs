package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.SafeText
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSqlText
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateInspectionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.DeletionFenceReport
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.FenceKeepReason
import com.iomete.cleanup.untrackedtablefolders.storage.FenceStop
import com.iomete.cleanup.untrackedtablefolders.storage.FenceStopCause
import com.iomete.cleanup.untrackedtablefolders.storage.FolderDeletionOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.FolderDeletionResult
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.spyk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException
import java.time.Instant

private val OWNERSHIP_CHECK_KEYS = setOf("ownership_checks", "ownership_not_checked")

// Every row states the configured age and the deletion-mode minimum. The recorder below uses the default config.
private val DEFAULT_AGE_DETAILS = mapOf("configured_older_than_hours" to "24", "deletion_min_age_hours" to "72")

class CleanupAuditRecorderTest {

    private val cleanupAuditTableService = mockk<CleanupAuditTableService>(relaxed = true)
    private val excludePathResolver = mockk<ExcludePathResolver>(relaxed = true)

    private val recorder = CleanupAuditRecorder().apply {
        config = ApplicationConfig(catalog = "example_catalog", databases = listOf("analytics"))
        cleanupAuditTableService = this@CleanupAuditRecorderTest.cleanupAuditTableService
        auditDiagnosticDetailsBuilder = CleanupAuditDiagnosticDetailsBuilder()
        excludePathResolver = this@CleanupAuditRecorderTest.excludePathResolver
    }

    @Test
    fun `deletion failure writes a FAILED record with the confirmed deletions`() {
        recorder.recordDeletionFailed(
            runId = "run-4",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/db/orders"),
            storageFolderPaths = listOf("s3a://example-bucket/db/orders", "s3a://example-bucket/db/a", "s3a://example-bucket/db/b"),
            candidateFolderPaths = listOf("s3a://example-bucket/db/a", "s3a://example-bucket/db/b"),
            candidateSizeStats = StorageSizeStats(objectCount = 2, totalSizeBytes = 20),
            deletedFolderPaths = listOf("s3a://example-bucket/db/a"),
            deletedSizeStats = StorageSizeStats(objectCount = 1, totalSizeBytes = 10),
            failedFolderPath = "s3a://example-bucket/db/b",
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = listOf("s3a://example-bucket/other/events"),
            error = IllegalStateException("simulated delete failure"),
        )

        val record = capturedRecord()

        assertEquals("FAILED", record.status)
        assertEquals("deletion_failed", record.statusReason)
        assertEquals(1, record.deletedFolderCount)
        assertEquals(listOf("s3a://example-bucket/db/a"), record.deletedFolders)
        assertEquals(1L, record.deletedObjectCount)
        assertEquals("s3a://example-bucket/db/b", record.diagnosticDetails["deletion_failed_folder_path"])
        assertEquals("1", record.diagnosticDetails["unscanned_table_location_count"])
    }

    @Test
    fun `audit integrity loss writes a BLOCKED record that keeps the candidates and deletes nothing`() {
        recorder.recordDeletionBlocked(
            reason = DeletionBlockReason.AUDIT_INTEGRITY_LOST,
            runId = "run-5",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/db/orders"),
            storageFolderPaths = listOf("s3a://example-bucket/db/orders", "s3a://example-bucket/db/a"),
            candidateFolderPaths = listOf("s3a://example-bucket/db/a"),
            candidateSizeStats = null,
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = listOf("s3a://example-bucket/other/events"),
            scanPlan = null,
        )

        val record = capturedRecord()

        assertEquals("BLOCKED", record.status)
        assertEquals("audit_integrity_lost", record.statusReason)
        assertNull(record.errorMessage)
        assertEquals(1, record.candidateFolderCount)
        assertEquals(0, record.deletedFolderCount)
        assertEquals("1", record.diagnosticDetails["unscanned_table_location_count"])
    }

    @Test
    fun `a cleanup root with no visible table writes a BLOCKED record that deletes nothing`() {
        recorder.recordDeletionBlocked(
            reason = DeletionBlockReason.CLEANUP_ROOT_HAS_NO_VISIBLE_TABLE,
            runId = "run-6",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/other/orders"),
            storageFolderPaths = listOf("s3a://example-bucket/db/a"),
            candidateFolderPaths = listOf("s3a://example-bucket/db/a"),
            candidateSizeStats = null,
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = listOf("s3a://example-bucket/other/orders"),
            scanPlan = null,
        )

        val record = capturedRecord()

        assertEquals("BLOCKED", record.status)
        assertEquals("cleanup_root_has_no_visible_table", record.statusReason)
        assertEquals(listOf("s3a://example-bucket/db/a"), record.candidateFolders)
        assertEquals(0, record.deletedFolderCount)
    }

    @Test
    fun `a database that disappears after some checks keeps exactly those checks on its row`() {
        recorder.recordDatabaseNotFound(
            "run",
            "analytics",
            Instant.EPOCH,
            IllegalStateException("missing"),
            ownershipChecks = OwnershipChecks(otherDatabaseFolders = true),
        )

        val record = capturedRecord()
        assertEquals("SKIPPED", record.status)
        assertEquals("database_not_found", record.statusReason)
        assertEquals(
            "visible_tables_of_this_database,write_paths_of_visible_tables_of_this_database," +
                "default_folders_of_visible_other_databases,configured_exclusions",
            record.diagnosticDetails["ownership_checks"],
        )
        assertEquals(
            "other_database_tables,same_bucket_session_catalogs,final_recheck_before_deletion," +
                "other_bucket_session_catalogs,catalogs_outside_session,hidden_tables,nested_namespace_tables,files_outside_table_folders",
            record.diagnosticDetails["ownership_not_checked"],
        )
    }

    @Test
    fun `a deletion stopped at a safety recheck writes a FAILED record with its own reason and the fence details`() {
        recorder.recordDeletionStopped(
            runId = "run-5",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/db/orders"),
            storageFolderPaths = listOf("s3a://example-bucket/db/orders", "s3a://example-bucket/db/a", "s3a://example-bucket/db/b"),
            candidateFolderPaths = listOf("s3a://example-bucket/db/a", "s3a://example-bucket/db/b"),
            candidateSizeStats = StorageSizeStats(objectCount = 2, totalSizeBytes = 20),
            deletedFolderPaths = listOf("s3a://example-bucket/db/a"),
            deletedSizeStats = StorageSizeStats(objectCount = 1, totalSizeBytes = 10),
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = emptyList(),
            scanPlan = null,
            inspectionOutcomes = emptyMap(),
            notFoundAtDeletionPaths = emptyList(),
            error = IllegalStateException("Deletion stopped at a safety recheck after 1 deleted folder(s): slow", IOException("secret=hunter2")),
            ownershipChecks = OwnershipChecks(),
            deletionFence =
                DeletionFenceReport(
                    notAttemptedPaths = listOf("s3a://example-bucket/db/b"),
                    stop = FenceStop(FenceStopCause.RECHECK_TOO_SLOW, "slow"),
                ),
            tooNewPaths = emptyList(),
        )

        val record = capturedRecord()

        assertEquals("FAILED" to "deletion_stopped_at_recheck", record.status to record.statusReason)
        assertEquals(listOf("s3a://example-bucket/db/a"), record.deletedFolders)
        assertEquals(1, record.deletedFolderCount)
        assertEquals("recheck_too_slow", record.diagnosticDetails["deletion_stop_cause"])
        assertEquals("s3a://example-bucket/db/b", record.diagnosticDetails["folder_not_attempted_paths_sample"])
        assertFalse(record.diagnosticDetails.containsKey("deletion_failed_folder_path"))
        assertEquals(
            "Deletion stopped at a safety recheck after 1 deleted folder(s): slow; caused by java.io.IOException",
            record.errorMessage,
        )
    }

    @Test
    fun `a partly deleted folder with a control character in its name stays one escaped value on the row`() {
        val folder = "s3a://example-bucket/db/b\u001B[2J\nFORGED=1\\x"
        recorder.recordDeletionStopped(
            runId = "run-6",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/db/orders"),
            storageFolderPaths = listOf("s3a://example-bucket/db/orders", "s3a://example-bucket/db/a", folder),
            candidateFolderPaths = listOf("s3a://example-bucket/db/a", folder),
            candidateSizeStats = StorageSizeStats(objectCount = 3, totalSizeBytes = 30),
            deletedFolderPaths = listOf("s3a://example-bucket/db/a"),
            deletedSizeStats = StorageSizeStats(objectCount = 1, totalSizeBytes = 10),
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = emptyList(),
            scanPlan = null,
            inspectionOutcomes = emptyMap(),
            notFoundAtDeletionPaths = emptyList(),
            error = IllegalStateException("Deletion stopped at a safety recheck after 1 deleted folder(s): expired"),
            ownershipChecks = OwnershipChecks(),
            deletionFence =
                DeletionFenceReport(
                    partialFolder =
                        FolderDeletionResult(
                            folderPath = folder,
                            outcome = FolderDeletionOutcome.PARTIAL_STOPPED,
                            listedObjectCount = 2,
                            deletedKeys = listOf("d\nFORGED=2.parquet"),
                            remainingKeys = listOf("r\u001B.parquet"),
                        ),
                    stop = FenceStop(FenceStopCause.SNAPSHOT_EXPIRED, "expired"),
                ),
            tooNewPaths = emptyList(),
        )

        val details = capturedRecord().diagnosticDetails

        assertEquals("s3a://example-bucket/db/b\\u001B[2J\\u000AFORGED=1\\\\x", details["partially_deleted_folder_path"])
        assertEquals("d\\u000AFORGED=2.parquet", details["partially_deleted_folder_deleted_keys_sample"])
        assertEquals("r\\u001B.parquet", details["partially_deleted_folder_remaining_keys_sample"])
        assertFalse(details.values.any { value -> value.any { it == '\u001B' } }, details.toString())
        assertFalse(details.getValue("partially_deleted_folder_path").contains('\n'))
    }

    @Test
    fun `every diagnostic value gets a last pass on the row, line by line, which keeps a backslash already there`() {
        val builder = spyk(CleanupAuditDiagnosticDetailsBuilder())
        every { builder.ageDetails(any()) } returns
            mapOf("configured_older_than_hours" to "a\u001Bb\nkeep s3a://FAKEUSER:FAKEPASS@example-bucket/x\nalready \\u001B escaped\u2028end")
        val recorder =
            CleanupAuditRecorder().apply {
                config = ApplicationConfig(catalog = "example_catalog", databases = listOf("analytics"))
                cleanupAuditTableService = this@CleanupAuditRecorderTest.cleanupAuditTableService
                auditDiagnosticDetailsBuilder = builder
                excludePathResolver = this@CleanupAuditRecorderTest.excludePathResolver
            }

        recorder.recordDeletionFailed(
            runId = "run-7",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 0,
            activeTableLocations = emptyList(),
            storageFolderPaths = listOf("s3a://example-bucket/db/a"),
            candidateFolderPaths = listOf("s3a://example-bucket/db/a"),
            candidateSizeStats = StorageSizeStats(objectCount = 1, totalSizeBytes = 10),
            deletedFolderPaths = emptyList(),
            deletedSizeStats = StorageSizeStats(objectCount = 0, totalSizeBytes = 0),
            failedFolderPath = "s3a://example-bucket/db/a",
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = emptyList(),
            error = IllegalStateException("simulated delete failure"),
        )

        val value = capturedRecord().diagnosticDetails.getValue("configured_older_than_hours")
        assertEquals("a\\u001Bb\nkeep s3a://***@example-bucket/x\nalready \\u001B escaped\\u2028end", value)
        assertFalse("FAKEPASS" in value || "FAKEUSER" in value, value)
    }

    private fun capturedRecord(): CleanupAuditRecord {
        val record = slot<CleanupAuditRecord>()
        verify { cleanupAuditTableService.writeAuditRecord(capture(record)) }
        return record.captured
    }

    @Test
    fun `unsafe scan root writes a SKIPPED record with its reason`() {
        recorder.recordUnsafeScanRoot(
            runId = "run-1",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "default",
            discoveredDatabaseLocation = "s3a://bucket/lake",
            activeTableCount = 1,
            unresolvedTables = emptyList(),
            activeTableLocations = listOf("s3a://bucket/lake/events"),
            reason = "database_location_contains_warehouse_root",
            errorMessage = "Database location equals or contains the catalog warehouse root",
            excludedPaths = emptyList(),
        )

        val record = capturedRecord()

        assertEquals("SKIPPED", record.status)
        assertEquals("database_location_contains_warehouse_root", record.statusReason)
        assertEquals("Database location equals or contains the catalog warehouse root", record.errorMessage)
        assertEquals(0, record.candidateFolderCount)
        assertEquals(0, record.deletedFolderCount)
    }

    @Test
    fun `ownership unverified writes a BLOCKED record with no error message`() {
        recorder.recordOwnershipUnverified(
            runId = "run-1",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 2,
            unresolvedTables = listOf("example_catalog.analytics.orders"),
            activeTableLocations = listOf("s3a://example-bucket/db/active_table"),
            storageFolderPaths = listOf("s3a://example-bucket/db/active_table", "s3a://example-bucket/db/unmatched"),
            potentiallyUntrackedFolderPaths = listOf("s3a://example-bucket/db/unmatched"),
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
        )

        val record = capturedRecord()

        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertNull(record.errorMessage)
    }

    @Test
    fun `ownership unverified counts folders as potentially untracked rather than candidates`() {
        recorder.recordOwnershipUnverified(
            runId = "run-2",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            unresolvedTables = listOf("example_catalog.analytics.orders"),
            activeTableLocations = listOf("s3a://example-bucket/db/active_table"),
            storageFolderPaths = listOf("s3a://example-bucket/db/active_table", "s3a://example-bucket/db/unmatched"),
            potentiallyUntrackedFolderPaths = listOf("s3a://example-bucket/db/unmatched"),
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
        )

        val record = capturedRecord()

        assertEquals(1, record.unresolvedTableCount)
        assertEquals(1, record.potentiallyUntrackedFolderCount)
        assertEquals(0, record.candidateFolderCount)
        assertEquals(emptyList<String>(), record.candidateFolders)
    }

    @Test
    fun `too many folders with unresolved tables reports them as potentially untracked, not candidates`() {
        val unmatchedFolders = (0..MAX_AUDIT_PATH_SAMPLE_SIZE).map { "s3a://example-bucket/db/folder_$it" }

        recorder.recordTooManyCandidateFolders(
            runId = "run-5",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 2,
            unresolvedTables = listOf("example_catalog.analytics.orders"),
            activeTableLocations = listOf("s3a://example-bucket/db/active_table"),
            storageFolderPaths = unmatchedFolders,
            candidateFolderPaths = unmatchedFolders,
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            errorMessage = "limit exceeded",
        )

        val record = capturedRecord()

        assertEquals("SKIPPED", record.status)
        assertEquals("too_many_candidate_folders", record.statusReason)
        assertEquals(1, record.unresolvedTableCount)
        assertEquals(unmatchedFolders.size.toLong(), record.potentiallyUntrackedFolderCount)
        assertEquals(0, record.candidateFolderCount)
        assertEquals(emptyList<String>(), record.candidateFolders)
        assertEquals("true", record.diagnosticDetails["potentially_untracked_folder_paths_truncated"])
        assertEquals("example_catalog.analytics.orders", record.diagnosticDetails["unresolved_tables_sample"])
        assertNull(record.diagnosticDetails["candidate_folder_paths_sample"])
    }

    @Test
    fun `unexpected failure still records an error message`() {
        every { excludePathResolver.normalizedConfiguredExcludePaths() } returns emptyList()

        recorder.recordFailed(
            runId = "run-3",
            database = "analytics",
            databaseStartTime = Instant.EPOCH,
            unresolvedTables = emptyList(),
            error = IllegalStateException("storage listing failed"),
        )

        val record = capturedRecord()

        assertEquals("FAILED", record.status)
        assertNotNull(record.errorMessage)
    }

    @Test
    fun `unexpected failure before scan coverage was evaluated records no coverage`() {
        recorder.recordFailed(
            runId = "run-3",
            database = "analytics",
            databaseStartTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unresolvedTables = emptyList(),
            error = IllegalStateException("catalog lookup failed"),
        )

        assertEquals(DEFAULT_AGE_DETAILS, capturedRecord().diagnosticDetails - OWNERSHIP_CHECK_KEYS)
    }

    @Test
    fun `unexpected failure after a complete scan records zero unscanned table locations`() {
        recorder.recordFailed(
            runId = "run-3",
            database = "analytics",
            databaseStartTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = emptyList(),
            unresolvedTables = emptyList(),
            error = IllegalStateException("storage listing failed"),
        )

        assertEquals(
            DEFAULT_AGE_DETAILS + mapOf("unscanned_table_location_count" to "0"),
            capturedRecord().diagnosticDetails - OWNERSHIP_CHECK_KEYS,
        )
    }

    @Test
    fun `deletion failure after a complete scan records zero unscanned table locations`() {
        recorder.recordDeletionFailed(
            runId = "run-4",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/db/orders"),
            storageFolderPaths = listOf("s3a://example-bucket/db/orders", "s3a://example-bucket/db/a"),
            candidateFolderPaths = listOf("s3a://example-bucket/db/a"),
            candidateSizeStats = null,
            deletedFolderPaths = emptyList(),
            deletedSizeStats = StorageSizeStats.ZERO,
            failedFolderPath = "s3a://example-bucket/db/a",
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = emptyList(),
            error = IllegalStateException("simulated delete failure"),
        )

        val details = capturedRecord().diagnosticDetails
        assertEquals("0", details["unscanned_table_location_count"])
        assertNull(details["unscanned_table_locations_sample"])
    }

    @Test
    fun `success with unscanned table locations is qualified as partial coverage`() {
        recorder.recordSuccess(
            runId = "run-4",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 2,
            activeTableLocations = listOf("s3a://example-bucket/db/orders", "s3a://example-bucket/other/events"),
            storageFolderPaths = listOf("s3a://example-bucket/db/orders"),
            candidateFolderPaths = emptyList(),
            candidateSizeStats = StorageSizeStats.ZERO,
            deletedFolderPaths = emptyList(),
            deletedSizeStats = StorageSizeStats.ZERO,
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = listOf("s3a://example-bucket/other/events"),
        )

        val record = capturedRecord()

        assertEquals("SUCCESS", record.status)
        assertEquals("partial_scan_coverage", record.statusReason)
        assertEquals("1", record.diagnosticDetails["unscanned_table_location_count"])
        assertEquals("s3a://example-bucket/other/events", record.diagnosticDetails["unscanned_table_locations_sample"])
    }

    @Test
    fun `success with full coverage keeps no status reason`() {
        recorder.recordSuccess(
            runId = "run-5",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/db/orders"),
            storageFolderPaths = listOf("s3a://example-bucket/db/orders"),
            candidateFolderPaths = emptyList(),
            candidateSizeStats = StorageSizeStats.ZERO,
            deletedFolderPaths = emptyList(),
            deletedSizeStats = StorageSizeStats.ZERO,
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
        )

        val record = capturedRecord()

        assertNull(record.statusReason)
        assertNull(record.diagnosticDetails["unscanned_table_location_count"])
    }

    @Test
    fun `every row type states which ownership checks ran and which did not`() {
        val records = mutableListOf<CleanupAuditRecord>()
        every { cleanupAuditTableService.writeAuditRecord(capture(records)) } just Runs
        val db = "s3a://example-bucket/db"
        val tables = listOf("$db/orders")
        val folders = listOf("$db/orders", "$db/a")
        val candidates = listOf("$db/a")
        val allRead = OwnershipChecks(otherDatabaseFolders = true, otherTables = true, finalRecheck = true)

        recorder.recordSuccess(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null, emptyList(), null,
            Instant.EPOCH, emptyList(), ownershipChecks = allRead,
        )
        DeletionBlockReason.entries.forEach { reason ->
            recorder.recordDeletionBlocked(
                reason, "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null,
                Instant.EPOCH, emptyList(), emptyList(), null, ownershipChecks = allRead,
            )
        }
        recorder.recordOwnershipUnverified(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 2, listOf("example_catalog.analytics.t2"), tables, folders,
            candidates, Instant.EPOCH, emptyList(), ownershipChecks = allRead,
        )
        recorder.recordTooManyCandidateFolders(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, emptyList(), tables, folders, candidates, Instant.EPOCH,
            emptyList(), "too many", ownershipChecks = allRead,
        )
        recorder.recordDeletionFailed(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null, emptyList(), null,
            "$db/a", Instant.EPOCH, emptyList(), emptyList(), error = IllegalStateException("delete failed"), ownershipChecks = allRead,
        )
        recorder.recordFailed(
            "run", "analytics", Instant.EPOCH, unresolvedTables = emptyList(), error = IllegalStateException("boom"), ownershipChecks = allRead,
        )
        // Written before other storage is read (database_not_found here as at the first read), so these rows state only
        // this database's checks.
        recorder.recordDatabaseNotFound("run", "analytics", Instant.EPOCH, IllegalStateException("missing"))
        recorder.recordNoActiveTables("run", Instant.EPOCH, "example_catalog", "analytics", db, emptyList(), 0, emptyList())
        recorder.recordDatabaseLocationMissing("run", Instant.EPOCH, "example_catalog", "analytics", null, 1, emptyList(), tables, "missing", emptyList())
        recorder.recordUnsafeScanRoot("run", Instant.EPOCH, "example_catalog", "analytics", db, 1, emptyList(), tables, "unsafe", "unsafe", emptyList())

        assertEquals(
            listOf(
                "SUCCESS", "BLOCKED", "BLOCKED", "BLOCKED", "BLOCKED", "BLOCKED", "SKIPPED", "FAILED", "FAILED", "SKIPPED", "SKIPPED",
                "SKIPPED", "SKIPPED",
            ),
            records.map { it.status },
        )
        records.dropLast(4).forEach { record ->
            assertEquals(
                "visible_tables_of_this_database,write_paths_of_visible_tables_of_this_database," +
                    "default_folders_of_visible_other_databases,configured_exclusions,visible_tables_of_other_databases," +
                    "visible_storage_of_same_bucket_session_catalogs,final_recheck_before_deletion",
                record.diagnosticDetails["ownership_checks"],
                record.statusReason,
            )
            assertEquals(
                "other_bucket_session_catalogs,catalogs_outside_session,hidden_tables,nested_namespace_tables,files_outside_table_folders",
                record.diagnosticDetails["ownership_not_checked"],
                record.statusReason,
            )
        }
        records.takeLast(4).forEach { record ->
            assertEquals(
                "visible_tables_of_this_database,write_paths_of_visible_tables_of_this_database,configured_exclusions",
                record.diagnosticDetails["ownership_checks"],
                record.statusReason,
            )
            assertEquals(
                "default_folders_of_other_databases,other_database_tables,same_bucket_session_catalogs,final_recheck_before_deletion," +
                    "other_bucket_session_catalogs,catalogs_outside_session,hidden_tables,nested_namespace_tables,files_outside_table_folders",
                record.diagnosticDetails["ownership_not_checked"],
                record.statusReason,
            )
        }
    }

    @Test
    fun `a FAILED row keeps the top message and only the class names of its causes`() {
        recorder.recordFailed(
            runId = "run-6",
            database = "analytics",
            databaseStartTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unresolvedTables = emptyList(),
            error =
                IllegalStateException(
                    "catalog lookup failed\u001B[2J: token=CANARY_TOP",
                    IOException("Connection reset: secret=CANARY_CAUSE", RuntimeException("CANARY_ROOT")),
                ),
        )

        assertEquals(
            "catalog lookup failed\\u001B[2J: token=***; caused by java.io.IOException; caused by java.lang.RuntimeException",
            capturedRecord().errorMessage,
        )
    }

    @Test
    fun `a long error message is bounded with a marker`() {
        recorder.recordFailed(
            runId = "run-7",
            database = "analytics",
            databaseStartTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unresolvedTables = emptyList(),
            error = IllegalStateException("x".repeat(5_000)),
        )

        val errorMessage = capturedRecord().errorMessage.orEmpty()
        assertTrue(errorMessage.length <= SafeText.MAX_AUDIT_ERROR_LENGTH, "length=${errorMessage.length}")
        assertTrue(errorMessage.endsWith(SafeText.TRUNCATION_MARKER))
    }

    @Test
    fun `names and paths in a row keep their text with control characters escaped`() {
        val orphan = "s3a://example-bucket/db/a\r\nFORGED\u202Egpj.exe"
        recorder.recordDeletionFailed(
            runId = "run-8",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics\u0000",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db\u0085",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/db/orders"),
            storageFolderPaths = listOf("s3a://example-bucket/db/orders", orphan),
            candidateFolderPaths = listOf(orphan, "s3a://example-bucket/db/token=1"),
            candidateSizeStats = StorageSizeStats(objectCount = 2, totalSizeBytes = 20),
            deletedFolderPaths = listOf(orphan),
            deletedSizeStats = StorageSizeStats(objectCount = 1, totalSizeBytes = 10),
            failedFolderPath = "s3a://example-bucket/db/token=1",
            cutoffTime = Instant.EPOCH,
            excludedPaths = listOf("s3a://example-bucket/db/keep\u2028x"),
            unscannedTableLocations = emptyList(),
            error = IllegalStateException("simulated delete failure"),
        )

        val record = capturedRecord()
        val escapedOrphan = "s3a://example-bucket/db/a\\u000D\\u000AFORGED\\u202Egpj.exe"
        assertEquals("analytics\\u0000", record.databaseName)
        assertEquals("s3a://example-bucket/db\\u0085", record.storageScanLocation)
        assertEquals(listOf(escapedOrphan, "s3a://example-bucket/db/token=1"), record.candidateFolders)
        assertEquals(listOf(escapedOrphan), record.deletedFolders)
        assertEquals(listOf("s3a://example-bucket/db/keep\\u2028x"), record.excludedPaths)
        assertEquals("s3a://example-bucket/db/token=1", record.diagnosticDetails["deletion_failed_folder_path"])
        assertEquals(
            "s3a://example-bucket/db/orders\n$escapedOrphan",
            record.diagnosticDetails["storage_folder_paths_sample"],
        )
        assertFalse(record.toString().any { it == '\r' || it == '\u0000' || it == '\u0085' || it == '\u202E' || it == '\u2028' })
    }

    @Test
    fun `folders whose names differ only by a backslash escape stay apart in every path field and read back exactly`() {
        val newline = "s3a://example-bucket/db/a\nb"
        val literal = "s3a://example-bucket/db/a\\u000Ab"
        val backslash = "s3a://example-bucket/db/c\\d"
        recorder.recordDeletionFailed(
            runId = "run-10",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics\\x",
            discoveredDatabaseLocation = "s3a://example-bucket/db",
            storageScanLocation = "s3a://example-bucket/db",
            activeTableCount = 1,
            activeTableLocations = listOf("s3a://example-bucket/db/orders"),
            storageFolderPaths = listOf(newline, literal, backslash),
            candidateFolderPaths = listOf(newline, literal, backslash),
            candidateSizeStats = StorageSizeStats(objectCount = 3, totalSizeBytes = 30),
            deletedFolderPaths = listOf(newline, literal),
            deletedSizeStats = StorageSizeStats(objectCount = 2, totalSizeBytes = 20),
            failedFolderPath = backslash,
            cutoffTime = Instant.EPOCH,
            excludedPaths = listOf("s3a://example-bucket/db/keep\\me"),
            unscannedTableLocations = emptyList(),
            error = IllegalStateException("simulated delete failure"),
        )

        val record = capturedRecord()
        val written = listOf("s3a://example-bucket/db/a\\u000Ab", "s3a://example-bucket/db/a\\\\u000Ab", "s3a://example-bucket/db/c\\\\d")
        assertEquals("analytics\\\\x", record.databaseName)
        assertEquals(written, record.candidateFolders)
        assertEquals(written.take(2), record.deletedFolders)
        assertEquals(listOf("s3a://example-bucket/db/keep\\\\me"), record.excludedPaths)
        // Escaped once where the sample is built; the last pass at the row keeps it as it is.
        assertEquals(written.joinToString("\n"), record.diagnosticDetails["storage_folder_paths_sample"])
        assertEquals(written[2], record.diagnosticDetails["deletion_failed_folder_path"])

        val values = SparkSqlText(CleanupAuditTableService().buildInsertAuditRecordSql(record)).insert().values
        assertEquals(written, values["candidate_folders"])
        assertEquals(written.take(2), values["deleted_folders"])
        assertEquals("analytics\\\\x", values["database_name"])
        assertEquals(written.joinToString("\n"), (values["diagnostic_details"] as Map<*, *>)["storage_folder_paths_sample"])
    }

    @Test
    fun `an error message passed in as text is cleaned when the row is built`() {
        recorder.recordUnsafeScanRoot(
            runId = "run-9",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "default",
            discoveredDatabaseLocation = "s3a://bucket/lake",
            activeTableCount = 1,
            unresolvedTables = emptyList(),
            activeTableLocations = listOf("s3a://bucket/lake/events"),
            reason = "scan_root_outside_database_boundary",
            errorMessage = "Resolved storage scan location escapes database boundary. location=s3a://bucket/x\u001B[2J?X-Amz-Signature=CANARY_SIGNATURE",
            excludedPaths = emptyList(),
        )

        assertEquals(
            "Resolved storage scan location escapes database boundary. location=s3a://bucket/x\\u001B[2J?***",
            capturedRecord().errorMessage,
        )
    }

    @Test
    fun `every row type records the age the run used and keeps the configured age`() {
        val deletionRows = writeEveryRowType(ApplicationConfig("example_catalog", listOf("analytics"), dryRun = false, deleteEnabled = true))
        val clampedRows =
            writeEveryRowType(ApplicationConfig("example_catalog", listOf("analytics"), olderThanHours = 0, dryRun = false, deleteEnabled = true))
        val longerRows =
            writeEveryRowType(ApplicationConfig("example_catalog", listOf("analytics"), olderThanHours = 100, dryRun = false, deleteEnabled = true))
        val dryRunRows = writeEveryRowType(ApplicationConfig("example_catalog", listOf("analytics"), olderThanHours = 0, dryRun = true))

        listOf(deletionRows to (72L to "24"), clampedRows to (72L to "0"), longerRows to (100L to "100"), dryRunRows to (0L to "0"))
            .forEach { (rows, expected) ->
                val (olderThanHours, configured) = expected
                assertEquals(13, rows.size)
                rows.forEach { row ->
                    assertEquals(olderThanHours, row.olderThanHours, row.statusReason)
                    assertEquals(configured, row.diagnosticDetails["configured_older_than_hours"], row.statusReason)
                    assertEquals("72", row.diagnosticDetails["deletion_min_age_hours"], row.statusReason)
                }
            }
    }

    @Test
    fun `rows that list folders also name the ones a deletion run would hold`() {
        val records = mutableListOf<CleanupAuditRecord>()
        every { cleanupAuditTableService.writeAuditRecord(capture(records)) } just Runs
        val db = "s3a://example-bucket/db"
        val tables = listOf("$db/orders")
        val folders = listOf("$db/orders", "$db/a", "$db/young")
        val candidates = listOf("$db/a", "$db/young")
        val held = listOf("$db/young")

        recorder.recordSuccess(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null, emptyList(), null,
            Instant.EPOCH, emptyList(), wouldBeHeldByDeletionFloorPaths = held,
        )
        recorder.recordDeletionBlocked(
            DeletionBlockReason.OTHER_DATABASE_STORAGE_UNKNOWN, "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables,
            folders, candidates, null, Instant.EPOCH, emptyList(), emptyList(), null, wouldBeHeldByDeletionFloorPaths = held,
        )
        recorder.recordOwnershipUnverified(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 2, listOf("example_catalog.analytics.t2"), tables, folders,
            candidates, Instant.EPOCH, emptyList(), wouldBeHeldByDeletionFloorPaths = held,
        )
        recorder.recordSuccess(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null, emptyList(), null,
            Instant.EPOCH, emptyList(),
        )

        records.take(3).forEach { row ->
            assertEquals("1", row.diagnosticDetails["would_be_held_by_deletion_floor_count"], row.statusReason)
            assertEquals("$db/young", row.diagnosticDetails["would_be_held_by_deletion_floor_paths_sample"], row.statusReason)
            assertEquals("false", row.diagnosticDetails["would_be_held_by_deletion_floor_paths_truncated"], row.statusReason)
        }
        assertTrue(records.last().diagnosticDetails.keys.none { it.startsWith("would_be_held_by_deletion_floor") })
    }

    @Test
    fun `rows that inspected folders count every held folder by reason, and the too-many row does not`() {
        val records = mutableListOf<CleanupAuditRecord>()
        every { cleanupAuditTableService.writeAuditRecord(capture(records)) } just Runs
        val db = "s3a://example-bucket/db"
        val tables = listOf("$db/orders")
        val folders = listOf("$db/orders", "$db/a", "$db/empty", "$db/young", "$db/moved")
        val candidates = listOf("$db/a", "$db/moved")
        val outcomes = mapOf(CandidateInspectionOutcome.EMPTY_FOLDER to listOf("$db/empty"))
        val young = listOf("$db/young")
        val fence = DeletionFenceReport(keptPaths = mapOf(FenceKeepReason.CHANGED_SINCE_INSPECTION to listOf("$db/moved")))

        recorder.recordSuccess(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null, emptyList(), null,
            Instant.EPOCH, emptyList(), inspectionOutcomes = outcomes, deletionFence = fence, tooNewPaths = young,
        )
        recorder.recordDeletionBlocked(
            DeletionBlockReason.OTHER_DATABASE_STORAGE_UNKNOWN, "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables,
            folders, candidates, null, Instant.EPOCH, emptyList(), emptyList(), null,
            inspectionOutcomes = outcomes, deletionFence = fence, tooNewPaths = young,
        )
        recorder.recordDeletionFailed(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null, emptyList(), null,
            "$db/a", Instant.EPOCH, emptyList(), emptyList(), error = IllegalStateException("delete failed"),
            inspectionOutcomes = outcomes, deletionFence = fence, tooNewPaths = young,
        )
        recorder.recordDeletionStopped(
            runId = "run",
            databaseStartTime = Instant.EPOCH,
            catalogName = "example_catalog",
            databaseName = "analytics",
            discoveredDatabaseLocation = db,
            storageScanLocation = db,
            activeTableCount = 1,
            activeTableLocations = tables,
            storageFolderPaths = folders,
            candidateFolderPaths = candidates,
            candidateSizeStats = null,
            deletedFolderPaths = emptyList(),
            deletedSizeStats = null,
            cutoffTime = Instant.EPOCH,
            excludedPaths = emptyList(),
            unscannedTableLocations = emptyList(),
            scanPlan = null,
            inspectionOutcomes = outcomes,
            notFoundAtDeletionPaths = emptyList(),
            error = IllegalStateException("Deletion stopped at a safety recheck after 0 deleted folder(s): slow"),
            ownershipChecks = OwnershipChecks(),
            deletionFence = fence.copy(stop = FenceStop(FenceStopCause.RECHECK_TOO_SLOW, "slow"), notAttemptedPaths = listOf("$db/a")),
            tooNewPaths = young,
        )
        recorder.recordOwnershipUnverified(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 2, listOf("example_catalog.analytics.t2"), tables, folders,
            candidates, Instant.EPOCH, emptyList(), inspectionOutcomes = outcomes, tooNewPaths = young,
        )
        recorder.recordTooManyCandidateFolders(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, emptyList(), tables, folders, folders.drop(1), Instant.EPOCH,
            emptyList(), "too many",
        )

        assertEquals(
            listOf("SUCCESS", "BLOCKED", "FAILED", "FAILED", "BLOCKED", "SKIPPED"),
            records.map { it.status },
        )
        records.take(4).forEach { row ->
            assertEquals("3", row.diagnosticDetails["held_folder_count"], row.statusReason)
            assertEquals(
                "empty_folder=1\ntoo_new=1\nfolder_kept_changed_since_inspection=1",
                row.diagnosticDetails["held_folder_counts"],
                row.statusReason,
            )
            assertEquals("1", row.diagnosticDetails["too_new_count"], row.statusReason)
            assertEquals("$db/young", row.diagnosticDetails["too_new_paths_sample"], row.statusReason)
        }
        // The fence never ran for a database with unresolved ownership, so only the age check's reasons are counted.
        assertEquals("2", records[4].diagnosticDetails["held_folder_count"])
        assertEquals("empty_folder=1\ntoo_new=1", records[4].diagnosticDetails["held_folder_counts"])
        assertEquals("1", records[4].diagnosticDetails["too_new_count"])
        assertTrue(records[5].diagnosticDetails.keys.none { it.startsWith("held_folder") || it.startsWith("too_new") })
    }

    // One row of each type, in the order of `every row type states which ownership checks ran and which did not`.
    private fun writeEveryRowType(config: ApplicationConfig): List<CleanupAuditRecord> {
        val tableService = mockk<CleanupAuditTableService>(relaxed = true)
        val records = mutableListOf<CleanupAuditRecord>()
        every { tableService.writeAuditRecord(capture(records)) } just Runs
        val rowRecorder =
            CleanupAuditRecorder().apply {
                this.config = config
                cleanupAuditTableService = tableService
                auditDiagnosticDetailsBuilder = CleanupAuditDiagnosticDetailsBuilder()
                excludePathResolver = this@CleanupAuditRecorderTest.excludePathResolver
            }
        val db = "s3a://example-bucket/db"
        val tables = listOf("$db/orders")
        val folders = listOf("$db/orders", "$db/a")
        val candidates = listOf("$db/a")

        rowRecorder.recordSuccess(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null, emptyList(), null,
            Instant.EPOCH, emptyList(),
        )
        DeletionBlockReason.entries.forEach { reason ->
            rowRecorder.recordDeletionBlocked(
                reason, "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null,
                Instant.EPOCH, emptyList(), emptyList(), null,
            )
        }
        rowRecorder.recordOwnershipUnverified(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 2, listOf("example_catalog.analytics.t2"), tables, folders,
            candidates, Instant.EPOCH, emptyList(),
        )
        rowRecorder.recordTooManyCandidateFolders(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, emptyList(), tables, folders, candidates, Instant.EPOCH,
            emptyList(), "too many",
        )
        rowRecorder.recordDeletionFailed(
            "run", Instant.EPOCH, "example_catalog", "analytics", db, db, 1, tables, folders, candidates, null, emptyList(), null,
            "$db/a", Instant.EPOCH, emptyList(), emptyList(), error = IllegalStateException("delete failed"),
        )
        rowRecorder.recordFailed("run", "analytics", Instant.EPOCH, unresolvedTables = emptyList(), error = IllegalStateException("boom"))
        rowRecorder.recordDatabaseNotFound("run", "analytics", Instant.EPOCH, IllegalStateException("missing"))
        rowRecorder.recordNoActiveTables("run", Instant.EPOCH, "example_catalog", "analytics", db, emptyList(), 0, emptyList())
        rowRecorder.recordDatabaseLocationMissing("run", Instant.EPOCH, "example_catalog", "analytics", null, 1, emptyList(), tables, "missing", emptyList())
        rowRecorder.recordUnsafeScanRoot("run", Instant.EPOCH, "example_catalog", "analytics", db, 1, emptyList(), tables, "unsafe", "unsafe", emptyList())
        return records
    }
}
