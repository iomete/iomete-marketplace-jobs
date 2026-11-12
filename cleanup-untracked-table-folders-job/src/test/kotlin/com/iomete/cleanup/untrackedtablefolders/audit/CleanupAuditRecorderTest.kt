package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Instant

private val OWNERSHIP_CHECK_KEYS = setOf("ownership_checks", "ownership_not_checked")

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

        assertEquals(emptyMap<String, String>(), capturedRecord().diagnosticDetails - OWNERSHIP_CHECK_KEYS)
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

        assertEquals(mapOf("unscanned_table_location_count" to "0"), capturedRecord().diagnosticDetails - OWNERSHIP_CHECK_KEYS)
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
        // Written before other storage is read, so these rows state only this database's checks.
        recorder.recordDatabaseNotFound("run", "analytics", Instant.EPOCH, IllegalStateException("missing"))
        recorder.recordNoActiveTables("run", Instant.EPOCH, "example_catalog", "analytics", db, emptyList())
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
}
