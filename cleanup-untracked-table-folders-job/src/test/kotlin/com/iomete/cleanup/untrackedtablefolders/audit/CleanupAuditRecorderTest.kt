package com.iomete.cleanup.untrackedtablefolders.audit

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.time.Instant

class CleanupAuditRecorderTest {

    private val cleanupAuditTableService = mockk<CleanupAuditTableService>(relaxed = true)
    private val excludePathResolver = mockk<ExcludePathResolver>(relaxed = true)

    private val recorder = CleanupAuditRecorder().apply {
        config = ApplicationConfig(catalog = "example_catalog", databases = listOf("analytics"))
        cleanupAuditTableService = this@CleanupAuditRecorderTest.cleanupAuditTableService
        auditDiagnosticDetailsBuilder = CleanupAuditDiagnosticDetailsBuilder()
        excludePathResolver = this@CleanupAuditRecorderTest.excludePathResolver
    }

    private fun capturedRecord(): CleanupAuditRecord {
        val record = slot<CleanupAuditRecord>()
        verify { cleanupAuditTableService.writeAuditRecord(capture(record)) }
        return record.captured
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
    fun `unexpected failure still records an error message`() {
        every { excludePathResolver.normalizedConfiguredExcludePaths() } returns emptyList()

        recorder.recordFailed(
            runId = "run-3",
            database = "analytics",
            databaseStartTime = Instant.EPOCH,
            error = IllegalStateException("storage listing failed"),
        )

        val record = capturedRecord()

        assertEquals("FAILED", record.status)
        assertNotNull(record.errorMessage)
    }
}
