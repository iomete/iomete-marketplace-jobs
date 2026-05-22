package com.iomete.cleanup.untrackedtablefolders.audit

import java.time.Instant
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CleanupAuditTableServiceTest {
    private val service = CleanupAuditTableService()

    @Test
    fun `builds audit insert sql with nullable cutoff timestamp`() {
        val sql =
            service.buildInsertAuditRecordSql(
                auditRecord(cutoffTime = null)
            )

        assertTrue(sql.contains("INSERT INTO ${CleanupAuditTableService.AUDIT_TABLE_NAME} ("))
        assertTrue(sql.contains("cutoff_time,"))
        assertTrue(sql.contains("CAST(NULL AS TIMESTAMP) AS cutoff_time"))
        assertTrue(sql.contains("diagnostic_details"))
        assertFalse(sql.contains("metrics"))
    }

    @Test
    fun `builds audit insert sql with non-null cutoff timestamp`() {
        val cutoffTime = Instant.parse("2026-05-21T10:15:30Z")

        val sql =
            service.buildInsertAuditRecordSql(
                auditRecord(cutoffTime = cutoffTime)
            )

        assertTrue(sql.contains("TIMESTAMP '"))
        assertTrue(sql.contains("' AS cutoff_time"))
        assertFalse(sql.contains("CAST(NULL AS TIMESTAMP) AS cutoff_time"))
    }

    @Test
    fun `escapes string values in audit insert sql`() {
        val sql =
            service.buildInsertAuditRecordSql(
                auditRecord(
                    databaseName = "customer's_db",
                    errorMessage = "can't find schema",
                )
            )

        assertTrue(sql.contains("'customer''s_db' AS database_name"))
        assertTrue(sql.contains("'can''t find schema' AS error_message"))
    }

    private fun auditRecord(
        cutoffTime: Instant? = null,
        databaseName: String = "test_db",
        errorMessage: String? = null,
    ): CleanupAuditRecord =
        CleanupAuditRecord(
            runId = "run-1",
            sparkAppId = "spark-app-1",
            initiatedBy = "hasan",
            catalogName = "spark_catalog",
            databaseName = databaseName,
            operation = "DISCOVER_UNTRACKED_TABLE_FOLDERS",
            dryRun = true,
            deleteEnabled = false,
            olderThanHours = 24,
            cutoffTime = cutoffTime,
            maxCandidateFoldersPerDatabase = 10,
            excludedPaths = listOf("s3a://bucket/db/protected"),
            status = "SUCCESS",
            statusReason = null,
            errorMessage = errorMessage,
            discoveredDatabaseLocation = "s3a://bucket/db.db",
            storageScanLocation = "s3a://bucket/db",
            activeTableCount = 1,
            storageFolderCount = 2,
            candidateFolderCount = 1,
            deletedFolderCount = 0,
            candidateFolders = listOf("s3a://bucket/db/orphan"),
            deletedFolders = emptyList(),
            diagnosticDetails = mapOf("candidate_folder_paths_truncated" to "false"),
            startTime = Instant.parse("2026-05-21T10:00:00Z"),
            endTime = Instant.parse("2026-05-21T10:01:00Z"),
        )
}
