package com.iomete.backup.stats

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.CopyConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.copy.CopyJobSummary
import com.iomete.backup.copy.CopyResult
import com.iomete.backup.copy.CopyStats
import com.iomete.backup.copy.ExecutorTimings
import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class RunStatsTest {
    private val startedAt = Instant.parse("2026-01-02T03:04:05Z")
    private val identity = RunIdentity(runId = "run-1", jobId = "job-1", startedBy = null)

    private val config =
        ApplicationConfig(
            source = S3Config(bucket = "src", prefix = "in", accessKey = "k", secretKey = "s"),
            target = HdfsConfig(namenode = "nn:8020", path = "out", user = "u"),
            copy = CopyConfig(bytesPerTask = 42, filesPerTask = 7, skipIdentical = false, maxBandwidthMbPerSec = 12.5),
        )

    private fun finishedProgress() =
        RunProgress().apply {
            filesListed = 10
            dirsListed = 2
            bytesSource = 5000
            sourceListingMs = 111
            summary =
                CopyJobSummary(
                    totalEntries = 10,
                    successCount = 8,
                    failureCount = 1,
                    skippedCount = 1,
                    totalBytesCopied = 4000,
                    skippedBytes = 500,
                    errors = emptyList(),
                )
            copy =
                CopyStats(
                    targetListingMs = 22,
                    planningMs = 33,
                    copyMs = 44,
                    dirCreateMs = 55,
                    taskCount = 3,
                    largestFileBytes = 3000,
                    filesCopied = 6,
                    dirsCreated = 2,
                    retriesUsed = 4,
                    failuresTruncated = true,
                    executor =
                        ExecutorTimings(
                            copyTaskMs = 100,
                            fsInitMs = 10,
                            sourceReadMs = 20,
                            targetWriteMs = 30,
                            throttleWaitMs = 40,
                            verifyMs = 5,
                            commitMs = 6,
                            retrySleepMs = 7,
                        ),
                )
        }

    @Test
    fun `every row the code builds fits the table it is written to`() {
        rowOf(RUNS_SCHEMA, runRow(identity, config, startedAt, null, RunStatus.RUNNING, null, RunProgress()))
        rowOf(RUNS_SCHEMA, runRow(identity, config, startedAt, startedAt, RunStatus.SUCCEEDED, null, finishedProgress()))
        rowOf(FILE_FAILURES_SCHEMA, fileFailureRow(identity, startedAt, CopyResult("a", "b", success = false)))

        assertFailsWith<IllegalArgumentException> { rowOf(RUNS_SCHEMA, mapOf("run_id" to "x")) }
        assertFailsWith<IllegalArgumentException> {
            rowOf(FILE_FAILURES_SCHEMA, fileFailureRow(identity, startedAt, CopyResult("a", "b", false)) + ("attempts_used" to 1L))
        }
    }

    @Test
    fun `a row derives what it cannot copy straight through`() {
        val row = runRow(identity, config, startedAt, null, RunStatus.RUNNING, null, finishedProgress())

        assertEquals("s3", row["source_type"])
        assertEquals("s3a://src/in", row["source_uri"])
        assertEquals("hdfs", row["target_type"])
        assertEquals("hdfs://nn:8020/out", row["target_uri"])
        assertEquals(Timestamp.from(startedAt), row["started_at"])
        assertEquals(1L, row["files_failed"])

        val failure = fileFailureRow(identity, startedAt, CopyResult("a", "b", success = false))
        assertEquals("unknown", failure["error"])
    }

    @Test
    fun `every column tells an operator what it holds`() {
        assertTrue(RUNS_SCHEMA.fields().all { it.getComment().isDefined }, "DESCRIBE TABLE is where an operator looks")
        assertTrue(FILE_FAILURES_SCHEMA.fields().all { it.getComment().isDefined })
    }
}
