package com.iomete.backup.stats

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig
import com.iomete.backup.copy.CopyJobSummary
import com.iomete.backup.copy.CopyResult
import com.iomete.backup.copy.CopyStats
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp
import java.time.Instant

const val RUNS_TABLE = "lakehouse_backup_runs"
const val FILE_FAILURES_TABLE = "lakehouse_backup_run_file_failures"

enum class RunStatus { RUNNING, SUCCEEDED, FAILED }

private fun field(
    name: String,
    type: DataType,
    comment: String,
    nullable: Boolean = true,
) = StructField(name, type, nullable, Metadata.empty()).withComment(comment)

private val STRING = DataTypes.StringType
private val LONG = DataTypes.LongType
private val INT = DataTypes.IntegerType
private val DOUBLE = DataTypes.DoubleType
private val BOOLEAN = DataTypes.BooleanType
private val TIMESTAMP = DataTypes.TimestampType

val RUNS_SCHEMA: StructType =
    StructType(
        arrayOf(
            field("run_id", STRING, "the run ID shown in the console", nullable = false),
            field("job_id", STRING, "groups every run of one job"),
            field("started_by", STRING, "null means a scheduled run"),
            field("source_type", STRING, "s3 or hdfs", nullable = false),
            field("source_uri", STRING, "root URI the run reads from", nullable = false),
            field("target_type", STRING, "s3 or hdfs", nullable = false),
            field("target_uri", STRING, "root URI the run writes to", nullable = false),
            field(
                "status",
                STRING,
                "RUNNING, SUCCEEDED or FAILED; RUNNING after the run means the driver died",
                nullable = false,
            ),
            field("error_message", STRING, "null unless FAILED"),
            field(
                "started_at",
                TIMESTAMP,
                "when the run claimed this row, before source enumeration",
                nullable = false,
            ),
            field("ended_at", TIMESTAMP, "null while RUNNING"),
            field("files_listed", LONG, "files found by source enumeration"),
            field("dirs_listed", LONG, "empty directories found; HDFS source only"),
            field("files_copied", LONG, "files written to the target"),
            field("files_skipped", LONG, "files already identical at the target"),
            field("files_failed", LONG, "true failure count, not the capped row count in the failures table"),
            field("dirs_created", LONG, "empty directories replicated at the target"),
            field("retries_used", LONG, "sum of attempts beyond the first across every entry"),
            field(
                "failures_truncated",
                BOOLEAN,
                "failure rows hit stats.maxFailureRows, so the failures table is a sample",
            ),
            field("bytes_source", LONG, "bytes across everything enumerated at the source"),
            field("bytes_copied", LONG, "bytes written to the target"),
            field("bytes_skipped", LONG, "bytes in files already identical at the target"),
            field("source_listing_ms", LONG, "driver phase: enumerating the source tree"),
            field(
                "target_listing_ms",
                LONG,
                "driver phase: enumerating the target tree; 0 when skipIdentical is false",
            ),
            field("planning_ms", LONG, "driver phase: deciding what to copy and skip"),
            field("copy_ms", LONG, "driver phase: wall clock of the distributed copy"),
            field("dir_create_ms", LONG, "driver phase: replicating empty directories"),
            field(
                "copy_task_ms",
                LONG,
                "per-file wall time summed across tasks; divided by copy_ms gives average concurrency",
            ),
            field("fs_init_ms", LONG, "executor time building a FileSystem per file"),
            field("source_read_ms", LONG, "executor time reading from the source"),
            field("target_write_ms", LONG, "executor time writing to the target, including the closing upload"),
            field("throttle_wait_ms", LONG, "executor time blocked by the bandwidth cap"),
            field("verify_ms", LONG, "executor time on the post-write length check"),
            field("commit_ms", LONG, "executor time on the delete and rename that publish a file"),
            field("retry_sleep_ms", LONG, "executor time sleeping between copy attempts"),
            field("bytes_per_task", LONG, "copy.bytesPerTask for this run", nullable = false),
            field("files_per_task", INT, "copy.filesPerTask for this run", nullable = false),
            field("skip_identical", BOOLEAN, "copy.skipIdentical for this run", nullable = false),
            field("max_bandwidth_mb_per_sec", DOUBLE, "copy.maxBandwidthMbPerSec for this run; null means uncapped"),
            field("task_count", INT, "Spark tasks the copy was split into"),
            field("largest_file_bytes", LONG, "biggest single file the run had to copy"),
        ),
    )

val FILE_FAILURES_SCHEMA: StructType =
    StructType(
        arrayOf(
            field("run_id", STRING, "joins to lakehouse_backup_runs.run_id", nullable = false),
            field(
                "started_at",
                TIMESTAMP,
                "copied from the run row so both tables share a partition key",
                nullable = false,
            ),
            field("source_path", STRING, "entry that failed to copy", nullable = false),
            field("target_path", STRING, "where it would have gone", nullable = false),
            field("attempts_used", INT, "copy attempts before giving up", nullable = false),
            field("error", STRING, "exception class and message from the last attempt", nullable = false),
        ),
    )

data class RunIdentity(
    val runId: String,
    val jobId: String?,
    val startedBy: String?,
) {
    companion object {
        fun current(spark: SparkSession): RunIdentity =
            RunIdentity(
                runId = spark.conf().get("spark.app.name"),
                jobId = env("IOMETE_EXTERNAL_JOB_ID"),
                startedBy = env("IOMETE_JOB_STARTED_BY"),
            )

        private fun env(name: String): String? = System.getenv(name)?.takeIf { it.isNotBlank() }
    }
}

class RunProgress {
    var filesListed: Long? = null
    var dirsListed: Long? = null
    var bytesSource: Long? = null
    var sourceListingMs: Long? = null
    var summary: CopyJobSummary? = null
    var copy: CopyStats? = null
    var failures: List<CopyResult> = emptyList()
}

fun runRow(
    identity: RunIdentity,
    config: ApplicationConfig,
    startedAt: Instant,
    endedAt: Instant?,
    status: RunStatus,
    errorMessage: String?,
    progress: RunProgress,
): Map<String, Any?> {
    val summary = progress.summary
    val copy = progress.copy

    return mapOf(
        "run_id" to identity.runId,
        "job_id" to identity.jobId,
        "started_by" to identity.startedBy,
        "source_type" to storageType(config.source),
        "source_uri" to config.source.rootUri,
        "target_type" to storageType(config.target),
        "target_uri" to config.target.rootUri,
        "status" to status.name,
        "error_message" to errorMessage,
        "started_at" to Timestamp.from(startedAt),
        "ended_at" to endedAt?.let { Timestamp.from(it) },
        "files_listed" to progress.filesListed,
        "dirs_listed" to progress.dirsListed,
        "files_copied" to copy?.filesCopied,
        "files_skipped" to summary?.skippedCount?.toLong(),
        "files_failed" to summary?.failureCount?.toLong(),
        "dirs_created" to copy?.dirsCreated,
        "retries_used" to copy?.retriesUsed,
        "failures_truncated" to copy?.failuresTruncated,
        "bytes_source" to progress.bytesSource,
        "bytes_copied" to summary?.totalBytesCopied,
        "bytes_skipped" to summary?.skippedBytes,
        "source_listing_ms" to progress.sourceListingMs,
        "target_listing_ms" to copy?.targetListingMs,
        "planning_ms" to copy?.planningMs,
        "copy_ms" to copy?.copyMs,
        "dir_create_ms" to copy?.dirCreateMs,
        "copy_task_ms" to copy?.executor?.copyTaskMs,
        "fs_init_ms" to copy?.executor?.fsInitMs,
        "source_read_ms" to copy?.executor?.sourceReadMs,
        "target_write_ms" to copy?.executor?.targetWriteMs,
        "throttle_wait_ms" to copy?.executor?.throttleWaitMs,
        "verify_ms" to copy?.executor?.verifyMs,
        "commit_ms" to copy?.executor?.commitMs,
        "retry_sleep_ms" to copy?.executor?.retrySleepMs,
        "bytes_per_task" to config.copy.bytesPerTask,
        "files_per_task" to config.copy.filesPerTask,
        "skip_identical" to config.copy.skipIdentical,
        "max_bandwidth_mb_per_sec" to config.copy.maxBandwidthMbPerSec,
        "task_count" to copy?.taskCount,
        "largest_file_bytes" to copy?.largestFileBytes,
    )
}

fun fileFailureRow(
    identity: RunIdentity,
    startedAt: Instant,
    result: CopyResult,
): Map<String, Any?> =
    mapOf(
        "run_id" to identity.runId,
        "started_at" to Timestamp.from(startedAt),
        "source_path" to result.sourcePath,
        "target_path" to result.targetPath,
        "attempts_used" to result.attemptsUsed,
        "error" to (result.error ?: "unknown"),
    )

private fun storageType(config: StorageConfig): String =
    when (config) {
        is S3Config -> "s3"
        is HdfsConfig -> "hdfs"
    }
