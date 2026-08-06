package com.iomete.backup.stats.internal

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig
import com.iomete.backup.copy.CopyResult
import com.iomete.backup.stats.RunProgress
import com.iomete.backup.stats.RunStatus
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp
import java.time.Instant

internal data class RunIdentity(
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

internal fun runRow(
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

internal fun fileFailureRow(
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

// Spark only rejects a mistyped value once the write reaches the executors, far from the mapping.
private val EXTERNAL_TYPES: Map<DataType, Class<*>> =
    mapOf(
        DataTypes.StringType to String::class.java,
        DataTypes.LongType to Long::class.javaObjectType,
        DataTypes.IntegerType to Int::class.javaObjectType,
        DataTypes.DoubleType to Double::class.javaObjectType,
        DataTypes.BooleanType to Boolean::class.javaObjectType,
        DataTypes.TimestampType to Timestamp::class.java,
    )

internal fun rowOf(
    schema: StructType,
    values: Map<String, Any?>,
): Row {
    val names = schema.fieldNames().toSet()
    require(values.keys == names) {
        "row keys do not match the schema: missing ${names - values.keys}, unknown ${values.keys - names}"
    }
    schema.fields().forEach { field ->
        val value = values[field.name()] ?: return@forEach
        val expected = EXTERNAL_TYPES.getValue(field.dataType())
        require(expected.isInstance(value)) {
            "${field.name()} is ${field.dataType().simpleString()} but got a ${value.javaClass.simpleName}"
        }
    }
    return RowFactory.create(*schema.fieldNames().map { values[it] }.toTypedArray())
}
