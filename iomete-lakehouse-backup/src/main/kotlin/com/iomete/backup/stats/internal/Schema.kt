package com.iomete.backup.stats.internal

import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

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

internal val RUNS_SCHEMA: StructType =
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

internal val FILE_FAILURES_SCHEMA: StructType =
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

internal fun createTableSql(
    table: String,
    schema: StructType,
): String =
    """
    CREATE TABLE IF NOT EXISTS $table (${schema.toDDL()})
    USING iceberg
    PARTITIONED BY (days(started_at))
    """.trimIndent()
