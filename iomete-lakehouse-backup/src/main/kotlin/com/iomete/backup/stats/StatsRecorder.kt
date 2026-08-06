package com.iomete.backup.stats

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.stats.internal.FILE_FAILURES_SCHEMA
import com.iomete.backup.stats.internal.RUNS_SCHEMA
import com.iomete.backup.stats.internal.RunIdentity
import com.iomete.backup.stats.internal.createTableSql
import com.iomete.backup.stats.internal.fileFailureRow
import com.iomete.backup.stats.internal.rowOf
import com.iomete.backup.stats.internal.runRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import java.time.Instant

const val RUNS_TABLE = "lakehouse_backup_runs"
const val FILE_FAILURES_TABLE = "lakehouse_backup_run_file_failures"

private const val FINAL_ROW_VIEW = "lakehouse_backup_run_final"

/**
 * A row still RUNNING after the run is a run whose driver never came back to finalise it.
 *
 * Every write is best effort: a catalog that is down must not fail a backup that copied correctly.
 */
class StatsRecorder(
    private val spark: SparkSession,
    private val config: ApplicationConfig,
) {
    private val logger = LoggerFactory.getLogger(StatsRecorder::class.java)

    private val runsTable = "${config.stats.database}.$RUNS_TABLE"
    private val failuresTable = "${config.stats.database}.$FILE_FAILURES_TABLE"

    // Resolved inside the guarded path so a session without spark.app.name cannot fail the backup.
    private val identity: RunIdentity by lazy { RunIdentity.current(spark) }

    fun claim(startedAt: Instant) =
        record("claim") {
            ensureTables()

            val row = runRow(identity, config, startedAt, null, RunStatus.RUNNING, null, RunProgress())
            dataFrame(RUNS_SCHEMA, listOf(row)).writeTo(runsTable).append()

            logger.info("Recording run {} in {}", identity.runId, runsTable)
        }

    fun finalise(
        startedAt: Instant,
        progress: RunProgress,
        error: Throwable?,
    ) = record("finalise") {
        // Repeated so a run whose claim failed is still recorded by the INSERT branch of the merge.
        ensureTables()

        val status = if (error == null) RunStatus.SUCCEEDED else RunStatus.FAILED
        val message = error?.let { "${it.javaClass.simpleName}: ${it.message}" }
        val row = runRow(identity, config, startedAt, Instant.now(), status, message, progress)

        dataFrame(RUNS_SCHEMA, listOf(row)).createOrReplaceTempView(FINAL_ROW_VIEW)

        spark.sql(
            """
            MERGE INTO $runsTable t USING $FINAL_ROW_VIEW s
              ON t.run_id = s.run_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """.trimIndent(),
        )

        val failures = progress.failures.map { fileFailureRow(identity, startedAt, it) }
        if (failures.isNotEmpty()) {
            dataFrame(FILE_FAILURES_SCHEMA, failures).writeTo(failuresTable).append()
        }
    }

    private fun ensureTables() {
        spark.sql("CREATE DATABASE IF NOT EXISTS ${config.stats.database}")
        spark.sql(createTableSql(runsTable, RUNS_SCHEMA))
        spark.sql(createTableSql(failuresTable, FILE_FAILURES_SCHEMA))
    }

    private fun dataFrame(
        schema: StructType,
        rows: List<Map<String, Any?>>,
    ) = spark.createDataFrame(rows.map { rowOf(schema, it) }, schema)

    private inline fun record(
        phase: String,
        block: () -> Unit,
    ) {
        if (!config.stats.enabled) return

        try {
            block()
        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (e: Throwable) {
            // Throwable, not Exception: a missing Iceberg runtime arrives as an Error, and a backup
            // that copied every byte is still a success.
            logger.warn("Run stats {} failed; the backup is unaffected: {}", phase, e.toString(), e)
        }
    }
}
