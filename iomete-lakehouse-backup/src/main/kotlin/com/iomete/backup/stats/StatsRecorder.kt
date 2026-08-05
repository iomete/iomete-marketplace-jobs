package com.iomete.backup.stats

import com.iomete.backup.config.ApplicationConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import java.sql.Timestamp
import java.time.Instant

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
        // started_at is in the join so the merge prunes to one partition; the claim wrote the same value.
        spark.sql(
            """
            MERGE INTO $runsTable t USING $FINAL_ROW_VIEW s
              ON t.run_id = s.run_id AND t.started_at = s.started_at
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

internal fun createTableSql(
    table: String,
    schema: StructType,
): String =
    """
    CREATE TABLE IF NOT EXISTS $table (${schema.toDDL()})
    USING iceberg
    PARTITIONED BY (days(started_at))
    """.trimIndent()

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
