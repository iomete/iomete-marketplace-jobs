package com.iomete.backup.integration

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.CopyConfig
import com.iomete.backup.config.StatsConfig
import com.iomete.backup.integration.fixtures.seedS3
import com.iomete.backup.integration.harness.IntegrationHarness
import com.iomete.backup.stats.FILE_FAILURES_TABLE
import com.iomete.backup.stats.RUNS_TABLE
import com.iomete.backup.stats.RunStatus
import org.apache.spark.sql.Row
import org.junit.jupiter.api.Test
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class StatsIntegrationTest {
    private val runsTable = "${IntegrationHarness.STATS_DATABASE}.$RUNS_TABLE"
    private val failuresTable = "${IntegrationHarness.STATS_DATABASE}.$FILE_FAILURES_TABLE"

    private val statsConfig = StatsConfig(database = IntegrationHarness.STATS_DATABASE)

    private fun runRow(runId: String): Row {
        val rows = IntegrationHarness.spark.sql("SELECT * FROM $runsTable WHERE run_id = '$runId'").collectAsList()
        assertEquals(1, rows.size, "a run must end with exactly one row")
        return rows.single()
    }

    private fun Row.long(column: String): Long = getAs<Long>(column)

    @Test
    fun `a succeeded run records its counts, bytes and phase clocks`() {
        val source = IntegrationHarness.freshBucket()
        val target = IntegrationHarness.freshBucket()
        val tree =
            mapOf(
                "a.txt" to "alpha".toByteArray(),
                "nested/b.txt" to "bravo".toByteArray(),
                "nested/deeper/c.txt" to "charlie".toByteArray(),
            )
        seedS3(source, tree)
        val sourceBytes = tree.values.sumOf { it.size.toLong() }

        val runId = UUID.randomUUID().toString()
        IntegrationHarness.runBackup(
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = IntegrationHarness.s3Config(target),
                stats = statsConfig,
            ),
            IntegrationHarness.runSession(runId),
        )

        val row = runRow(runId)

        assertEquals(RunStatus.SUCCEEDED.name, row.getAs<String>("status"))
        assertNotNull(row.getAs<Any?>("ended_at"))
        assertEquals(null, row.getAs<Any?>("error_message"))
        assertEquals("s3", row.getAs<String>("source_type"))

        assertEquals(tree.size.toLong(), row.long("files_listed"))
        assertEquals(tree.size.toLong(), row.long("files_copied"))
        assertEquals(0L, row.long("files_skipped"))
        assertEquals(0L, row.long("files_failed"))
        assertEquals(sourceBytes, row.long("bytes_source"))
        assertEquals(sourceBytes, row.long("bytes_copied"))
        assertEquals(false, row.getAs<Boolean>("failures_truncated"))

        listOf("source_listing_ms", "target_listing_ms", "planning_ms", "copy_ms", "dir_create_ms")
            .forEach { assertNotNull(row.getAs<Any?>(it), "$it must be filled on a finished run") }

        assertTrue(row.long("copy_task_ms") > 0, "copy tasks must report the time they spent")
        assertTrue(row.long("source_read_ms") > 0, "reading the source must show up")
        assertTrue(row.long("target_write_ms") > 0, "writing the target must show up")
        assertTrue(row.getAs<Int>("task_count") > 0)
        assertEquals(tree.values.maxOf { it.size.toLong() }, row.long("largest_file_bytes"))

        // A local master is one executor of one vCPU, whatever the machine underneath has.
        assertEquals(1, row.getAs<Int>("executor_count"))
        assertEquals(1.0, row.getAs<Double>("vcpu_per_executor"))
        assertEquals(1, row.getAs<Int>("slots_per_executor"))
        assertEquals(CopyConfig().tasksPerSlot, row.getAs<Int>("tasks_per_slot"))
        assertTrue(row.getAs<Int>("max_files_in_task") > 0)
    }

    @Test
    fun `a failed run is recorded as FAILED with a row per failed file, and still fails`() {
        val source = IntegrationHarness.freshBucket()
        seedS3(source, mapOf("a.txt" to "alpha".toByteArray(), "b.txt" to "bravo".toByteArray()))

        val runId = UUID.randomUUID().toString()
        assertFailsWith<IllegalStateException> {
            IntegrationHarness.runBackup(
                ApplicationConfig(
                    source = IntegrationHarness.s3Config(source),
                    // Never created, so every write fails.
                    target = IntegrationHarness.s3Config("missing-${UUID.randomUUID()}"),
                    stats = statsConfig,
                ),
                IntegrationHarness.runSession(runId),
            )
        }

        val row = runRow(runId)

        assertEquals(RunStatus.FAILED.name, row.getAs<String>("status"))
        assertNotNull(row.getAs<String?>("error_message"))
        assertEquals(2L, row.long("files_failed"))

        val failures =
            IntegrationHarness.spark
                .sql("SELECT source_path, attempts_used, error FROM $failuresTable WHERE run_id = '$runId'")
                .collectAsList()

        assertEquals(2, failures.size, "each failed file is its own row")
        assertTrue(failures.all { it.getAs<Int>("attempts_used") > 0 })
        assertTrue(failures.all { it.getAs<String>("error").isNotBlank() })
    }

    @Test
    fun `a failure row cap of zero records no failure rows but keeps the true count`() {
        val source = IntegrationHarness.freshBucket()
        seedS3(source, mapOf("a.txt" to "alpha".toByteArray(), "b.txt" to "bravo".toByteArray()))

        val runId = UUID.randomUUID().toString()
        assertFailsWith<IllegalStateException> {
            IntegrationHarness.runBackup(
                ApplicationConfig(
                    source = IntegrationHarness.s3Config(source),
                    target = IntegrationHarness.s3Config("missing-${UUID.randomUUID()}"),
                    stats = statsConfig.copy(maxFailureRows = 0),
                ),
                IntegrationHarness.runSession(runId),
            )
        }

        val row = runRow(runId)

        assertEquals(2L, row.long("files_failed"))
        assertEquals(true, row.getAs<Boolean>("failures_truncated"))
        assertEquals(
            0L,
            IntegrationHarness.spark.sql("SELECT * FROM $failuresTable WHERE run_id = '$runId'").count(),
        )
    }

    @Test
    fun `recording disabled leaves no trace of the run`() {
        val source = IntegrationHarness.freshBucket()
        val target = IntegrationHarness.freshBucket()
        seedS3(source, mapOf("a.txt" to "alpha".toByteArray()))

        val runId = UUID.randomUUID().toString()
        IntegrationHarness.runBackup(
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = IntegrationHarness.s3Config(target),
                stats = statsConfig.copy(enabled = false),
            ),
            IntegrationHarness.runSession(runId),
        )

        val recorded =
            IntegrationHarness.spark.catalog().tableExists(runsTable) &&
                IntegrationHarness.spark.sql("SELECT * FROM $runsTable WHERE run_id = '$runId'").count() > 0

        assertFalse(recorded, "a disabled recorder must not write a row")
    }

    @Test
    fun `two runs of the same backup are two rows with different run ids`() {
        val source = IntegrationHarness.freshBucket()
        val target = IntegrationHarness.freshBucket()
        seedS3(source, mapOf("a.txt" to "alpha".toByteArray()))

        val config =
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = IntegrationHarness.s3Config(target),
                stats = statsConfig,
            )

        val first = UUID.randomUUID().toString()
        val second = UUID.randomUUID().toString()
        IntegrationHarness.runBackup(config, IntegrationHarness.runSession(first))
        IntegrationHarness.runBackup(config, IntegrationHarness.runSession(second))

        // runRow fails on anything but exactly one row, so this also proves the merge does not duplicate.
        assertEquals(1L, runRow(first).long("files_listed"))
        assertEquals(1L, runRow(second).long("files_listed"))
    }
}
