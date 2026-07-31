package com.iomete.backup.integration

import com.iomete.backup.BackupJob
import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.CopyConfig
import com.iomete.backup.config.StorageConfig
import com.iomete.backup.integration.fixtures.assertMatches
import com.iomete.backup.integration.fixtures.directoryExists
import com.iomete.backup.integration.fixtures.fixture
import com.iomete.backup.integration.fixtures.readHdfs
import com.iomete.backup.integration.fixtures.readS3
import com.iomete.backup.integration.fixtures.seedHdfs
import com.iomete.backup.integration.fixtures.seedHdfsDirectories
import com.iomete.backup.integration.fixtures.seedS3
import com.iomete.backup.integration.harness.IntegrationHarness
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BackupJobIntegrationTest {
    class Target(
        val config: StorageConfig,
        val read: () -> Map<String, ByteArray>,
    )

    @ParameterizedTest(name = "s3 to {0} happy path copies every file byte-for-byte")
    @MethodSource("targets")
    fun s3SourceHappyPath(
        @Suppress("UNUSED_PARAMETER") name: String,
        makeTarget: () -> Target,
    ) {
        val source = IntegrationHarness.freshBucket()
        val tree = fixture()

        seedS3(source, tree)

        val target = makeTarget()
        BackupJob.run(
            IntegrationHarness.spark,
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = target.config,
            ),
        )

        assertMatches(tree, target.read())
    }

    @ParameterizedTest(name = "hdfs to {0} happy path copies every file byte-for-byte")
    @MethodSource("targets")
    fun hdfsSourceHappyPath(
        @Suppress("UNUSED_PARAMETER") name: String,
        makeTarget: () -> Target,
    ) {
        val source = IntegrationHarness.freshHdfsPath()
        val tree = fixture()

        seedHdfs(source, tree)

        val target = makeTarget()
        BackupJob.run(
            IntegrationHarness.spark,
            ApplicationConfig(
                source = IntegrationHarness.hdfsConfig(source),
                target = target.config,
            ),
        )

        assertMatches(tree, target.read())
    }

    @ParameterizedTest(name = "hdfs to {0} copies files and recreates empty directories together")
    @MethodSource("targets")
    fun hdfsSourceFilesAndEmptyDirectories(
        @Suppress("UNUSED_PARAMETER") name: String,
        makeTarget: () -> Target,
    ) {
        val source = IntegrationHarness.freshHdfsPath()
        val tree = fixture()
        seedHdfs(source, tree)
        seedHdfsDirectories(source, "empty", "nested/empty")
        val target = makeTarget()

        BackupJob.run(
            IntegrationHarness.spark,
            ApplicationConfig(
                source = IntegrationHarness.hdfsConfig(source),
                target = target.config,
            ),
        )

        assertMatches(tree, target.read())
        assertTrue(directoryExists(target.config, "empty"))
        assertTrue(directoryExists(target.config, "nested/empty"))
    }

    @ParameterizedTest(name = "hdfs to {0} recreates empty directories")
    @MethodSource("targets")
    fun hdfsSourceEmptyDirectories(
        @Suppress("UNUSED_PARAMETER") name: String,
        makeTarget: () -> Target,
    ) {
        val source = IntegrationHarness.freshHdfsPath()
        seedHdfsDirectories(source, "empty", "nested/empty")
        val target = makeTarget()

        BackupJob.run(
            IntegrationHarness.spark,
            ApplicationConfig(
                source = IntegrationHarness.hdfsConfig(source),
                target = target.config,
            ),
        )

        assertTrue(directoryExists(target.config, "empty"))
        assertTrue(directoryExists(target.config, "nested/empty"))
    }

    @ParameterizedTest(name = "s3 to {0} re-run skips identical files and copies only what changed")
    @MethodSource("targets")
    fun rerunSkipsIdenticalFiles(
        @Suppress("UNUSED_PARAMETER") name: String,
        makeTarget: () -> Target,
    ) {
        val source = IntegrationHarness.freshBucket()
        val tree = fixture()
        seedS3(source, tree)

        val target = makeTarget()
        val config =
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = target.config,
                copy = CopyConfig(clockSkewToleranceMs = 0),
            )

        // MinIO truncates timestamps to whole seconds, so a copy taken within the same second as its
        // source can be reported as older than it; wait the truncation out before each run.
        Thread.sleep(1_100)

        val first = BackupJob.run(IntegrationHarness.spark, config)
        assertEquals(tree.size, first.successCount, "first run must copy everything")
        assertEquals(0, first.skippedCount)

        val second = BackupJob.run(IntegrationHarness.spark, config)
        assertEquals(0, second.successCount, "re-run against an unchanged source must copy nothing")
        assertEquals(tree.size, second.skippedCount)
        assertEquals(tree.size, second.totalEntries)
        assertEquals(tree.values.sumOf { it.size.toLong() }, second.skippedBytes)

        Thread.sleep(1_100)
        val changed = tree + ("root.txt" to "ROOT FILE".toByteArray())
        seedS3(source, mapOf("root.txt" to changed.getValue("root.txt")))

        val third = BackupJob.run(IntegrationHarness.spark, config)
        assertEquals(1, third.successCount, "only the rewritten file must be copied again")
        assertEquals(tree.size - 1, third.skippedCount)
        assertMatches(changed, target.read())
    }

    @Test
    fun `empty source is a no-op and writes nothing`() {
        val source = IntegrationHarness.freshBucket()
        val target = IntegrationHarness.freshBucket()

        BackupJob.run(
            IntegrationHarness.spark,
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = IntegrationHarness.s3Config(target),
            ),
        )

        assertTrue(readS3(target).isEmpty(), "empty source must not write to target")
    }

    @Test
    fun `a failed copy surfaces as IllegalStateException from the entry point`() {
        val source = IntegrationHarness.freshBucket()
        seedS3(source, mapOf("root.txt" to "payload".toByteArray()))

        // Target bucket is never created, so every write fails -> the job's failure check throws.
        val missingTarget = IntegrationHarness.s3Config("missing-${UUID.randomUUID()}")

        assertFailsWith<IllegalStateException> {
            BackupJob.run(
                IntegrationHarness.spark,
                ApplicationConfig(source = IntegrationHarness.s3Config(source), target = missingTarget),
            )
        }
    }

    companion object {
        @JvmStatic
        fun targets(): List<Arguments> {
            val s3: () -> Target = {
                val bucket = IntegrationHarness.freshBucket()
                Target(IntegrationHarness.s3Config(bucket)) { readS3(bucket) }
            }
            val hdfs: () -> Target = {
                val path = IntegrationHarness.freshHdfsPath()
                Target(IntegrationHarness.hdfsConfig(path)) { readHdfs(path) }
            }
            return listOf(Arguments.of("s3", s3), Arguments.of("hdfs", hdfs))
        }
    }
}
