package com.iomete.backup.integration

import com.iomete.backup.BackupJob
import com.iomete.backup.config.ApplicationConfig
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
