package com.iomete.backup.integration

import com.iomete.backup.BackupJob
import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.StorageConfig
import com.iomete.backup.integration.fixtures.assertMatches
import com.iomete.backup.integration.fixtures.fixture
import com.iomete.backup.integration.fixtures.readHdfs
import com.iomete.backup.integration.fixtures.readS3
import com.iomete.backup.integration.fixtures.seedS3
import com.iomete.backup.integration.harness.IntegrationHarness
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.test.assertTrue

class BackupJobIntegrationTest {
    class Target(
        val config: StorageConfig,
        val read: () -> Map<String, ByteArray>,
    )

    @ParameterizedTest(name = "s3 to {0} happy path copies every file byte-for-byte")
    @MethodSource("targets")
    fun happyPath(
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
