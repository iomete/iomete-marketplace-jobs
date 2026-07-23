package com.iomete.backup.integration

import com.iomete.backup.BackupJob
import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.integration.fixtures.assertMatches
import com.iomete.backup.integration.fixtures.fixture
import com.iomete.backup.integration.fixtures.readHdfs
import com.iomete.backup.integration.fixtures.readS3
import com.iomete.backup.integration.fixtures.seedS3
import com.iomete.backup.integration.harness.IntegrationHarness
import org.junit.jupiter.api.Test
import kotlin.test.assertTrue

class BackupJobIntegrationTest {
    @Test
    fun `s3 to s3 happy path copies every file byte-for-byte`() {
        val source = IntegrationHarness.freshBucket()
        val target = IntegrationHarness.freshBucket()
        val tree = fixture()

        seedS3(source, tree)

        BackupJob.run(
            IntegrationHarness.spark,
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = IntegrationHarness.s3Config(target),
            ),
        )

        assertMatches(tree, readS3(target))
    }

    @Test
    fun `s3 to hdfs happy path copies every file byte-for-byte`() {
        val source = IntegrationHarness.freshBucket()
        val target = IntegrationHarness.freshHdfsPath()
        val tree = fixture()

        seedS3(source, tree)

        BackupJob.run(
            IntegrationHarness.spark,
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = IntegrationHarness.hdfsConfig(target),
            ),
        )

        assertMatches(tree, readHdfs(target))
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
}
