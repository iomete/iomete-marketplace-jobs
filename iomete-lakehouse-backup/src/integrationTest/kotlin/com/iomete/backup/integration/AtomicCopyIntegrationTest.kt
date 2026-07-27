package com.iomete.backup.integration

import com.iomete.backup.BackupJob
import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.StorageConfig
import com.iomete.backup.integration.fixtures.assertMatches
import com.iomete.backup.integration.fixtures.readHdfs
import com.iomete.backup.integration.fixtures.readS3
import com.iomete.backup.integration.fixtures.seedS3
import com.iomete.backup.integration.harness.FaultInjection
import com.iomete.backup.integration.harness.IntegrationHarness
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments
import org.junit.jupiter.params.provider.MethodSource
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Verify-before-visible under induced mid-copy failure, on both target schemes. */
class AtomicCopyIntegrationTest {
    class Target(
        val config: StorageConfig,
        val read: () -> Map<String, ByteArray>,
    )

    interface Scheme {
        fun target(hadoopOptions: Map<String, String> = emptyMap()): Target

        fun faultOptions(
            afterBytes: Long,
            maxFailures: Int,
            targetName: String? = null,
        ): Map<String, String>
    }

    @ParameterizedTest(name = "mid-copy failure on {0} leaves no file at final path and no temp residue")
    @MethodSource("schemes")
    fun midCopyFailureLeavesNothing(
        @Suppress("UNUSED_PARAMETER") name: String,
        scheme: Scheme,
    ) {
        val source = IntegrationHarness.freshBucket()
        seedS3(source, mapOf("data/part.bin" to Random(7).nextBytes(FILE_SIZE)))

        val target = scheme.target(scheme.faultOptions(afterBytes = FAIL_AT, maxFailures = -1))

        assertFailsWith<IllegalStateException> {
            BackupJob.run(
                IntegrationHarness.spark,
                ApplicationConfig(source = IntegrationHarness.s3Config(source), target = target.config),
            )
        }

        assertTrue(target.read().isEmpty(), "a failed copy must leave neither final file nor temp residue")
    }

    @ParameterizedTest(name = "partial failure on {0} keeps completed files and removes the failed one")
    @MethodSource("schemes")
    fun partialFailureKeepsCompletedFiles(
        @Suppress("UNUSED_PARAMETER") name: String,
        scheme: Scheme,
    ) {
        val source = IntegrationHarness.freshBucket()
        val completed =
            mapOf(
                "data/a.bin" to Random(1).nextBytes(FILE_SIZE),
                "data/b.bin" to Random(2).nextBytes(FILE_SIZE),
            )
        seedS3(source, completed + mapOf("data/c.bin" to Random(3).nextBytes(FILE_SIZE)))

        val target = scheme.target(scheme.faultOptions(afterBytes = FAIL_AT, maxFailures = -1, targetName = "c.bin"))

        assertFailsWith<IllegalStateException> {
            BackupJob.run(
                IntegrationHarness.spark,
                ApplicationConfig(source = IntegrationHarness.s3Config(source), target = target.config),
            )
        }

        assertMatches(completed, target.read())
    }

    @ParameterizedTest(name = "fail-once-then-recover on {0} succeeds within the retry loop")
    @MethodSource("schemes")
    fun failOnceThenRecover(
        @Suppress("UNUSED_PARAMETER") name: String,
        scheme: Scheme,
    ) {
        val source = IntegrationHarness.freshBucket()
        val payload = Random(9).nextBytes(FILE_SIZE)
        val tree = mapOf("data/part.bin" to payload)
        seedS3(source, tree)

        val target = scheme.target(scheme.faultOptions(afterBytes = FAIL_AT, maxFailures = 1))

        BackupJob.run(
            IntegrationHarness.spark,
            ApplicationConfig(source = IntegrationHarness.s3Config(source), target = target.config),
        )

        assertMatches(tree, target.read())
    }

    companion object {
        private const val FILE_SIZE = 8192
        private const val FAIL_AT = 2048L

        private val s3Scheme =
            object : Scheme {
                override fun target(hadoopOptions: Map<String, String>): Target {
                    val bucket = IntegrationHarness.freshBucket()
                    return Target(
                        config = IntegrationHarness.s3Config(bucket, hadoopOptions),
                        read = { readS3(bucket) },
                    )
                }

                override fun faultOptions(
                    afterBytes: Long,
                    maxFailures: Int,
                    targetName: String?,
                ) = FaultInjection.s3Options(afterBytes, maxFailures, targetName)
            }

        private val hdfsScheme =
            object : Scheme {
                override fun target(hadoopOptions: Map<String, String>): Target {
                    val path = IntegrationHarness.freshHdfsPath()
                    return Target(
                        config = IntegrationHarness.hdfsConfig(path, hadoopOptions),
                        read = { readHdfs(path) },
                    )
                }

                override fun faultOptions(
                    afterBytes: Long,
                    maxFailures: Int,
                    targetName: String?,
                ) = FaultInjection.hdfsOptions(afterBytes, maxFailures, targetName)
            }

        @JvmStatic
        fun schemes(): List<Arguments> = listOf(Arguments.of("s3", s3Scheme), Arguments.of("hdfs", hdfsScheme))
    }
}
