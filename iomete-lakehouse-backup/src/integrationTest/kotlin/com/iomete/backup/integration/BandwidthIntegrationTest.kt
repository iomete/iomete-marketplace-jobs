package com.iomete.backup.integration

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.CopyConfig
import com.iomete.backup.integration.fixtures.assertMatches
import com.iomete.backup.integration.fixtures.readS3
import com.iomete.backup.integration.fixtures.seedS3
import com.iomete.backup.integration.harness.IntegrationHarness
import org.junit.jupiter.api.Test
import kotlin.random.Random
import kotlin.test.assertTrue
import kotlin.time.measureTime

/** The cap holds end to end: config, rate resolution, shipping to the worker, pacing. */
class BandwidthIntegrationTest {
    private val capMbPerSec = 0.25
    private val payloadBytes = 2 * 1024 * 1024

    @Test
    fun `a capped run takes at least as long as its budget allows`() {
        val source = IntegrationHarness.freshBucket()
        val target = IntegrationHarness.freshBucket()
        // One file carries nearly the whole payload, so the elapsed floor bounds the rate within a file.
        val tree =
            mapOf(
                "big/part.bin" to Random(1).nextBytes(payloadBytes - 3 * 1024),
                "small/a.bin" to Random(2).nextBytes(1024),
                "small/b.bin" to Random(3).nextBytes(1024),
                "small/c.bin" to Random(4).nextBytes(1024),
            )
        seedS3(source, tree)
        IntegrationHarness.spark // started outside the measurement: session start-up is not pacing.

        val elapsed =
            measureTime {
                IntegrationHarness.runBackup(
                    ApplicationConfig(
                        source = IntegrationHarness.s3Config(source),
                        target = IntegrationHarness.s3Config(target),
                        copy = CopyConfig(maxBandwidthMbPerSec = capMbPerSec),
                        stats = IntegrationHarness.STATS_DISABLED,
                    ),
                )
            }

        // One-sided on purpose: an upper bound would fail on a loaded CI runner with no defect present.
        val budgetMs = (payloadBytes / (capMbPerSec * 1024 * 1024) * 1000).toLong()
        assertTrue(
            elapsed.inWholeMilliseconds >= budgetMs,
            "capped run finished in ${elapsed.inWholeMilliseconds} ms, faster than the $budgetMs ms budget",
        )
        assertMatches(tree, readS3(target))
    }

    @Test
    fun `an uncapped run needs no bandwidth configuration`() {
        val source = IntegrationHarness.freshBucket()
        val target = IntegrationHarness.freshBucket()
        val tree = mapOf("data/part.bin" to Random(5).nextBytes(payloadBytes))
        seedS3(source, tree)

        IntegrationHarness.runBackup(
            ApplicationConfig(
                source = IntegrationHarness.s3Config(source),
                target = IntegrationHarness.s3Config(target),
                stats = IntegrationHarness.STATS_DISABLED,
            ),
        )

        assertMatches(tree, readS3(target))
    }
}
