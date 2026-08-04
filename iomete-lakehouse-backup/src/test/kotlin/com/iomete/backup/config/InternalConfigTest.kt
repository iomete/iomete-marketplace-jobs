package com.iomete.backup.config

import org.apache.spark.SparkConf
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

private const val MB = 1024.0 * 1024.0

class InternalConfigTest {
    private fun config(maxBandwidthMbPerSec: Double? = 600.0): ApplicationConfig {
        val storage = S3Config(bucket = "bucket", accessKey = "key", secretKey = "secret")
        return ApplicationConfig(
            source = storage,
            target = storage,
            copy = CopyConfig(maxBandwidthMbPerSec = maxBandwidthMbPerSec),
        )
    }

    private fun clusterConf(vararg settings: Pair<String, String>): SparkConf =
        SparkConf(false)
            .set("spark.master", "k8s://https://kubernetes.default.svc")
            .also { conf -> settings.forEach { (key, value) -> conf.set(key, value) } }

    @Test
    fun `no cap needs no executor count`() {
        val internalConfig = ConfigLoader.loadInternalConfig(config(maxBandwidthMbPerSec = null), clusterConf())

        assertNull(internalConfig.bytesPerSecPerExecutor)
    }

    @Test
    fun `a local master counts as a single executor`() {
        val conf = SparkConf(false).set("spark.master", "local[8]")

        assertEquals(600 * MB, ConfigLoader.loadInternalConfig(config(), conf).bytesPerSecPerExecutor)
    }

    @Test
    fun `a static cluster divides the cap by the configured instances`() {
        val conf = clusterConf("spark.executor.instances" to "4")

        assertEquals(150 * MB, ConfigLoader.loadInternalConfig(config(), conf).bytesPerSecPerExecutor)
    }

    @Test
    fun `a dynamic cluster divides the cap by the maximum executors`() {
        val conf =
            clusterConf(
                "spark.dynamicAllocation.enabled" to "true",
                "spark.dynamicAllocation.maxExecutors" to "10",
                "spark.executor.instances" to "2",
            )

        assertEquals(60 * MB, ConfigLoader.loadInternalConfig(config(), conf).bytesPerSecPerExecutor)
    }

    @Test
    fun `a missing instance count is rejected naming the setting to fix`() {
        val error =
            assertFailsWith<ConfigValidationException> { ConfigLoader.loadInternalConfig(config(), clusterConf()) }

        assertTrue(error.errors.single().contains("spark.executor.instances"), error.message!!)
    }

    @Test
    fun `an unbounded dynamic cluster is rejected naming the setting to fix`() {
        val conf = clusterConf("spark.dynamicAllocation.enabled" to "true")

        val error = assertFailsWith<ConfigValidationException> { ConfigLoader.loadInternalConfig(config(), conf) }

        assertTrue(error.errors.single().contains("spark.dynamicAllocation.maxExecutors"), error.message!!)
    }
}
