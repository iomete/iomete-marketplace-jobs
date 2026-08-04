package com.iomete.backup.config.internal

import org.apache.spark.SparkConf
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

private const val MB = 1024.0 * 1024.0

class SparkRuntimeTest {
    private fun clusterConf(vararg settings: Pair<String, String>): SparkConf =
        SparkConf(false)
            .set("spark.master", "k8s://https://kubernetes.default.svc")
            .also { conf -> settings.forEach { (key, value) -> conf.set(key, value) } }

    @Test
    fun `a local master counts as a single executor`() {
        assertEquals(1, SparkRuntime.executorCount(SparkConf(false).set("spark.master", "local[8]")))
    }

    @Test
    fun `a static cluster counts the configured instances`() {
        assertEquals(4, SparkRuntime.executorCount(clusterConf("spark.executor.instances" to "4")))
    }

    @Test
    fun `a dynamic cluster counts the maximum executors, not the instances`() {
        val conf =
            clusterConf(
                "spark.dynamicAllocation.enabled" to "true",
                "spark.dynamicAllocation.maxExecutors" to "10",
                "spark.executor.instances" to "2",
            )

        assertEquals(10, SparkRuntime.executorCount(conf))
    }

    @Test
    fun `a missing instance count is unknown rather than assumed`() {
        assertNull(SparkRuntime.executorCount(clusterConf()))
    }

    @Test
    fun `an unbounded dynamic cluster is unknown rather than assumed`() {
        assertNull(SparkRuntime.executorCount(clusterConf("spark.dynamicAllocation.enabled" to "true")))
    }

    @Test
    fun `a non-positive instance count is unknown rather than assumed`() {
        assertNull(SparkRuntime.executorCount(clusterConf("spark.executor.instances" to "0")))
    }

    @Test
    fun `the setting to fix names the one the cluster actually reads`() {
        assertEquals("spark.executor.instances", SparkRuntime.executorSetting(clusterConf()))
        assertEquals(
            "spark.dynamicAllocation.maxExecutors",
            SparkRuntime.executorSetting(clusterConf("spark.dynamicAllocation.enabled" to "true")),
        )
    }

    @Test
    fun `the cap is divided across the executors`() {
        assertEquals(150 * MB, SparkRuntime.bytesPerSecPerExecutor(600.0, 4))
    }
}
