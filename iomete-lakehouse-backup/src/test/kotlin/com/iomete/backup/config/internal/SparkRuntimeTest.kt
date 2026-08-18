package com.iomete.backup.config.internal

import com.iomete.backup.config.ConfigValidationException
import org.apache.spark.SparkConf
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

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
    fun `a vCPU limit is read as a plain count or as millicores`() {
        assertEquals(2.0, SparkRuntime.vcpuPerExecutor(clusterConf(SparkRuntime.LIMIT_CORES to "2")))
        assertEquals(2.0, SparkRuntime.vcpuPerExecutor(clusterConf(SparkRuntime.LIMIT_CORES to "2000m")))
        assertEquals(1.5, SparkRuntime.vcpuPerExecutor(clusterConf(SparkRuntime.LIMIT_CORES to "1500m")))
    }

    @Test
    fun `a local master is one vCPU, with no pod limit to read`() {
        assertEquals(1.0, SparkRuntime.vcpuPerExecutor(SparkConf(false).set("spark.master", "local[8]")))
    }

    @Test
    fun `a vCPU limit that is missing or unreadable fails the run instead of being ignored`() {
        listOf(null, "0", "two", "2000mm").forEach { limit ->
            val conf = limit?.let { clusterConf(SparkRuntime.LIMIT_CORES to it) } ?: clusterConf()

            val e = assertThrows<ConfigValidationException> { SparkRuntime.vcpuPerExecutor(conf) }

            assertTrue(e.errors.single().contains(SparkRuntime.LIMIT_CORES), e.message!!)
        }
    }

    @Test
    fun `slots are the vCPU count times the setting, rounded up to a whole slot`() {
        assertEquals(8, SparkRuntime.slotsPerExecutor(clusterConf(SparkRuntime.LIMIT_CORES to "4"), 2))
        assertEquals(3, SparkRuntime.slotsPerExecutor(clusterConf(SparkRuntime.LIMIT_CORES to "1500m"), 2))
        assertEquals(1, SparkRuntime.slotsPerExecutor(clusterConf(SparkRuntime.LIMIT_CORES to "250m"), 1))
    }

    @Test
    fun `the cap is divided across the executors`() {
        assertEquals(150 * MB, SparkRuntime.bytesPerSecPerExecutor(600.0, 4))
    }
}
