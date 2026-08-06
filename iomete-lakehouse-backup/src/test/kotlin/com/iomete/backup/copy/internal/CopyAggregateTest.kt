package com.iomete.backup.copy.internal

import com.iomete.backup.copy.CopyResult
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CopyAggregateTest {
    private lateinit var jsc: JavaSparkContext

    @BeforeAll
    fun setup() {
        jsc = JavaSparkContext(SparkConf().setAppName("copy-aggregate-test").setMaster("local[2]"))
    }

    @AfterAll
    fun tearDown() {
        jsc.close()
    }

    private fun success(
        path: String,
        bytes: Long,
    ) = CopyResult(sourcePath = path, targetPath = "t/$path", success = true, bytesCopied = bytes, attemptsUsed = 1)

    private fun failure(path: String) =
        CopyResult(sourcePath = path, targetPath = "t/$path", success = false, error = "boom", attemptsUsed = 3)

    @Test
    fun `aggregates counts and bytes across partitions`() {
        val results = listOf(success("a", 10), success("b", 20), failure("c"), success("d", 30), failure("e"))
        val rdd = jsc.parallelize(results, 3)

        val agg = aggregateCopyResults(rdd, maxSampledFailures = 1000)

        assertEquals(3, agg.successCount)
        assertEquals(2, agg.failureCount)
        assertEquals(60L, agg.totalBytesCopied)
        assertEquals(setOf("c", "e"), agg.failures.map { it.sourcePath }.toSet())
    }

    @Test
    fun `caps sampled failures while keeping exact failure count`() {
        val results = (1..1500).map { failure("f$it") }
        val rdd = jsc.parallelize(results, 4)

        val agg = aggregateCopyResults(rdd, maxSampledFailures = 1000)

        assertEquals(0, agg.successCount)
        assertEquals(1500, agg.failureCount)
        assertEquals(1000, agg.failures.size)
        assertTrue(agg.failuresTruncated)
    }

    @Test
    fun `the failure cap is configurable and zero keeps none`() {
        val rdd = jsc.parallelize((1..20).map { failure("f$it") }, 4)

        val capped = aggregateCopyResults(rdd, maxSampledFailures = 5)
        assertEquals(20, capped.failureCount)
        assertEquals(5, capped.failures.size)
        assertTrue(capped.failuresTruncated)

        val none = aggregateCopyResults(rdd, maxSampledFailures = 0)
        assertEquals(20, none.failureCount)
        assertEquals(0, none.failures.size)
        assertTrue(none.failuresTruncated)
    }

    @Test
    fun `counts the attempts beyond the first, and reports no truncation when every failure fits`() {
        val rdd = jsc.parallelize(listOf(success("a", 10), failure("b"), success("c", 20)), 2)

        val agg = aggregateCopyResults(rdd, maxSampledFailures = 1000)

        assertEquals(2L, agg.retriesUsed)
        assertFalse(agg.failuresTruncated)
    }
}
