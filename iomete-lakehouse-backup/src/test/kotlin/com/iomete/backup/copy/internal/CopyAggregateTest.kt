package com.iomete.backup.copy.internal

import com.iomete.backup.copy.CopyResult
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

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

        val agg = aggregateCopyResults(rdd)

        assertEquals(3, agg.successCount)
        assertEquals(2, agg.failureCount)
        assertEquals(60L, agg.totalBytesCopied)
        assertEquals(setOf("c", "e"), agg.failures.map { it.sourcePath }.toSet())
    }

    @Test
    fun `caps sampled failures while keeping exact failure count`() {
        val results = (1..1500).map { failure("f$it") }
        val rdd = jsc.parallelize(results, 4)

        val agg = aggregateCopyResults(rdd)

        assertEquals(0, agg.successCount)
        assertEquals(1500, agg.failureCount)
        assertEquals(1000, agg.failures.size)
    }
}
