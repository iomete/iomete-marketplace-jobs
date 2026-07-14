package com.iomete.backup.copy.internal

import com.iomete.backup.copy.CopyResult
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CopyAggregateFoldTest {
    private fun success(bytes: Long) = CopyResult(sourcePath = "s", targetPath = "t", success = true, bytesCopied = bytes, attemptsUsed = 1)

    private fun failure(path: String) =
        CopyResult(sourcePath = path, targetPath = "t/$path", success = false, error = "boom", attemptsUsed = 3)

    @Test
    fun `empty aggregate is zero`() {
        val agg = CopyAggregate()
        assertEquals(0, agg.successCount)
        assertEquals(0, agg.failureCount)
        assertEquals(0L, agg.totalBytesCopied)
        assertEquals(emptyList(), agg.failures)
    }

    @Test
    fun `add accumulates success counts and bytes without recording failures`() {
        val agg = listOf(success(10), success(20), success(30)).fold(CopyAggregate()) { a, r -> a.add(r) }
        assertEquals(3, agg.successCount)
        assertEquals(0, agg.failureCount)
        assertEquals(60L, agg.totalBytesCopied)
        assertEquals(emptyList(), agg.failures)
    }

    @Test
    fun `add caps sampled failures within a single partition while counting all`() {
        val agg = (1..1500).fold(CopyAggregate()) { a, i -> a.add(failure("f$i")) }
        assertEquals(1500, agg.failureCount)
        assertEquals(1000, agg.failures.size)
        assertEquals("f1", agg.failures.first().sourcePath)
        assertEquals("f1000", agg.failures.last().sourcePath)
    }

    @Test
    fun `merge re-caps concatenated failures and sums counts`() {
        val left = (1..700).fold(CopyAggregate()) { a, i -> a.add(failure("l$i")) }
        val right = (1..700).fold(CopyAggregate()) { a, i -> a.add(failure("r$i")) }

        val merged = left.merge(right)

        assertEquals(1400, merged.failureCount)
        assertEquals(1000, merged.failures.size)
    }
}
