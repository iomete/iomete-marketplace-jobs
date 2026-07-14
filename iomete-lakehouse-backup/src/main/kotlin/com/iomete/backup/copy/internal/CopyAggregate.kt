package com.iomete.backup.copy.internal

import com.iomete.backup.copy.CopyResult
import org.apache.spark.api.java.JavaRDD

private const val MAX_SAMPLED_FAILURES = 1000

internal data class CopyAggregate(
    val successCount: Int = 0,
    val failureCount: Int = 0,
    val totalBytesCopied: Long = 0,
    val failures: List<CopyResult> = emptyList(),
) : java.io.Serializable {
    fun add(result: CopyResult): CopyAggregate =
        if (result.success) {
            copy(
                successCount = successCount + 1,
                totalBytesCopied = totalBytesCopied + result.bytesCopied,
            )
        } else {
            copy(
                failureCount = failureCount + 1,
                failures = if (failures.size < MAX_SAMPLED_FAILURES) failures + result else failures,
            )
        }

    fun merge(other: CopyAggregate): CopyAggregate =
        CopyAggregate(
            successCount = successCount + other.successCount,
            failureCount = failureCount + other.failureCount,
            totalBytesCopied = totalBytesCopied + other.totalBytesCopied,
            failures = (failures + other.failures).take(MAX_SAMPLED_FAILURES),
        )
}

// Fold per-file results on the executors; only a bounded summary reaches the driver.
internal fun aggregateCopyResults(results: JavaRDD<CopyResult>): CopyAggregate =
    results.aggregate(
        CopyAggregate(),
        { acc, result -> acc.add(result) },
        { a, b -> a.merge(b) },
    )
