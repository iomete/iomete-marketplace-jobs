package com.iomete.backup.copy.internal

import com.iomete.backup.copy.CopyResult
import org.apache.spark.api.java.JavaRDD

internal data class CopyAggregate(
    val successCount: Int = 0,
    val failureCount: Int = 0,
    val totalBytesCopied: Long = 0,
    val retriesUsed: Long = 0,
    val failures: List<CopyResult> = emptyList(),
    val maxSampledFailures: Int,
) : java.io.Serializable {
    val failuresTruncated: Boolean get() = failureCount > failures.size

    fun add(result: CopyResult): CopyAggregate {
        val retried = copy(retriesUsed = retriesUsed + maxOf(0, result.attemptsUsed - 1))

        return if (result.success) {
            retried.copy(
                successCount = successCount + 1,
                totalBytesCopied = totalBytesCopied + result.bytesCopied,
            )
        } else {
            retried.copy(
                failureCount = failureCount + 1,
                failures = if (failures.size < maxSampledFailures) failures + result else failures,
            )
        }
    }

    fun merge(other: CopyAggregate): CopyAggregate =
        copy(
            successCount = successCount + other.successCount,
            failureCount = failureCount + other.failureCount,
            totalBytesCopied = totalBytesCopied + other.totalBytesCopied,
            retriesUsed = retriesUsed + other.retriesUsed,
            failures = (failures + other.failures).take(maxSampledFailures),
        )
}

// Fold per-file results on the executors; only a bounded summary reaches the driver.
internal fun aggregateCopyResults(
    results: JavaRDD<CopyResult>,
    maxSampledFailures: Int,
): CopyAggregate =
    results.aggregate(
        CopyAggregate(maxSampledFailures = maxSampledFailures),
        { acc, result -> acc.add(result) },
        { a, b -> a.merge(b) },
    )
