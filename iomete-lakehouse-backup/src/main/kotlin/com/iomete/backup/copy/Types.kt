package com.iomete.backup.copy

import java.io.Serializable

data class CopyResult(
    val sourcePath: String,
    val targetPath: String,
    val success: Boolean,
    val bytesCopied: Long = 0,
    val error: String? = null,
    val attemptsUsed: Int = 1,
) : Serializable

data class CopyJobResult(
    val summary: CopyJobSummary,
    val failedResults: List<CopyResult>,
    val stats: CopyStats = CopyStats(),
)

data class CopyStats(
    val targetListingMs: Long = 0,
    val planningMs: Long = 0,
    val copyMs: Long = 0,
    val dirCreateMs: Long = 0,
    val taskCount: Int = 0,
    val largestFileBytes: Long = 0,
    val filesCopied: Long = 0,
    val dirsCreated: Long = 0,
    val retriesUsed: Long = 0,
    val failuresTruncated: Boolean = false,
    val executor: ExecutorTimings = ExecutorTimings(),
)

data class ExecutorTimings(
    val copyTaskMs: Long = 0,
    val fsInitMs: Long = 0,
    val sourceReadMs: Long = 0,
    val targetWriteMs: Long = 0,
    val throttleWaitMs: Long = 0,
    val verifyMs: Long = 0,
    val commitMs: Long = 0,
    val retrySleepMs: Long = 0,
)

data class CopyJobSummary(
    val totalEntries: Int,
    val successCount: Int,
    val failureCount: Int,
    val skippedCount: Int,
    val totalBytesCopied: Long,
    val skippedBytes: Long,
    val errors: List<String>,
) {
    companion object {
        val EMPTY = CopyJobSummary(0, 0, 0, 0, 0, 0, emptyList())
    }
}
