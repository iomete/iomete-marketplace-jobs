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
