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
    val totalEntries: Int = 0,
    val successCount: Int = 0,
    val failureCount: Int = 0,
    val skippedCount: Int = 0,
    val totalBytesCopied: Long = 0,
    val skippedBytes: Long = 0,
    val errors: List<String> = emptyList(),
)
