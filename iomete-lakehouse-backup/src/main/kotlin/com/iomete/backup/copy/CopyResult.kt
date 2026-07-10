package com.iomete.backup.copy

import java.io.Serializable

data class CopyResult(
    val sourcePath: String,
    val targetPath: String,
    val success: Boolean,
    val bytesCopied: Long = 0,
    val error: String? = null,
    val attemptsUsed: Int = 1
) : Serializable

data class CopyJobResult(
    val summary: CopyJobSummary,
    val fileResults: List<CopyResult>
)

data class CopyJobSummary(
    val totalFiles: Int,
    val successCount: Int,
    val failureCount: Int,
    val totalBytesCopied: Long,
    val errors: List<String>
)