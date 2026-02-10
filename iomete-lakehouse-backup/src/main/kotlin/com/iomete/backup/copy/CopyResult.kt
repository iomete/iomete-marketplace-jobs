package com.iomete.backup.copy

import java.io.Serializable

/**
 * Result of copying a single file.
 *
 * @property sourcePath The source file URI that was copied.
 * @property targetPath The target file URI that was written.
 * @property success Whether the copy succeeded.
 * @property bytesCopied Number of bytes written (0 on failure).
 * @property error Error message if the copy failed, null on success.
 */
data class CopyResult(
    val sourcePath: String,
    val targetPath: String,
    val success: Boolean,
    val bytesCopied: Long = 0,
    val error: String? = null,
    val attemptsUsed: Int = 1
) : Serializable

/**
 * Aggregated summary of a distributed copy job.
 *
 * @property totalFiles Total number of files that were attempted.
 * @property successCount Number of files copied successfully.
 * @property failureCount Number of files that failed to copy.
 * @property totalBytesCopied Total bytes successfully written.
 * @property errors List of error messages from failed copies.
 */
data class CopyJobSummary(
    val totalFiles: Int,
    val successCount: Int,
    val failureCount: Int,
    val totalBytesCopied: Long,
    val errors: List<String>
)

/**
 * Full output of a distributed copy execution.
 *
 * @property summary Aggregated counts and bytes.
 * @property fileResults Per-file success/failure state with reason and attempts used.
 */
data class CopyJobResult(
    val summary: CopyJobSummary,
    val fileResults: List<CopyResult>
)
