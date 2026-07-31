package com.iomete.backup.copy.internal

import com.iomete.backup.copy.TempFiles
import com.iomete.backup.model.FileEntry
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException

private val logger = LoggerFactory.getLogger("com.iomete.backup.copy.internal.CopyPlanner")

internal data class CopyPlan(
    val toCopy: List<FileEntry>,
    val skipped: List<FileEntry>,
)

internal fun listTargetWithRetries(
    maxAttempts: Int = RetryPolicy.LISTING_MAX_ATTEMPTS,
    retryDelayMs: Long = RetryPolicy.DELAY_MS,
    list: () -> List<FileEntry>,
): List<FileEntry> {
    for (attempt in 1..maxAttempts) {
        try {
            return list()
        } catch (_: FileNotFoundException) {
            // Target root absent is the normal first run, not an error.
            return emptyList()
        } catch (e: Exception) {
            if (isTerminal(e) || attempt == maxAttempts) {
                logger.warn(
                    "Target listing failed after {} attempt(s), copying every file: {}: {}",
                    attempt,
                    e.javaClass.simpleName,
                    e.message,
                )
                return emptyList()
            }

            logger.warn(
                "Target listing attempt {}/{} failed: {}: {}",
                attempt,
                maxAttempts,
                e.javaClass.simpleName,
                e.message,
            )
            Thread.sleep(fullJitterDelayMs(attempt, retryDelayMs))
        }
    }

    return emptyList()
}

// Length and modification time only: S3 and HDFS checksums are mutually incompatible.
internal fun planCopy(
    sourceFiles: List<FileEntry>,
    sourceRoot: String,
    targetFiles: List<FileEntry>,
    targetRoot: String,
    clockSkewToleranceMs: Long,
): CopyPlan {
    val targetIndex =
        targetFiles
            .filterNot { TempFiles.isTemp(it.path.substringAfterLast('/')) }
            .associateBy { PathResolver.relativize(it.path, targetRoot) }

    val (skipped, toCopy) =
        sourceFiles.partition { source ->
            val target = targetIndex[PathResolver.relativize(source.path, sourceRoot)]
            target != null && target.size == source.size && source.modificationTime + clockSkewToleranceMs <= target.modificationTime
        }

    return CopyPlan(toCopy = toCopy, skipped = skipped)
}
