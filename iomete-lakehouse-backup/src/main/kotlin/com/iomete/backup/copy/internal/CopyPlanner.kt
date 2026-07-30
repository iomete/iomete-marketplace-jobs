package com.iomete.backup.copy.internal

import com.iomete.backup.copy.TempFiles
import com.iomete.backup.model.FileEntry

internal data class CopyPlan(
    val toCopy: List<FileEntry>,
    val skipped: List<FileEntry>,
)

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
            target != null &&
                target.size == source.size &&
                source.modificationTime + clockSkewToleranceMs <= target.modificationTime
        }

    return CopyPlan(toCopy = toCopy, skipped = skipped)
}
