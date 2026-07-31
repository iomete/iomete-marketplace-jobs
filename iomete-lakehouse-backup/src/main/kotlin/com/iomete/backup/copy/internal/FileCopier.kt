package com.iomete.backup.copy.internal

import com.iomete.backup.config.StorageConfig
import com.iomete.backup.copy.CopyResult
import com.iomete.backup.copy.TempFiles
import com.iomete.backup.fs.FileSystemFactory
import com.iomete.backup.fs.HadoopConfigBuilder
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.Serializable

class FileCopier(
    private val sourceConfig: StorageConfig,
    private val targetConfig: StorageConfig,
    private val sourceRoot: String,
    private val targetRoot: String,
    private val maxAttempts: Int = RetryPolicy.COPY_MAX_ATTEMPTS,
    private val retryDelayMs: Long = RetryPolicy.DELAY_MS,
) : Serializable {
    @Transient
    private var logger = LoggerFactory.getLogger(FileCopier::class.java)

    /**
     * Lazily get or re-create the logger after deserialization.
     */
    private fun log(): Logger {
        if (logger == null) {
            logger = LoggerFactory.getLogger(FileCopier::class.java)
        }
        return logger
    }

    fun copySingleFile(sourceFilePath: String): CopyResult {
        val targetFilePath =
            try {
                PathResolver.resolveTargetPath(sourceFilePath, sourceRoot, targetRoot)
            } catch (e: IllegalArgumentException) {
                return CopyResult(
                    sourcePath = sourceFilePath,
                    targetPath = "",
                    success = false,
                    error = "${e.javaClass.simpleName}: ${e.message}",
                    attemptsUsed = 0,
                )
            }

        var attemptsUsed = 0

        return try {
            val bytesCopied =
                withRetries(
                    maxAttempts = maxAttempts,
                    retryDelayMs = retryDelayMs,
                    onFailedAttempt = { attempt, e ->
                        log().warn(
                            "Attempt {}/{} failed for {} -> {}: {}: {}",
                            attempt,
                            maxAttempts,
                            sourceFilePath,
                            targetFilePath,
                            e.javaClass.simpleName,
                            e.message,
                        )
                    },
                ) { attempt ->
                    attemptsUsed = attempt
                    copyOnce(sourceFilePath, targetFilePath)
                }

            log().debug(
                "Copied on attempt {}/{}: {} -> {} ({} bytes)",
                attemptsUsed,
                maxAttempts,
                sourceFilePath,
                targetFilePath,
                bytesCopied,
            )
            CopyResult(
                sourcePath = sourceFilePath,
                targetPath = targetFilePath,
                success = true,
                bytesCopied = bytesCopied,
                attemptsUsed = attemptsUsed,
            )
        } catch (e: Exception) {
            CopyResult(
                sourcePath = sourceFilePath,
                targetPath = targetFilePath,
                success = false,
                error = "${e.javaClass.simpleName}: ${e.message}",
                attemptsUsed = attemptsUsed,
            )
        }
    }

    private fun copyOnce(
        sourceFilePath: String,
        targetFilePath: String,
    ): Long {
        val sourceConf = HadoopConfigBuilder.build(sourceConfig)
        val targetConf = HadoopConfigBuilder.build(targetConfig)

        val sourcePath = Path(sourceFilePath)
        val targetPath = Path(targetFilePath)

        return FileSystemFactory.create(sourceConfig, sourcePath.toUri(), sourceConf).use { sourceFs ->
            FileSystemFactory.create(targetConfig, targetPath.toUri(), targetConf).use { targetFs ->
                val tempPath = TempFiles.pathFor(targetPath)

                targetPath.parent?.let { if (!targetFs.exists(it)) targetFs.mkdirs(it) }

                val sourceLen = sourceFs.getFileStatus(sourcePath).len

                try {
                    val copied = FileUtil.copy(sourceFs, sourcePath, targetFs, tempPath, false, true, targetConf)

                    if (!copied) {
                        throw IOException("FileUtil.copy reported failure: $sourceFilePath -> $tempPath")
                    }

                    val writtenLen = targetFs.getFileStatus(tempPath).len
                    if (writtenLen != sourceLen) {
                        throw IOException(
                            "Length verification failed for $targetFilePath: " +
                                "source=$sourceLen bytes, written=$writtenLen bytes",
                        )
                    }

                    // Overwrite deletes first (rename returns false on an existing destination), so
                    // targetPath is briefly absent; an atomic swap needs FileContext, and HDFS only.
                    if (targetFs.exists(targetPath)) targetFs.delete(targetPath, false)
                    if (!targetFs.rename(tempPath, targetPath)) {
                        throw IOException("Rename failed: $tempPath -> $targetFilePath")
                    }
                    sourceLen
                } catch (e: Exception) {
                    deleteQuietly(targetFs, tempPath)
                    throw e
                }
            }
        }
    }

    private fun deleteQuietly(
        fs: FileSystem,
        path: Path,
    ) {
        try {
            fs.delete(path, false)
        } catch (e: Exception) {
            log().warn("Best-effort temp cleanup failed for {}: {}", path, e.toString())
        }
    }
}
