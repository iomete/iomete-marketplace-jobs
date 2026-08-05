package com.iomete.backup.copy.internal

import com.iomete.backup.config.StorageConfig
import com.iomete.backup.copy.CopyResult
import com.iomete.backup.copy.TempFiles
import com.iomete.backup.fs.FileSystemFactory
import com.iomete.backup.fs.HadoopConfigBuilder
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.IOException
import java.io.Serializable

private const val BUFFER_SIZE = 64 * 1024

class FileCopier(
    private val sourceConfig: StorageConfig,
    private val targetConfig: StorageConfig,
    private val sourceRoot: String,
    private val targetRoot: String,
    private val bytesPerSecPerExecutor: Double? = null,
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
        } catch (e: InterruptedException) {
            // A cancelled task must die, not become a failed file that fails the whole run.
            Thread.currentThread().interrupt()
            throw e
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
                    copyBytes(sourceFs, sourcePath, targetFs, tempPath)

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

    private fun copyBytes(
        sourceFs: FileSystem,
        sourcePath: Path,
        targetFs: FileSystem,
        tempPath: Path,
    ) {
        val limiter = bytesPerSecPerExecutor?.let { RateLimiter.shared(it) }
        val buffer = ByteArray(BUFFER_SIZE)

        sourceFs.open(sourcePath, BUFFER_SIZE).use { input ->
            targetFs.create(tempPath, true, BUFFER_SIZE).use { output ->
                while (true) {
                    val read = input.read(buffer)
                    if (read < 0) break
                    limiter?.acquire(read.toLong())
                    output.write(buffer, 0, read)
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
