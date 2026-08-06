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
    private val timers: CopyTimers = CopyTimers.unregistered(),
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

    fun copySingleFile(sourceFilePath: String): CopyResult = timers.copyTask.timeNanos { copyFile(sourceFilePath) }

    private fun copyFile(sourceFilePath: String): CopyResult {
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
                    onRetrySleep = { timers.retrySleep.add(it) },
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
        val sourcePath = Path(sourceFilePath)
        val targetPath = Path(targetFilePath)

        return openFileSystem(sourceConfig, sourcePath).use { sourceFs ->
            openFileSystem(targetConfig, targetPath).use { targetFs ->
                val tempPath = TempFiles.pathFor(targetPath)

                timers.targetWrite.timeNanos { targetPath.parent?.let { if (!targetFs.exists(it)) targetFs.mkdirs(it) } }

                val sourceLen = timers.sourceRead.timeNanos { sourceFs.getFileStatus(sourcePath).len }

                try {
                    copyBytes(sourceFs, sourcePath, targetFs, tempPath)

                    val writtenLen = timers.verify.timeNanos { targetFs.getFileStatus(tempPath).len }
                    if (writtenLen != sourceLen) {
                        throw IOException(
                            "Length verification failed for $targetFilePath: " +
                                "source=$sourceLen bytes, written=$writtenLen bytes",
                        )
                    }

                    timers.commit.timeNanos {
                        // Overwrite deletes first (rename returns false on an existing destination), so
                        // targetPath is briefly absent; an atomic swap needs FileContext, and HDFS only.
                        if (targetFs.exists(targetPath)) targetFs.delete(targetPath, false)
                        if (!targetFs.rename(tempPath, targetPath)) {
                            throw IOException("Rename failed: $tempPath -> $targetFilePath")
                        }
                    }
                    sourceLen
                } catch (e: Exception) {
                    deleteQuietly(targetFs, tempPath)
                    throw e
                }
            }
        }
    }

    private fun openFileSystem(
        config: StorageConfig,
        path: Path,
    ): FileSystem =
        timers.fsInit.timeNanos {
            FileSystemFactory.create(config, path.toUri(), HadoopConfigBuilder.build(config))
        }

    private fun copyBytes(
        sourceFs: FileSystem,
        sourcePath: Path,
        targetFs: FileSystem,
        tempPath: Path,
    ) {
        val limiter = bytesPerSecPerExecutor?.let { RateLimiter.shared(it) }
        val buffer = ByteArray(BUFFER_SIZE)
        var readNanos = 0L
        var throttleNanos = 0L

        timers.sourceRead.timeNanos { sourceFs.open(sourcePath, BUFFER_SIZE) }.use { input ->
            val startNanos = System.nanoTime()
            try {
                targetFs.create(tempPath, true, BUFFER_SIZE).use { output ->
                    while (true) {
                        val readStartNanos = System.nanoTime()
                        val read = input.read(buffer)
                        readNanos += System.nanoTime() - readStartNanos
                        if (read < 0) break

                        val throttleStartNanos = System.nanoTime()
                        limiter?.acquire(read.toLong())
                        throttleNanos += System.nanoTime() - throttleStartNanos

                        output.write(buffer, 0, read)
                    }
                }
            } finally {
                // On S3A the upload happens in close(), which .use owns, so target time is the
                // window around the write side minus the read and throttle nested inside it.
                val windowNanos = System.nanoTime() - startNanos
                timers.sourceRead.add(readNanos)
                timers.throttleWait.add(throttleNanos)
                timers.targetWrite.add(windowNanos - readNanos - throttleNanos)
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
