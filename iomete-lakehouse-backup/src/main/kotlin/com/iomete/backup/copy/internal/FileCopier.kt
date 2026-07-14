package com.iomete.backup.copy.internal

import com.iomete.backup.config.StorageConfig
import com.iomete.backup.copy.CopyResult
import com.iomete.backup.fs.HadoopConfigBuilder
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.AccessControlException
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.io.FileNotFoundException
import java.io.Serializable
import java.net.URI
import java.nio.file.AccessDeniedException

class FileCopier(
    private val sourceConfig: StorageConfig,
    private val targetConfig: StorageConfig,
    private val sourceRoot: String,
    private val targetRoot: String,
    private val maxAttempts: Int = 3,
    private val retryDelayMs: Long = 1000L,
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

        var lastError: String? = null
        var attemptsMade = 0

        for (attempt in 1..maxAttempts) {
            attemptsMade = attempt
            try {
                val bytesCopied = copyOnce(sourceFilePath, targetFilePath)
                log().debug(
                    "Copied on attempt {}/{}: {} -> {} ({} bytes)",
                    attempt,
                    maxAttempts,
                    sourceFilePath,
                    targetFilePath,
                    bytesCopied,
                )
                return CopyResult(
                    sourcePath = sourceFilePath,
                    targetPath = targetFilePath,
                    success = true,
                    bytesCopied = bytesCopied,
                    attemptsUsed = attempt,
                )
            } catch (e: Exception) {
                lastError = "${e.javaClass.simpleName}: ${e.message}"
                log().warn(
                    "Attempt {}/{} failed for {} -> {}: {}",
                    attempt,
                    maxAttempts,
                    sourceFilePath,
                    targetFilePath,
                    lastError,
                )

                if (isTerminal(e)) break
                if (attempt < maxAttempts) {
                    Thread.sleep(retryDelayMs)
                }
            }
        }

        return CopyResult(
            sourcePath = sourceFilePath,
            targetPath = targetFilePath,
            success = false,
            error = lastError ?: "Unknown copy failure",
            attemptsUsed = attemptsMade,
        )
    }

    private fun copyOnce(
        sourceFilePath: String,
        targetFilePath: String,
    ): Long {
        val sourceConf = HadoopConfigBuilder.build(sourceConfig)
        val targetConf = HadoopConfigBuilder.build(targetConfig)

        return FileSystem.newInstance(URI(sourceFilePath), sourceConf).use { sourceFs ->
            FileSystem.newInstance(URI(targetFilePath), targetConf).use { targetFs ->
                val sourcePath = Path(sourceFilePath)
                val targetPath = Path(targetFilePath)

                targetPath.parent?.let { if (!targetFs.exists(it)) targetFs.mkdirs(it) }

                val fileSize = sourceFs.getFileStatus(sourcePath).len
                FileUtil.copy(sourceFs, sourcePath, targetFs, targetPath, false, true, targetConf)
                fileSize
            }
        }
    }

    private fun isTerminal(e: Throwable): Boolean =
        e is FileNotFoundException ||
            e is AccessDeniedException ||
            e is AccessControlException ||
            e is IllegalArgumentException
}
