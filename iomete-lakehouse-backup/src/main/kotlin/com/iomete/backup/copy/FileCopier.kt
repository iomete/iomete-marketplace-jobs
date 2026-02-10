package com.iomete.backup.copy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.net.URI

/**
 * Copies a single file between Hadoop-compatible filesystems.
 *
 * This class is [Serializable] so Spark can ship it to executors.
 * The Hadoop [Configuration] objects are reconstructed from serializable
 * property maps on each executor to avoid shipping non-serializable state.
 *
 * @property sourceConfMap Hadoop config properties for the source filesystem.
 * @property targetConfMap Hadoop config properties for the target filesystem.
 * @property sourceRoot    Root URI of the source storage.
 * @property targetRoot    Root URI of the target storage.
 */
class FileCopier(
    private val sourceConfMap: Map<String, String>,
    private val targetConfMap: Map<String, String>,
    private val sourceRoot: String,
    private val targetRoot: String
) : Serializable {
    companion object {
        private const val MAX_ATTEMPTS = 3
        private const val RETRY_DELAY_MS = 1000L
    }

    @Transient
    private var logger = LoggerFactory.getLogger(FileCopier::class.java)

    /**
     * Lazily get or re-create the logger after deserialization.
     */
    private fun log(): org.slf4j.Logger {
        if (logger == null) {
            logger = LoggerFactory.getLogger(FileCopier::class.java)
        }
        return logger
    }

    /**
     * Copy a single file from source to target.
     *
     * The target path is computed by translating the source file path
     * using [PathResolver.resolveTargetPath]. Parent directories on the
     * target are created automatically.
     *
     * @param sourceFilePath Full URI of the source file.
     * @return [CopyResult] indicating success or failure.
     */
    fun copySingleFile(sourceFilePath: String): CopyResult {
        val targetFilePath = PathResolver.resolveTargetPath(sourceFilePath, sourceRoot, targetRoot)
        var lastError: String? = null

        for (attempt in 1..MAX_ATTEMPTS) {
            try {
                val sourceConf = HadoopConfigBuilder.toHadoopConf(sourceConfMap)
                val targetConf = HadoopConfigBuilder.toHadoopConf(targetConfMap)

                val sourcePath = Path(sourceFilePath)
                val targetPath = Path(targetFilePath)

                val sourceFs = FileSystem.get(URI(sourceFilePath), sourceConf)
                val targetFs = FileSystem.get(URI(targetFilePath), targetConf)

                // Ensure parent directory exists on target
                val parentDir = targetPath.parent
                if (parentDir != null && !targetFs.exists(parentDir)) {
                    targetFs.mkdirs(parentDir)
                }

                // Get source file size before copy
                val fileStatus = sourceFs.getFileStatus(sourcePath)
                val fileSize = fileStatus.len

                // Copy the file
                FileUtil.copy(
                    sourceFs, sourcePath,
                    targetFs, targetPath,
                    false,  // deleteSource
                    true,   // overwrite
                    targetConf
                )

                log().debug(
                    "Copied on attempt {}/{}: {} -> {} ({} bytes)",
                    attempt, MAX_ATTEMPTS, sourceFilePath, targetFilePath, fileSize
                )

                return CopyResult(
                    sourcePath = sourceFilePath,
                    targetPath = targetFilePath,
                    success = true,
                    bytesCopied = fileSize,
                    attemptsUsed = attempt
                )
            } catch (e: Exception) {
                lastError = "${e.javaClass.simpleName}: ${e.message}"
                log().warn(
                    "Attempt {}/{} failed for {} -> {}: {}",
                    attempt, MAX_ATTEMPTS, sourceFilePath, targetFilePath, lastError
                )

                if (attempt < MAX_ATTEMPTS) {
                    Thread.sleep(RETRY_DELAY_MS)
                }
            }
        }

        return CopyResult(
            sourcePath = sourceFilePath,
            targetPath = targetFilePath,
            success = false,
            error = lastError ?: "Unknown copy failure",
            attemptsUsed = MAX_ATTEMPTS
        )
    }
}
