package com.iomete.backup.copy

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.net.URI

class FileCopier(
    private val sourceConfMap: Map<String, String>,
    private val targetConfMap: Map<String, String>,
    private val sourceRoot: String,
    private val targetRoot: String,
    private val maxAttempts: Int = 3,
    private val retryDelayMs: Long = 1000L
    ) : Serializable {

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

    fun copySingleFile(sourceFilePath: String): CopyResult {
        val targetFilePath = PathResolver.resolveTargetPath(sourceFilePath, sourceRoot, targetRoot)
        var lastError: String? = null

        for (attempt in 1..maxAttempts) {
            try {
                val sourceConf = HadoopConfigBuilder.toHadoopConf(sourceConfMap) // TODO: should these be recreated again and again ?
                val targetConf = HadoopConfigBuilder.toHadoopConf(targetConfMap) // TODO: should these be recreated again and again ?
                return FileSystem.newInstance(URI(sourceFilePath), sourceConf).use { sourceFs ->
                    FileSystem.newInstance(URI(targetFilePath), targetConf).use { targetFs -> // TODO: again can the source and destination FS be the same and reused instead of creating per file ?
                        val sourcePath = Path(sourceFilePath)
                        val targetPath = Path(targetFilePath)

                        val parentDir = targetPath.parent
                        if (parentDir != null && !targetFs.exists(parentDir)) {
                            targetFs.mkdirs(parentDir)
                        }

                        val fileStatus = sourceFs.getFileStatus(sourcePath)
                        val fileSize = fileStatus.len

                        FileUtil.copy(
                            sourceFs, sourcePath,
                            targetFs, targetPath,
                            false,
                            true, // TODO: should this not be a config and we handle both ?
                            targetConf
                        )

                        log().debug(
                            "Copied on attempt {}/{}: {} -> {} ({} bytes)",
                            attempt, maxAttempts, sourceFilePath, targetFilePath, fileSize
                        )

                        CopyResult(
                            sourcePath = sourceFilePath,
                            targetPath = targetFilePath,
                            success = true,
                            bytesCopied = fileSize,
                            attemptsUsed = attempt
                        )
                    }
                }
            } catch (e: Exception) {
                lastError = "${e.javaClass.simpleName}: ${e.message}"
                log().warn(
                    "Attempt {}/{} failed for {} -> {}: {}",
                    attempt, maxAttempts, sourceFilePath, targetFilePath, lastError
                )

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
            attemptsUsed = maxAttempts
        )
    }
}
