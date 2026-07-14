package com.iomete.backup

import com.iomete.backup.config.ConfigLoader
import com.iomete.backup.copy.CopyJobRunner
import com.iomete.backup.copy.PathResolver
import com.iomete.backup.fs.FileLister
import com.iomete.backup.fs.HadoopConfigBuilder
import com.iomete.backup.spark.SparkSessionProvider
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.net.URI

object App {
    private val logger = LoggerFactory.getLogger(App::class.java)

    private const val DEFAULT_CONFIG_PATH = "/etc/configs/application.json"

    @JvmStatic
    fun main(args: Array<String>) {
        logger.info("IOMETE Lakehouse Backup - starting")

        try {
            run(args)
            logger.info("IOMETE Lakehouse Backup - completed successfully")
        } catch (e: Exception) {
            logger.error("IOMETE Lakehouse Backup - failed: {}", e.message, e)
            throw e
        }
    }

    private fun run(args: Array<String>) {
        val configPath = args.getOrNull(0) ?: DEFAULT_CONFIG_PATH
        logger.info("Configuration path: {}", configPath)

        val config = ConfigLoader.load(configPath)
        val spark = SparkSessionProvider.sparkSession

        try {
            logger.info("Enumerating source files...")

            val sourceConf = HadoopConfigBuilder.build(config.source)
            val sourceRoot = PathResolver.resolveRootUri(config.source)

            val files =
                FileSystem.newInstance(URI(sourceRoot), sourceConf).use { sourceFs ->
                    val fileLister = FileLister(sourceFs)
                    fileLister.listRecursively(Path(sourceRoot)).toList()
                }

            logger.info("Found {} files to copy", files.size)
            val totalBytes = files.sumOf { it.size }
            logger.info("Total size: {} bytes ({} MB)", totalBytes, totalBytes / (1024 * 1024))

            if (files.isEmpty()) {
                logger.info("No files found in source. Nothing to copy.")
                return
            }

            val copyJobResult = CopyJobRunner.run(spark, config, files)
            val summary = copyJobResult.summary

            logger.info(
                "Copy summary: {} total, {} succeeded, {} failed, {} bytes copied",
                summary.totalFiles,
                summary.successCount,
                summary.failureCount,
                summary.totalBytesCopied,
            )

            copyJobResult.fileResults
                .filter { !it.success }
                .forEach { result ->
                    logger.warn(
                        "Copy failed: source={} target={} attemptsUsed={} reason={}",
                        result.sourcePath,
                        result.targetPath,
                        result.attemptsUsed,
                        result.error,
                    )
                }

            check(summary.failureCount == 0) {
                "${summary.failureCount} file(s) failed to copy"
            }
        } finally {
            SparkSessionProvider.stop()
        }
    }
}
