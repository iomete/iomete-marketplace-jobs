package com.iomete.backup

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.copy.CopyJobRunner
import com.iomete.backup.fs.FileEntry
import com.iomete.backup.fs.FileLister
import com.iomete.backup.fs.FileSystemFactory
import com.iomete.backup.fs.HadoopConfigBuilder
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.net.URI

object BackupJob {
    private val logger = LoggerFactory.getLogger(BackupJob::class.java)

    fun run(
        spark: SparkSession,
        config: ApplicationConfig,
    ) {
        logger.info("Enumerating source files...")
        val files = enumerateSource(config)

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

        copyJobResult.failedResults
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
    }

    private fun enumerateSource(config: ApplicationConfig): List<FileEntry> {
        val sourceConf = HadoopConfigBuilder.build(config.source)
        val sourceRoot = config.source.rootUri

        return FileSystemFactory.create(config.source, URI(sourceRoot), sourceConf).use { sourceFs ->
            FileLister(sourceFs).listRecursively(Path(sourceRoot)).toList()
        }
    }
}
