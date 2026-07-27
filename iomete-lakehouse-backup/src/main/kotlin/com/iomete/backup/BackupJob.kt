package com.iomete.backup

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.copy.CopyJobRunner
import com.iomete.backup.fs.FileLister
import com.iomete.backup.fs.FileSystemFactory
import com.iomete.backup.fs.HadoopConfigBuilder
import com.iomete.backup.model.SourceListing
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
        val (files, emptyDirs) = enumerateSource(config)

        logger.info("Found {} files to copy", files.size)
        if (emptyDirs.isNotEmpty()) {
            logger.info("Found {} empty directories to replicate", emptyDirs.size)
        }

        val totalBytes = files.sumOf { it.size }
        logger.info("Total size: {} bytes ({} MB)", totalBytes, totalBytes / (1024 * 1024))

        if (files.isEmpty() && emptyDirs.isEmpty()) {
            logger.info("No files found in source. Nothing to copy.")
            return
        }

        val copyJobResult = CopyJobRunner.run(spark, config, files, emptyDirs)
        val summary = copyJobResult.summary

        logger.info(
            "Copy summary: {} total, {} succeeded, {} failed, {} bytes copied",
            summary.totalEntries,
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
            "${summary.failureCount} entr${if (summary.failureCount == 1) "y" else "ies"} failed to copy"
        }
    }

    private fun enumerateSource(config: ApplicationConfig): SourceListing {
        val sourceConf = HadoopConfigBuilder.build(config.source)
        val sourceRoot = config.source.rootUri

        return FileSystemFactory.create(config.source, URI(sourceRoot), sourceConf).use { sourceFs ->
            val lister = FileLister(sourceFs)
            val files = lister.listRecursively(Path(sourceRoot)).toList()
            val emptyDirectories =
                if (config.source is HdfsConfig) {
                    lister.listLeafEmptyDirectories(Path(sourceRoot)).map { it.toString() }
                } else {
                    emptyList()
                }
            SourceListing(files, emptyDirectories)
        }
    }
}
