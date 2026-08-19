package com.iomete.backup

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.InternalConfig
import com.iomete.backup.config.TimestampFolder
import com.iomete.backup.copy.CopyJobRunner
import com.iomete.backup.copy.CopyJobSummary
import com.iomete.backup.fs.useFileLister
import com.iomete.backup.model.SourceListing
import com.iomete.backup.stats.RunProgress
import com.iomete.backup.stats.StatsRecorder
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.time.Instant
import kotlin.time.measureTimedValue

object BackupJob {
    private val logger = LoggerFactory.getLogger(BackupJob::class.java)

    fun run(
        spark: SparkSession,
        config: ApplicationConfig,
        internalConfig: InternalConfig,
    ): CopyJobSummary {
        val startedAt = Instant.now()
        val resolvedConfig = resolveTarget(config, startedAt)
        logger.info("Target root: {}", resolvedConfig.target.rootUri)

        val recorder = StatsRecorder(spark, resolvedConfig, internalConfig)
        val progress = RunProgress()

        // Claimed before the source listing, which is the long single-threaded phase.
        recorder.claim(startedAt)

        try {
            val summary = copy(spark, resolvedConfig, internalConfig, progress)
            recorder.finalise(startedAt, progress, null)
            return summary
        } catch (e: Throwable) {
            recorder.finalise(startedAt, progress, e)
            throw e
        }
    }

    private fun resolveTarget(
        config: ApplicationConfig,
        startedAt: Instant,
    ): ApplicationConfig {
        val granularity = config.copy.targetTimestampFolder ?: return config
        return config.copy(target = config.target.withSubFolder(TimestampFolder.folderName(granularity, startedAt)))
    }

    private fun copy(
        spark: SparkSession,
        config: ApplicationConfig,
        internalConfig: InternalConfig,
        progress: RunProgress,
    ): CopyJobSummary {
        logger.info("Enumerating source files...")
        val (listing, listingTime) = measureTimedValue { enumerateSource(config) }
        val (files, emptyDirs) = listing

        val totalBytes = files.sumOf { it.size }
        progress.filesListed = files.size.toLong()
        progress.dirsListed = emptyDirs.size.toLong()
        progress.bytesSource = totalBytes
        progress.sourceListingMs = listingTime.inWholeMilliseconds

        logger.info("Found {} files to copy", files.size)
        if (emptyDirs.isNotEmpty()) {
            logger.info("Found {} empty directories to replicate", emptyDirs.size)
        }

        logger.info("Total size: {} bytes ({} MB)", totalBytes, totalBytes / (1024 * 1024))

        if (files.isEmpty() && emptyDirs.isEmpty()) {
            logger.info("No files found in source. Nothing to copy.")
            progress.summary = CopyJobSummary.EMPTY
            return CopyJobSummary.EMPTY
        }

        val copyJobResult = CopyJobRunner.run(spark, config, internalConfig, files, emptyDirs)
        val summary = copyJobResult.summary
        progress.summary = summary
        progress.copy = copyJobResult.stats
        progress.failures = copyJobResult.failedResults

        logger.info(
            "Copy summary: {} total, {} succeeded, {} failed, {} skipped, {} bytes copied, {} bytes skipped",
            summary.totalEntries,
            summary.successCount,
            summary.failureCount,
            summary.skippedCount,
            summary.totalBytesCopied,
            summary.skippedBytes,
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

        return summary
    }

    private fun enumerateSource(config: ApplicationConfig): SourceListing {
        val sourceRoot = config.source.rootUri

        return useFileLister(config.source, sourceRoot) { lister ->
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
