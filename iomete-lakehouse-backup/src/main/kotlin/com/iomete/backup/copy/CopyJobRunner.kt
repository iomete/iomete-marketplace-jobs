package com.iomete.backup.copy

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.InternalConfig
import com.iomete.backup.copy.internal.CopyAggregate
import com.iomete.backup.copy.internal.FileCopier
import com.iomete.backup.copy.internal.PathResolver
import com.iomete.backup.copy.internal.aggregateCopyResults
import com.iomete.backup.copy.internal.batchFiles
import com.iomete.backup.copy.internal.listTargetWithRetries
import com.iomete.backup.copy.internal.planCopy
import com.iomete.backup.fs.useFileLister
import com.iomete.backup.fs.useFileSystem
import com.iomete.backup.model.FileEntry
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.io.IOException

object CopyJobRunner {
    private val logger = LoggerFactory.getLogger(CopyJobRunner::class.java)

    fun run(
        spark: SparkSession,
        config: ApplicationConfig,
        internalConfig: InternalConfig,
        files: List<FileEntry>,
        emptyDirectories: List<String> = emptyList(),
    ): CopyJobResult {
        val jsc = JavaSparkContext(spark.sparkContext())

        // Resolve root URIs
        val sourceRoot = config.source.rootUri
        val targetRoot = config.target.rootUri

        logger.info("Source root: {}", sourceRoot)
        logger.info("Target root: {}", targetRoot)

        val copier =
            FileCopier(
                sourceConfig = config.source,
                targetConfig = config.target,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
                bytesPerSecPerExecutor = internalConfig.bytesPerSecPerExecutor,
            )

        val plan =
            planCopy(
                sourceFiles = files,
                sourceRoot = sourceRoot,
                targetFiles = if (config.copy.skipIdentical) listTarget(config, targetRoot) else emptyList(),
                targetRoot = targetRoot,
                clockSkewToleranceMs = config.copy.clockSkewToleranceMs,
            )
        val skippedBytes = plan.skipped.sumOf { it.size }

        logger.info("Skipping {} files already at the target ({} bytes)", plan.skipped.size, skippedBytes)
        plan.skipped.forEach { logger.debug("Skipped, already at target: {}", it.path) }

        val batches = batchFiles(plan.toCopy, config.copy.bytesPerTask, config.copy.filesPerTask)
        logCopyPlan(plan.toCopy, batches.size, config.copy.bytesPerTask)

        val fileResults =
            if (batches.isEmpty()) {
                CopyAggregate()
            } else {
                val rdd = jsc.parallelize(batches, batches.size)
                aggregateCopyResults(
                    rdd.flatMap { batch -> batch.asSequence().map { copier.copySingleFile(it) }.iterator() },
                )
            }
        val directoryResults = createDirectories(config, sourceRoot, targetRoot, emptyDirectories)

        val aggregate = directoryResults.fold(fileResults) { acc, result -> acc.add(result) }
        val summary =
            CopyJobSummary(
                totalEntries = aggregate.successCount + aggregate.failureCount + plan.skipped.size,
                successCount = aggregate.successCount,
                failureCount = aggregate.failureCount,
                skippedCount = plan.skipped.size,
                totalBytesCopied = aggregate.totalBytesCopied,
                skippedBytes = skippedBytes,
                errors = aggregate.failures.map { "${it.sourcePath}: ${it.error}" },
            )

        logger.info(
            "Copy job completed: {} succeeded, {} failed, {} skipped, {} bytes copied",
            summary.successCount,
            summary.failureCount,
            summary.skippedCount,
            summary.totalBytesCopied,
        )

        return CopyJobResult(
            summary = summary,
            failedResults = aggregate.failures,
        )
    }

    private fun logCopyPlan(
        toCopy: List<FileEntry>,
        batchCount: Int,
        bytesPerTask: Long,
    ) {
        val largest = toCopy.maxOfOrNull { it.size } ?: 0
        val oversized = toCopy.count { it.size > bytesPerTask }

        logger.info(
            "Copying {} files ({} bytes) across {} tasks; largest single file {} bytes, {} files above the {} byte target",
            toCopy.size,
            toCopy.sumOf { it.size },
            batchCount,
            largest,
            oversized,
            bytesPerTask,
        )
    }

    private fun listTarget(
        config: ApplicationConfig,
        targetRoot: String,
    ): List<FileEntry> =
        listTargetWithRetries {
            useFileLister(config.target, targetRoot) { it.listRecursively(Path(targetRoot)).toList() }
        }

    private fun createDirectories(
        config: ApplicationConfig,
        sourceRoot: String,
        targetRoot: String,
        directories: List<String>,
    ): List<CopyResult> {
        if (directories.isEmpty()) return emptyList()

        logger.info("Replicating {} empty directories", directories.size)

        return useFileSystem(config.target, targetRoot) { targetFs ->
            directories.map { sourcePath ->
                val targetPath = PathResolver.resolveTargetPath(sourcePath, sourceRoot, targetRoot)

                try {
                    if (!targetFs.mkdirs(Path(targetPath))) {
                        throw IOException("mkdirs reported failure")
                    }
                    CopyResult(sourcePath = sourcePath, targetPath = targetPath, success = true)
                } catch (e: Exception) {
                    CopyResult(
                        sourcePath = sourcePath,
                        targetPath = targetPath,
                        success = false,
                        error = "${e.javaClass.simpleName}: ${e.message}",
                    )
                }
            }
        }
    }
}
