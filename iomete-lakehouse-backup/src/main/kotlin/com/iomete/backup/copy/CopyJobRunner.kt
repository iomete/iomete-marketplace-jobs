package com.iomete.backup.copy

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.InternalConfig
import com.iomete.backup.copy.internal.CopyAggregate
import com.iomete.backup.copy.internal.CopyBatches
import com.iomete.backup.copy.internal.CopyTimers
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
import kotlin.time.measureTimedValue

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

        val timers = CopyTimers.register(spark.sparkContext())
        val copier =
            FileCopier(
                sourceConfig = config.source,
                targetConfig = config.target,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
                bytesPerSecPerExecutor = internalConfig.bytesPerSecPerExecutor,
                timers = timers,
            )

        val (targetFiles, targetListingTime) =
            measureTimedValue {
                if (config.copy.skipIdentical) listTarget(config, targetRoot) else emptyList()
            }

        val (planned, planningTime) =
            measureTimedValue {
                val plan =
                    planCopy(
                        sourceFiles = files,
                        sourceRoot = sourceRoot,
                        targetFiles = targetFiles,
                        targetRoot = targetRoot,
                        clockSkewToleranceMs = config.copy.clockSkewToleranceMs,
                    )
                plan to
                    batchFiles(
                        files = plan.toCopy,
                        slots = internalConfig.slots,
                        tasksPerSlot = config.copy.tasksPerSlot,
                        maxBytesPerTask = config.copy.maxBytesPerTask,
                        perFileOverheadBytes = config.copy.perFileOverheadBytes,
                    )
            }
        val (plan, batched) = planned
        val batches = batched.batches
        val skippedBytes = plan.skipped.sumOf { it.size }

        logger.info("Skipping {} files already at the target ({} bytes)", plan.skipped.size, skippedBytes)
        plan.skipped.forEach { logger.debug("Skipped, already at target: {}", it.path) }
        logCopyPlan(plan.toCopy, batched, internalConfig.slots, config.copy.tasksPerSlot)

        val (fileResults, copyTime) =
            measureTimedValue {
                if (batches.isEmpty()) {
                    CopyAggregate(maxSampledFailures = config.stats.maxFailureRows)
                } else {
                    val rdd = jsc.parallelize(batches, batches.size)
                    aggregateCopyResults(
                        rdd.flatMap { batch -> batch.asSequence().map { copier.copySingleFile(it) }.iterator() },
                        config.stats.maxFailureRows,
                    )
                }
            }

        val (directoryResults, dirCreateTime) =
            measureTimedValue { createDirectories(config, sourceRoot, targetRoot, emptyDirectories) }

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
            stats =
                CopyStats(
                    targetListingMs = targetListingTime.inWholeMilliseconds,
                    planningMs = planningTime.inWholeMilliseconds,
                    copyMs = copyTime.inWholeMilliseconds,
                    dirCreateMs = dirCreateTime.inWholeMilliseconds,
                    taskCount = batches.size,
                    maxFilesInTask = batches.maxOfOrNull { it.size } ?: 0,
                    largestFileBytes = plan.toCopy.maxOfOrNull { it.size } ?: 0,
                    filesCopied = fileResults.successCount.toLong(),
                    dirsCreated = directoryResults.count { it.success }.toLong(),
                    retriesUsed = aggregate.retriesUsed,
                    failuresTruncated = aggregate.failuresTruncated,
                    executor = timers.snapshot(),
                ),
        )
    }

    private fun logCopyPlan(
        toCopy: List<FileEntry>,
        batched: CopyBatches,
        slots: Int,
        tasksPerSlot: Int,
    ) {
        val taskCount = batched.batches.size
        val tasksWanted = minOf(slots.toLong() * tasksPerSlot, toCopy.size.toLong())

        logger.info(
            "Copying {} files ({} bytes) across {} tasks weighing {} to {} bytes ({}); largest single file {} bytes",
            toCopy.size,
            toCopy.sumOf { it.size },
            taskCount,
            batched.taskWeights.minOrNull() ?: 0,
            batched.taskWeights.maxOrNull() ?: 0,
            if (taskCount > tasksWanted) "split further by copy.maxBytesPerTask" else "sized by copy.tasksPerSlot",
            toCopy.maxOfOrNull { it.size } ?: 0,
        )

        if (taskCount in 1 until slots) {
            logger.warn(
                "Only {} tasks for {} slots, so {} slots stay idle: {}",
                taskCount,
                slots,
                slots - taskCount,
                if (taskCount == toCopy.size) "there are only ${toCopy.size} files to copy" else "the files pack into fewer tasks",
            )
        }
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
