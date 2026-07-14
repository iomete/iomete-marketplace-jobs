package com.iomete.backup.copy

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.copy.internal.FileCopier
import com.iomete.backup.copy.internal.aggregateCopyResults
import com.iomete.backup.fs.FileEntry
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object CopyJobRunner {
    private val logger = LoggerFactory.getLogger(CopyJobRunner::class.java)

    fun run(
        spark: SparkSession,
        config: ApplicationConfig,
        files: List<FileEntry>,
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
            )

        val filePaths = files.map { it.path }
        // Interim: one partition per executor core (Spark default parallelism).
        // Byte-balanced partitioning will come later.
        val rdd = jsc.parallelize(filePaths, jsc.defaultParallelism())

        logger.info("Copying {} files across {} partitions", files.size, rdd.getNumPartitions())

        val aggregate = aggregateCopyResults(rdd.map { path -> copier.copySingleFile(path) })

        val summary =
            CopyJobSummary(
                totalFiles = aggregate.successCount + aggregate.failureCount,
                successCount = aggregate.successCount,
                failureCount = aggregate.failureCount,
                totalBytesCopied = aggregate.totalBytesCopied,
                errors = aggregate.failures.map { "${it.sourcePath}: ${it.error}" },
            )

        logger.info(
            "Copy job completed: {} succeeded, {} failed, {} bytes copied",
            summary.successCount,
            summary.failureCount,
            summary.totalBytesCopied,
        )

        return CopyJobResult(
            summary = summary,
            failedResults = aggregate.failures,
        )
    }
}
