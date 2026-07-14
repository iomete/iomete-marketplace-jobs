package com.iomete.backup.copy

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.fs.FileEntry
import com.iomete.backup.fs.HadoopConfigBuilder
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
        val sourceRoot = PathResolver.resolveRootUri(config.source)
        val targetRoot = PathResolver.resolveRootUri(config.target)

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

        // Execute the distributed copy
        val results: List<CopyResult> =
            rdd
                .map { path -> copier.copySingleFile(path) }
                .collect()

        // Aggregate results
        val successCount = results.count { it.success }
        val failureCount = results.count { !it.success }
        val totalBytesCopied = results.filter { it.success }.sumOf { it.bytesCopied }
        val errors = results.filter { !it.success }.map { "${it.sourcePath}: ${it.error}" }

        val summary =
            CopyJobSummary(
                totalFiles = results.size,
                successCount = successCount,
                failureCount = failureCount,
                totalBytesCopied = totalBytesCopied,
                errors = errors,
            )

        logger.info(
            "Copy job completed: {} succeeded, {} failed, {} bytes copied",
            summary.successCount,
            summary.failureCount,
            summary.totalBytesCopied,
        )

        return CopyJobResult(
            summary = summary,
            fileResults = results,
        )
    }
}
