package com.iomete.backup.copy

import com.iomete.backup.config.ApplicationConfig
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

        // Build serializable config maps
        val sourceConfMap = HadoopConfigBuilder.buildConfigMap(config.source)
        val targetConfMap = HadoopConfigBuilder.buildConfigMap(config.target)

        // Resolve root URIs
        val sourceRoot = PathResolver.resolveRootUri(config.source)
        val targetRoot = PathResolver.resolveRootUri(config.target)

        logger.info("Source root: {}", sourceRoot)
        logger.info("Target root: {}", targetRoot)

        val copier =
            FileCopier(
                sourceConfMap = sourceConfMap,
                targetConfMap = targetConfMap,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
            )

        val filePaths = files.map { it.path }
        val rdd = jsc.parallelize(filePaths)

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
