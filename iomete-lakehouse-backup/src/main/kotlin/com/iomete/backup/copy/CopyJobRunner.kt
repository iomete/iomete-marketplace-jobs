package com.iomete.backup.copy

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.fs.FileEntry
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Orchestrates the distributed file copy using Spark.
 *
 * Converts the enumerated file list into a Spark RDD, maps each entry
 * through [FileCopier.copySingleFile], collects results, and produces
 * a [CopyJobResult].
 */
object CopyJobRunner {

    private val logger = LoggerFactory.getLogger(CopyJobRunner::class.java)

    /**
     * Execute the distributed copy job.
     *
     * 1. Builds Hadoop config maps for source and target.
     * 2. Resolves source/target root URIs.
     * 3. Parallelizes the file list as an RDD with [maxMaps] partitions.
     * 4. Maps each [FileEntry] through the copy function.
     * 5. Collects [CopyResult] objects and aggregates into [CopyJobSummary].
     *
     * @param spark  The active SparkSession.
     * @param config The application configuration.
     * @param files  List of files enumerated from the source.
     * @return [CopyJobResult] with summary and per-file results.
     */
    fun run(spark: SparkSession, config: ApplicationConfig, files: List<FileEntry>): CopyJobResult {
        val jsc = JavaSparkContext(spark.sparkContext())

        // Build serializable config maps
        val sourceConfMap = HadoopConfigBuilder.buildConfigMap(config.source)
        val targetConfMap = HadoopConfigBuilder.buildConfigMap(config.target)

        // Resolve root URIs
        val sourceRoot = PathResolver.resolveRootUri(config.source)
        val targetRoot = PathResolver.resolveRootUri(config.target)

        logger.info("Source root: {}", sourceRoot)
        logger.info("Target root: {}", targetRoot)

        // Create the copier (serializable, shipped to executors)
        val copier = FileCopier(sourceConfMap, targetConfMap, sourceRoot, targetRoot)

        // Determine parallelism from config
        val numPartitions = config.copy.options.maxMaps

        logger.info("Creating RDD with {} partitions from {} files", numPartitions, files.size)

        // Parallelize file paths as strings (lightweight, serializable)
        val filePaths = files.map { it.path }
        val rdd = jsc.parallelize(filePaths, numPartitions)

        // Execute the distributed copy
        val results: List<CopyResult> = rdd
            .map { path -> copier.copySingleFile(path) }
            .collect()

        // Aggregate results
        val successCount = results.count { it.success }
        val failureCount = results.count { !it.success }
        val totalBytesCopied = results.filter { it.success }.sumOf { it.bytesCopied }
        val errors = results.filter { !it.success }.map { "${it.sourcePath}: ${it.error}" }

        val summary = CopyJobSummary(
            totalFiles = results.size,
            successCount = successCount,
            failureCount = failureCount,
            totalBytesCopied = totalBytesCopied,
            errors = errors
        )

        logger.info("Copy job completed: {} succeeded, {} failed, {} bytes copied",
            summary.successCount, summary.failureCount, summary.totalBytesCopied)

        return CopyJobResult(
            summary = summary,
            fileResults = results
        )
    }
}
