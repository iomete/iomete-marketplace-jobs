package com.iomete.backup.copy

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.copy.internal.FileCopier
import com.iomete.backup.copy.internal.PathResolver
import com.iomete.backup.copy.internal.aggregateCopyResults
import com.iomete.backup.fs.FileEntry
import com.iomete.backup.fs.FileSystemFactory
import com.iomete.backup.fs.HadoopConfigBuilder
import org.apache.hadoop.fs.Path
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.io.IOException
import java.net.URI

object CopyJobRunner {
    private val logger = LoggerFactory.getLogger(CopyJobRunner::class.java)

    fun run(
        spark: SparkSession,
        config: ApplicationConfig,
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
            )

        val filePaths = files.map { it.path }
        // Interim: one partition per executor core (Spark default parallelism).
        // Byte-balanced partitioning will come later.
        val rdd = jsc.parallelize(filePaths, jsc.defaultParallelism())

        logger.info("Copying {} files across {} partitions", files.size, rdd.getNumPartitions())

        val fileResults = aggregateCopyResults(rdd.map { path -> copier.copySingleFile(path) })
        val directoryResults = createDirectories(config, sourceRoot, targetRoot, emptyDirectories)

        val aggregate = directoryResults.fold(fileResults) { acc, result -> acc.add(result) }
        val summary =
            CopyJobSummary(
                totalEntries = aggregate.successCount + aggregate.failureCount,
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

    private fun createDirectories(
        config: ApplicationConfig,
        sourceRoot: String,
        targetRoot: String,
        directories: List<String>,
    ): List<CopyResult> {
        if (directories.isEmpty()) return emptyList()

        logger.info("Replicating {} empty directories", directories.size)

        val targetConf = HadoopConfigBuilder.build(config.target)

        return FileSystemFactory.create(config.target, URI(targetRoot), targetConf).use { targetFs ->
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
