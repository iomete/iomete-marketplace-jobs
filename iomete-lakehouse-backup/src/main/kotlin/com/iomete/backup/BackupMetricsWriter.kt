package com.iomete.backup

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.iomete.backup.copy.CopyResult
import com.iomete.backup.copy.HadoopConfigBuilder
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI
import java.nio.charset.StandardCharsets
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

/**
 * File-level metric record for copy output.
 */
data class FileCopyMetric(
    val sourcePath: String,
    val targetPath: String,
    val success: Boolean,
    val bytesCopied: Long,
    val attemptsUsed: Int,
    val error: String? = null
)

/**
 * Metrics payload written at the end of a backup run.
 */
data class BackupMetrics(
    val status: String,
    val filesTotal: Int,
    val filesCopied: Int,
    val filesSkipped: Int,
    val filesFailed: Int,
    val bytesTotal: Long,
    val bytesCopied: Long,
    val startTime: String,
    val endTime: String,
    val errors: List<String>,
    val fileResults: List<FileCopyMetric>
)

/**
 * Persists backup metrics JSON to target storage.
 */
object BackupMetricsWriter {
    private val mapper = jacksonObjectMapper()

    private val fileTimeFormatter: DateTimeFormatter = DateTimeFormatter
        .ofPattern("yyyyMMdd-HHmmss")
        .withZone(ZoneOffset.UTC)

    fun toFileMetrics(results: List<CopyResult>): List<FileCopyMetric> {
        return results.map { result ->
            FileCopyMetric(
                sourcePath = result.sourcePath,
                targetPath = result.targetPath,
                success = result.success,
                bytesCopied = result.bytesCopied,
                attemptsUsed = result.attemptsUsed,
                error = result.error
            )
        }
    }

    fun write(targetRoot: String, targetConfMap: Map<String, String>, metrics: BackupMetrics): String {
        val conf = HadoopConfigBuilder.toHadoopConf(targetConfMap)
        val fs = FileSystem.get(URI(targetRoot), conf)

        val metricsDir = Path("${targetRoot.trimEnd('/')}/_backup_metrics")
        if (!fs.exists(metricsDir)) {
            fs.mkdirs(metricsDir)
        }

        val timestamp = fileTimeFormatter.format(Instant.now())
        val outputPath = Path(metricsDir, "metrics-$timestamp.json")
        val json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(metrics)

        fs.create(outputPath, true).use { output ->
            output.write(json.toByteArray(StandardCharsets.UTF_8))
        }

        return outputPath.toString()
    }
}
