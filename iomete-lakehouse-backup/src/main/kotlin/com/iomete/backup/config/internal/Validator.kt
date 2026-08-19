package com.iomete.backup.config.internal

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.CopyConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StatsConfig
import com.iomete.backup.config.StorageConfig
import com.iomete.backup.config.TimestampFolder
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

object Validator {
    private val logger = LoggerFactory.getLogger(Validator::class.java)

    private val DOTTED_IDENTIFIER = Regex("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*")

    fun validate(config: ApplicationConfig): ValidationResult {
        val errors = mutableListOf<String>()

        validateStorageConfig(config.source, "source", errors)
        validateStorageConfig(config.target, "target", errors)

        config.copy.maxBandwidthMbPerSec?.let {
            if (!it.isFinite() || it <= 0) {
                errors.add("copy: maxBandwidthMbPerSec must be a finite number greater than 0 (got $it)")
            }
        }

        validateCopyConfig(config.copy, errors)

        validateStatsConfig(config.stats, errors)

        return result(errors)
    }

    fun validateInternalConfig(sparkConf: SparkConf): ValidationResult {
        val errors = mutableListOf<String>()

        if (SparkRuntime.executorCount(sparkConf) == null) {
            errors.add(
                "spark: ${SparkRuntime.executorSetting(sparkConf)} is not set to a positive value; " +
                    "set it on the job submission",
            )
        }

        return result(errors)
    }

    private fun result(errors: List<String>): ValidationResult =
        if (errors.isEmpty()) {
            logger.debug("Configuration validation passed")
            ValidationResult.Valid
        } else {
            logger.warn("Configuration validation failed with {} error(s)", errors.size)
            errors.forEach { logger.warn("  - {}", it) }
            ValidationResult.Invalid(errors)
        }

    private fun validateCopyConfig(
        copy: CopyConfig,
        errors: MutableList<String>,
    ) {
        if (copy.slotsPerVcpu < 1) {
            errors.add("copy: slotsPerVcpu must be at least 1 (got ${copy.slotsPerVcpu})")
        }

        if (copy.tasksPerSlot < 1) {
            errors.add("copy: tasksPerSlot must be at least 1 (got ${copy.tasksPerSlot})")
        }

        if (copy.perFileOverheadBytes < 0) {
            errors.add("copy: perFileOverheadBytes cannot be negative (got ${copy.perFileOverheadBytes})")
        }

        if (copy.maxBytesPerTask < 1) {
            errors.add("copy: maxBytesPerTask must be at least 1 (got ${copy.maxBytesPerTask})")
        }

        copy.targetTimestampFolder?.let {
            if (it !in TimestampFolder.supported) {
                errors.add(
                    "copy: targetTimestampFolder '$it' is not supported " +
                        "(expected one of ${TimestampFolder.supported.joinToString(", ")})",
                )
            }
        }
    }

    private fun validateStatsConfig(
        stats: StatsConfig,
        errors: MutableList<String>,
    ) {
        if (!DOTTED_IDENTIFIER.matches(stats.database)) {
            errors.add("stats: database '${stats.database}' is not a valid identifier (expected e.g. 'catalog.database' or 'database')")
        }

        if (stats.maxFailureRows < 0) {
            errors.add("stats: maxFailureRows cannot be negative (got ${stats.maxFailureRows})")
        }
    }

    private fun validateStorageConfig(
        storage: StorageConfig,
        location: String,
        errors: MutableList<String>,
    ) {
        if (storage.hadoopOptions.isNotEmpty()) {
            errors.add("$location: 'hadoopOptions' is not a supported configuration option")
        }

        when (storage) {
            is S3Config -> validateS3Config(storage, location, errors)
            is HdfsConfig -> validateHdfsConfig(storage, location, errors)
        }
    }

    private fun validateHdfsConfig(
        config: HdfsConfig,
        location: String,
        errors: MutableList<String>,
    ) {
        if (config.namenode.isBlank()) {
            errors.add("HDFS $location: namenode is required and cannot be empty")
        }

        if (config.user.isBlank()) {
            errors.add("HDFS $location: user is required and cannot be empty")
        }

        if (config.authentication != "simple") {
            errors.add("HDFS $location: authentication '${config.authentication}' is not supported (only 'simple')")
        }
    }

    private fun validateS3Config(
        config: S3Config,
        location: String,
        errors: MutableList<String>,
    ) {
        if (config.bucket.isBlank()) {
            errors.add("S3 $location: bucket is required and cannot be empty")
        }

        if (config.accessKey.isBlank()) {
            errors.add("S3 $location: accessKey is required and cannot be empty")
        }

        if (config.secretKey.isBlank()) {
            errors.add("S3 $location: secretKey is required and cannot be empty")
        }
    }
}

sealed class ValidationResult {
    /**
     * Configuration is valid.
     */
    object Valid : ValidationResult()

    /**
     * Configuration is invalid with a list of error messages.
     */
    data class Invalid(
        val errors: List<String>,
    ) : ValidationResult()
}
