package com.iomete.backup.config.internal

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig
import org.slf4j.LoggerFactory

object Validator {
    private val logger = LoggerFactory.getLogger(Validator::class.java)

    fun validate(config: ApplicationConfig): ValidationResult {
        val errors = mutableListOf<String>()

        validateStorageConfig(config.source, "source", errors)
        validateStorageConfig(config.target, "target", errors)

        return if (errors.isEmpty()) {
            logger.debug("Configuration validation passed")
            ValidationResult.Valid
        } else {
            logger.warn("Configuration validation failed with {} error(s)", errors.size)
            errors.forEach { logger.warn("  - {}", it) }
            ValidationResult.Invalid(errors)
        }
    }

    private fun validateStorageConfig(
        storage: StorageConfig,
        location: String,
        errors: MutableList<String>,
    ) {
        when (storage) {
            is S3Config -> validateS3Config(storage, location, errors)
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
