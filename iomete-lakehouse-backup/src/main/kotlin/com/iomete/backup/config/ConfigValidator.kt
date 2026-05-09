package com.iomete.backup.config

import org.slf4j.LoggerFactory

object ConfigValidator {

    private val logger = LoggerFactory.getLogger(ConfigValidator::class.java)

    fun validate(config: ApplicationConfig): ValidationResult {
        val errors = mutableListOf<String>()

        validateStorageConfig(config.source, "source", errors)
        validateStorageConfig(config.target, "target", errors)
        validateCopyConfig(config.copy, errors)

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
        errors: MutableList<String>
    ) {
        when (storage) {
            is S3Config -> validateS3Config(storage, location, errors)
//            is HdfsConfig -> validateHdfsConfig(storage, location, errors) #TODO
        }
    }

    private fun validateS3Config(
        config: S3Config,
        location: String,
        errors: MutableList<String>
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

    private fun validateCopyConfig(
        config: CopyConfig,
        errors: MutableList<String>
    ) {
        validateCopyOptions(config.options, errors)
    }

    private fun validateCopyOptions(
        options: CopyOptions,
        errors: MutableList<String>
    ) {
        if (options.maxMaps <= 0) {
            errors.add("Copy options: maxMaps must be a positive integer (got ${options.maxMaps})")
        }

        if (options.maxAttempts <= 0) {
            errors.add("Copy options: maxAttempts must be a positive integer (got ${options.maxAttempts})")
        }

        if (options.retryDelayMs < 0) {
            errors.add("Copy options: retryDelayMs must be zero or a positive integer (got ${options.retryDelayMs})")
        }

//        options.bandwidthMb?.let { bandwidth ->
//            if (bandwidth <= 0) {
//                errors.add("Copy options: bandwidthMb must be a positive integer if specified (got $bandwidth)")
//            }
//        }

//        if (options.numListStatusThreads <= 0) {
//            errors.add("Copy options: numListStatusThreads must be a positive integer (got ${options.numListStatusThreads})")
//        } #TODO
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
    data class Invalid(val errors: List<String>) : ValidationResult()
}
