package com.iomete.backup.config

import org.slf4j.LoggerFactory

/**
 * Result of configuration validation.
 */
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

/**
 * Validator for application configuration.
 * Validates all required fields and business rules.
 */
object ConfigValidator {
    private val logger = LoggerFactory.getLogger(ConfigValidator::class.java)

    /**
     * Validate the application configuration.
     *
     * @param config The configuration to validate
     * @return ValidationResult.Valid if valid, ValidationResult.Invalid with errors otherwise
     */
    fun validate(config: ApplicationConfig): ValidationResult {
        val errors = mutableListOf<String>()

        // Validate source
        validateStorageConfig(config.source, "source", errors)

        // Validate target
        validateStorageConfig(config.target, "target", errors)

        // Validate copy config
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

    /**
     * Validate a storage configuration (source or target).
     */
    private fun validateStorageConfig(
        storage: StorageConfig,
        location: String,
        errors: MutableList<String>
    ) {
        when (storage) {
            is S3Config -> validateS3Config(storage, location, errors)
            is HdfsConfig -> validateHdfsConfig(storage, location, errors)
        }
    }

    /**
     * Validate S3 configuration.
     */
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

    /**
     * Validate HDFS configuration.
     */
    private fun validateHdfsConfig(
        config: HdfsConfig,
        location: String,
        errors: MutableList<String>
    ) {
        if (config.path.isBlank()) {
            errors.add("HDFS $location: path is required and cannot be empty")
        }

        // Either namenode or HA must be specified, but not both
        when {
            config.namenode == null && config.ha == null -> {
                errors.add("HDFS $location: either namenode or ha configuration is required")
            }
            config.namenode != null && config.ha != null -> {
                errors.add("HDFS $location: cannot specify both namenode and ha configuration")
            }
        }

        // Validate HA config if present
        config.ha?.let { ha ->
            validateHaConfig(ha, location, errors)
        }

        // Validate auth config
        validateAuthConfig(config.auth, location, errors)
    }

    /**
     * Validate HDFS HA configuration.
     */
    private fun validateHaConfig(
        config: HaConfig,
        location: String,
        errors: MutableList<String>
    ) {
        if (config.nameservice.isBlank()) {
            errors.add("HDFS $location HA: nameservice is required and cannot be empty")
        }

        if (config.namenodes.isEmpty()) {
            errors.add("HDFS $location HA: namenodes list cannot be empty")
        }

        // Validate that all namenodes have corresponding rpcAddresses
        val missingAddresses = config.namenodes.filter { nn -> 
            !config.rpcAddresses.containsKey(nn) 
        }
        if (missingAddresses.isNotEmpty()) {
            errors.add("HDFS $location HA: missing rpcAddresses for namenodes: ${missingAddresses.joinToString(", ")}")
        }
    }

    /**
     * Validate authentication configuration.
     */
    private fun validateAuthConfig(
        config: AuthConfig,
        location: String,
        errors: MutableList<String>
    ) {
        when (config) {
            is AuthConfig.Simple -> {
                // Simple auth is always valid (user has a default)
            }
            is AuthConfig.Kerberos -> {
                if (config.principal.isBlank()) {
                    errors.add("HDFS $location Kerberos: principal is required and cannot be empty")
                }
                if (config.keytabPath.isBlank()) {
                    errors.add("HDFS $location Kerberos: keytabPath is required and cannot be empty")
                }
            }
        }
    }

    /**
     * Validate copy configuration.
     */
    private fun validateCopyConfig(
        config: CopyConfig,
        errors: MutableList<String>
    ) {
        validateCopyOptions(config.options, errors)
    }

    /**
     * Validate copy options.
     */
    private fun validateCopyOptions(
        options: CopyOptions,
        errors: MutableList<String>
    ) {
        if (options.maxMaps <= 0) {
            errors.add("Copy options: maxMaps must be a positive integer (got ${options.maxMaps})")
        }

        options.bandwidthMb?.let { bandwidth ->
            if (bandwidth <= 0) {
                errors.add("Copy options: bandwidthMb must be a positive integer if specified (got $bandwidth)")
            }
        }

        if (options.numListStatusThreads <= 0) {
            errors.add("Copy options: numListStatusThreads must be a positive integer (got ${options.numListStatusThreads})")
        }
    }
}
