package com.iomete.backup.config

/**
 * Utility functions for configuration handling.
 */
object ConfigUtils {
    private const val MASKED_VALUE = "***"

    /**
     * Create a redacted copy of the configuration safe for logging.
     * Sensitive fields (accessKey, secretKey, keytabPath) are masked.
     */
    fun redactSecrets(config: ApplicationConfig): ApplicationConfig {
        return config.copy(
            source = redactStorageConfig(config.source),
            target = redactStorageConfig(config.target)
        )
    }

    private fun redactStorageConfig(storage: StorageConfig): StorageConfig {
        return when (storage) {
            is S3Config -> storage.copy(
                accessKey = MASKED_VALUE,
                secretKey = MASKED_VALUE
            )
            is HdfsConfig -> storage.copy(
                auth = redactAuthConfig(storage.auth)
            )
        }
    }

    private fun redactAuthConfig(auth: AuthConfig): AuthConfig {
        return when (auth) {
            is AuthConfig.Simple -> auth
            is AuthConfig.Kerberos -> auth.copy(
                keytabPath = MASKED_VALUE
            )
        }
    }
}
