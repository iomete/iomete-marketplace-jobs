package com.iomete.backup.config

object ConfigUtils {

    private const val MASKED_VALUE = "***"

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
        }
    }
}