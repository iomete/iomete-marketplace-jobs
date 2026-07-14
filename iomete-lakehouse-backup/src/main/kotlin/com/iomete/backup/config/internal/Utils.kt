package com.iomete.backup.config.internal

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig

object Utils {
    private const val MASKED_VALUE = "********"

    fun redactSecrets(config: ApplicationConfig): ApplicationConfig =
        config.copy(
            source = redactStorageConfig(config.source),
            target = redactStorageConfig(config.target),
        )

    private fun redactStorageConfig(storage: StorageConfig): StorageConfig =
        when (storage) {
            is S3Config -> {
                storage.copy(
                    accessKey = MASKED_VALUE,
                    secretKey = MASKED_VALUE,
                )
            }

            is HdfsConfig -> {
                storage
            }
        }
}
