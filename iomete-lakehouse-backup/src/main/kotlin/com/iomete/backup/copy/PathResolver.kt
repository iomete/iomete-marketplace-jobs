package com.iomete.backup.copy

import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig

object PathResolver {

    fun resolveRootUri(config: StorageConfig): String {
        return when (config) {
            is S3Config -> resolveS3Root(config)
//            is HdfsConfig -> resolveHdfsRoot(config) #TODO
        }
    }

    private fun resolveS3Root(config: S3Config): String {
        val prefix = config.prefix.trim('/')
        return if (prefix.isEmpty()) {
            "s3a://${config.bucket}"
        } else {
            "s3a://${config.bucket}/$prefix"
        }
    }
}