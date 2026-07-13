package com.iomete.backup.copy

import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig

object PathResolver {
    fun resolveRootUri(config: StorageConfig): String =
        when (config) {
            is S3Config -> resolveS3Root(config)
//            is HdfsConfig -> resolveHdfsRoot(config) #TODO
        }

    private fun resolveS3Root(config: S3Config): String {
        val prefix = config.prefix.trim('/')
        return if (prefix.isEmpty()) {
            "s3a://${config.bucket}"
        } else {
            "s3a://${config.bucket}/$prefix"
        }
    }

    fun resolveTargetPath(
        sourceFilePath: String,
        sourceRoot: String,
        targetRoot: String,
    ): String {
        // Normalize: ensure sourceRoot ends without slash for clean stripping
        val normalizedSourceRoot = sourceRoot.trimEnd('/')
        val normalizedTargetRoot = targetRoot.trimEnd('/')
        val normalizedFilePath = sourceFilePath.trimEnd('/')

        require(normalizedFilePath.startsWith(normalizedSourceRoot)) {
            "Source file path '$sourceFilePath' is not under source root '$sourceRoot'"
        }

        // Strip source root to get relative path
        val relativePath = normalizedFilePath.removePrefix(normalizedSourceRoot)

        // relativePath starts with "/" or is empty
        return if (relativePath.isEmpty()) {
            normalizedTargetRoot
        } else {
            "$normalizedTargetRoot$relativePath"
        }
    }
}
