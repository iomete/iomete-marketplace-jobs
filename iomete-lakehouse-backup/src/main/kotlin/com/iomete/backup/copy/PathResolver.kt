package com.iomete.backup.copy

import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig

/**
 * Resolves file paths between source and target storage locations.
 *
 * Handles the translation of a full source file URI to the corresponding
 * target URI by stripping the source root prefix and appending the
 * relative path to the target root.
 */
object PathResolver {

    /**
     * Compute the root URI for a storage configuration.
     *
     * - S3: `s3a://<bucket>/<prefix>`
     * - HDFS with namenode: `<namenode><path>` (e.g. `hdfs://nn:8020/data/warehouse`)
     * - HDFS with HA: `hdfs://<nameservice><path>`
     *
     * The returned URI always ends without a trailing slash for consistent joining.
     */
    fun resolveRootUri(config: StorageConfig): String {
        return when (config) {
            is S3Config -> resolveS3Root(config)
            is HdfsConfig -> resolveHdfsRoot(config)
        }
    }

    /**
     * Translate a source file path to the corresponding target file path.
     *
     * Given:
     * - sourceFilePath: `s3a://src-bucket/data/warehouse/db/table/file.parquet`
     * - sourceRoot:     `s3a://src-bucket/data/warehouse`
     * - targetRoot:     `s3a://bkp-bucket/backups/warehouse`
     *
     * Returns: `s3a://bkp-bucket/backups/warehouse/db/table/file.parquet`
     *
     * @param sourceFilePath Full URI of the source file.
     * @param sourceRoot     Root URI of the source (as returned by [resolveRootUri]).
     * @param targetRoot     Root URI of the target (as returned by [resolveRootUri]).
     * @return The full target URI for the file.
     */
    fun resolveTargetPath(sourceFilePath: String, sourceRoot: String, targetRoot: String): String {
        // Normalize: ensure sourceRoot ends without slash for clean stripping
        val normalizedSourceRoot = sourceRoot.trimEnd('/')
        val normalizedTargetRoot = targetRoot.trimEnd('/')
        val normalizedFilePath = sourceFilePath.trimEnd('/')

        // Strip source root to get relative path
        val relativePath = if (normalizedFilePath.startsWith(normalizedSourceRoot)) {
            normalizedFilePath.removePrefix(normalizedSourceRoot)
        } else {
            // Fallback: use the file name portion after the last /
            "/${normalizedFilePath.substringAfterLast('/')}"
        }

        // relativePath starts with "/" or is empty
        return if (relativePath.isEmpty()) {
            normalizedTargetRoot
        } else {
            "$normalizedTargetRoot$relativePath"
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

    private fun resolveHdfsRoot(config: HdfsConfig): String {
        val path = config.path.trimEnd('/')

        return when {
            config.ha != null -> "hdfs://${config.ha.nameservice}$path"
            config.namenode != null -> {
                val nn = config.namenode.trimEnd('/')
                "$nn$path"
            }
            else -> path // Should not happen after validation
        }
    }
}
