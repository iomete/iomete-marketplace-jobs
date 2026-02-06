package com.iomete.backup.fs

/**
 * Represents a single file entry from a recursive directory listing.
 *
 * @property path Full URI string (e.g. "s3a://bucket/dir/file.parquet" or "hdfs://nn:8020/data/file.parquet")
 * @property size File size in bytes
 * @property modificationTime Last modification timestamp in epoch milliseconds
 */
data class FileEntry(
    val path: String,
    val size: Long,
    val modificationTime: Long
)
