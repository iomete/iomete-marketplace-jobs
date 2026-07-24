package com.iomete.backup.model

import java.io.Serializable

// Serializable so instances distribute across Spark executors via RDD.
data class FileEntry(
    val path: String,
    val size: Long,
    val modificationTime: Long,
) : Serializable

data class SourceListing(
    val files: List<FileEntry>,
    val emptyDirectories: List<String>,
)
