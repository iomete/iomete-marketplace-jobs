@file:Suppress("ktlint:standard:filename")

package com.iomete.backup.model

import com.iomete.backup.fs.FileEntry

data class SourceListing(
    val files: List<FileEntry>,
    val emptyDirectories: List<String>,
)
