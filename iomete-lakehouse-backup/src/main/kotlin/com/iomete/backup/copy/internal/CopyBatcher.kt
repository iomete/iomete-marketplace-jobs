package com.iomete.backup.copy.internal

import com.iomete.backup.model.FileEntry

// Largest first: Spark launches tasks roughly in partition order, so the longest tasks start first
// and the cheap ones overlap them.
internal fun batchFiles(
    files: List<FileEntry>,
    bytesPerTask: Long,
    filesPerTask: Int,
): List<List<String>> {
    val batches = mutableListOf<List<String>>()
    var current = mutableListOf<String>()
    var currentBytes = 0L

    for (file in files.sortedByDescending { it.size }) {
        // Limits bind only on a non-empty batch, so a file above the byte target gets a batch to itself.
        if (current.isNotEmpty() && (currentBytes + file.size > bytesPerTask || current.size >= filesPerTask)) {
            batches += current
            current = mutableListOf()
            currentBytes = 0
        }
        current += file.path
        currentBytes += file.size
    }
    if (current.isNotEmpty()) batches += current

    return batches
}
