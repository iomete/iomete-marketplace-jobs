package com.iomete.backup.copy.internal

import com.iomete.backup.model.FileEntry

internal data class CopyBatches(
    val batches: List<List<String>>,
    val weightPerTask: Long,
)

// Cost, not bytes: every file pays a fixed price on top of its size, so two tasks of equal weight
// take about the same time whether they hold many small files or few large ones.
// Largest first: Spark launches tasks roughly in partition order, so the longest tasks start first
// and the cheap ones overlap them.
internal fun batchFiles(
    files: List<FileEntry>,
    slots: Int,
    tasksPerSlot: Int,
    maxBytesPerTask: Long,
    perFileOverheadBytes: Long = 0,
): CopyBatches {
    val weight = { file: FileEntry -> file.size + perFileOverheadBytes }
    val tasksWanted = (slots.toLong() * tasksPerSlot).coerceAtLeast(1)
    // Rounded up: a target below the even share leaves every task short and adds tasks nobody asked for.
    val evenWeight = ((files.sumOf(weight) + tasksWanted - 1) / tasksWanted).coerceAtLeast(1)
    val weightPerTask = minOf(evenWeight, maxBytesPerTask)

    val batches = mutableListOf<List<String>>()
    var current = mutableListOf<String>()
    var currentWeight = 0L

    for (file in files.sortedByDescending { it.size }) {
        // The limit binds only on a non-empty batch, so a file above it gets a batch to itself.
        if (current.isNotEmpty() && currentWeight + weight(file) > weightPerTask) {
            batches += current
            current = mutableListOf()
            currentWeight = 0
        }
        current += file.path
        currentWeight += weight(file)
    }
    if (current.isNotEmpty()) batches += current

    return CopyBatches(batches, weightPerTask)
}
