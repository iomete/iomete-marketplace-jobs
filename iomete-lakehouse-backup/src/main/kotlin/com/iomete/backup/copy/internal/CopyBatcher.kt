package com.iomete.backup.copy.internal

import com.iomete.backup.model.FileEntry

internal data class CopyBatches(
    val batches: List<List<String>>,
    val taskWeights: List<Long>,
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

    val batches = mutableListOf<List<String>>()
    val weights = mutableListOf<Long>()
    var current = mutableListOf<String>()
    var currentWeight = 0L

    var remainingWeight = files.sumOf(weight)
    var remainingTasks = minOf(slots.toLong() * tasksPerSlot, files.size.toLong()).coerceAtLeast(1)

    for (file in files.sortedByDescending { it.size }) {
        // Recomputed per task: a frozen target acts as a cap and spills the remainder into extra tasks.
        val target = ((remainingWeight + remainingTasks - 1) / remainingTasks).coerceAtLeast(1)

        // Both limits bind only on a non-empty batch, so a file above the cap gets a batch to itself.
        if (current.isNotEmpty() && (currentWeight + weight(file) > maxBytesPerTask || currentWeight >= target)) {
            batches += current
            weights += currentWeight
            remainingWeight -= currentWeight
            remainingTasks = (remainingTasks - 1).coerceAtLeast(1)
            current = mutableListOf()
            currentWeight = 0
        }
        current += file.path
        currentWeight += weight(file)
    }
    if (current.isNotEmpty()) {
        batches += current
        weights += currentWeight
    }

    return CopyBatches(batches, weights)
}
