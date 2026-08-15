package com.iomete.backup.copy.internal

import com.iomete.backup.model.FileEntry
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CopyBatcherTest {
    private fun file(
        name: String,
        size: Long,
    ) = FileEntry("hdfs://namenode:8020/warehouse/$name", size, 1_000_000)

    private fun batch(
        files: List<FileEntry>,
        slots: Int = 1,
        tasksPerSlot: Int = 1,
        maxBytesPerTask: Long = Long.MAX_VALUE,
        perFileOverheadBytes: Long = 0,
    ) = batchFiles(files, slots, tasksPerSlot, maxBytesPerTask, perFileOverheadBytes)
        .batches
        .map { batch -> batch.map { it.substringAfterLast('/') } }

    @Test
    fun `the task count follows the slots the run has`() {
        val files = List(60) { file("f$it", 100) }

        assertEquals(6, batch(files, slots = 3, tasksPerSlot = 2).size)
        assertEquals(12, batch(files, slots = 3, tasksPerSlot = 4).size)
    }

    @Test
    fun `a total that does not divide evenly stays within one task of the target`() {
        val result = batch(List(10) { file("f$it", 10) }, slots = 1, tasksPerSlot = 3)

        assertTrue(result.size in 3..4, "whole files cannot always fill a target exactly, got ${result.size} tasks")
    }

    @Test
    fun `the size cap splits the work further than the slots ask for`() {
        val files = List(60) { file("f$it", 100) }

        val result = batch(files, slots = 1, tasksPerSlot = 2, maxBytesPerTask = 1000)

        assertEquals(6, result.size)
        assertTrue(result.all { it.size == 10 })
    }

    @Test
    fun `there is never a task without a file to copy`() {
        val result = batch(listOf(file("a", 10), file("b", 10)), slots = 8, tasksPerSlot = 4)

        assertEquals(listOf(listOf("a"), listOf("b")), result)
    }

    @Test
    fun `a file heavier than the cap gets a task to itself`() {
        val result = batch(listOf(file("huge", 5000), file("small", 10)), maxBytesPerTask = 1000)

        assertEquals(listOf(listOf("huge"), listOf("small")), result)
    }

    @Test
    fun `small files are charged their fixed cost, so they do not pile into one task`() {
        val files = listOf(file("huge", 1000)) + List(20) { file("s$it", 1) }

        val result = batch(files, slots = 1, tasksPerSlot = 3, perFileOverheadBytes = 100)

        // Total weight 3120 over 3 tasks: the large file pays 1100, so the small files fill 2 tasks.
        assertEquals(3, result.size)
        assertEquals(listOf("huge"), result.first())
        assertTrue(result.drop(1).all { it.size <= 10 }, "no task may swallow every small file")
    }

    @Test
    fun `batches are emitted largest first`() {
        val result = batch(listOf(file("small", 100), file("huge", 900), file("mid", 400)), maxBytesPerTask = 500)

        assertEquals(listOf(listOf("huge"), listOf("mid", "small")), result)
    }

    @Test
    fun `empty input yields no batches`() {
        assertEquals(emptyList(), batch(emptyList()))
    }
}
