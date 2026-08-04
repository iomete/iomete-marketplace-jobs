package com.iomete.backup.copy.internal

import com.iomete.backup.model.FileEntry
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class CopyBatcherTest {
    private fun file(
        name: String,
        size: Long,
    ) = FileEntry("hdfs://namenode:8020/warehouse/$name", size, 1_000_000)

    private fun batch(
        files: List<FileEntry>,
        bytesPerTask: Long = 1000,
        filesPerTask: Int = 1000,
    ) = batchFiles(files, bytesPerTask, filesPerTask).map { batch -> batch.map { it.substringAfterLast('/') } }

    @Test
    fun `a batch is closed before the byte target is exceeded`() {
        val result = batch(listOf(file("a", 600), file("b", 500)), bytesPerTask = 1000)

        assertEquals(listOf(listOf("a"), listOf("b")), result)
    }

    @Test
    fun `the file limit closes a batch far below the byte target`() {
        val result = batch(List(5) { file("f$it", 1) }, bytesPerTask = 1000, filesPerTask = 2)

        assertEquals(3, result.size)
        assertEquals(listOf(2, 2, 1), result.map { it.size })
    }

    @Test
    fun `a file larger than the byte target gets a batch to itself`() {
        val result = batch(listOf(file("huge", 5000), file("small", 10)), bytesPerTask = 1000)

        assertEquals(listOf(listOf("huge"), listOf("small")), result)
    }

    @Test
    fun `batches are emitted largest first`() {
        val result = batch(listOf(file("small", 100), file("huge", 900), file("mid", 400)), bytesPerTask = 500)

        assertEquals(listOf(listOf("huge"), listOf("mid", "small")), result)
    }

    @Test
    fun `empty input yields no batches`() {
        assertEquals(emptyList(), batch(emptyList()))
    }
}
