package com.iomete.backup.copy.internal

import com.iomete.backup.copy.TempFiles
import com.iomete.backup.model.FileEntry
import org.apache.hadoop.security.AccessControlException
import org.junit.jupiter.api.Test
import java.io.FileNotFoundException
import java.io.IOException
import kotlin.test.assertEquals

class CopyPlannerTest {
    private val sourceRoot = "hdfs://namenode:8020/warehouse"
    private val targetRoot = "s3a://backup/out"

    private fun source(
        relativePath: String,
        size: Long = 100,
        modificationTime: Long = 1_000_000,
    ) = FileEntry("$sourceRoot/$relativePath", size, modificationTime)

    private fun target(
        relativePath: String,
        size: Long = 100,
        modificationTime: Long = 2_000_000,
    ) = FileEntry("$targetRoot/$relativePath", size, modificationTime)

    private fun plan(
        sourceFiles: List<FileEntry>,
        targetFiles: List<FileEntry>,
        clockSkewToleranceMs: Long = 0,
        sourceRoot: String = this.sourceRoot,
        targetRoot: String = this.targetRoot,
    ) = planCopy(sourceFiles, sourceRoot, targetFiles, targetRoot, clockSkewToleranceMs)

    @Test
    fun `identical entry is skipped`() {
        val result = plan(listOf(source("db/t/file.parquet")), listOf(target("db/t/file.parquet")))

        assertEquals(emptyList(), result.toCopy)
        assertEquals(listOf(source("db/t/file.parquet")), result.skipped)
    }

    @Test
    fun `differing length is copied`() {
        val result = plan(listOf(source("file.txt", size = 100)), listOf(target("file.txt", size = 101)))

        assertEquals(1, result.toCopy.size)
        assertEquals(emptyList(), result.skipped)
    }

    @Test
    fun `source newer than target is copied`() {
        val result =
            plan(
                listOf(source("file.txt", modificationTime = 2_000_001)),
                listOf(target("file.txt", modificationTime = 2_000_000)),
            )

        assertEquals(1, result.toCopy.size)
    }

    @Test
    fun `clock skew tolerance skips at the edge and copies one millisecond past it`() {
        val atEdge =
            plan(
                listOf(source("file.txt", modificationTime = 1_970_000)),
                listOf(target("file.txt", modificationTime = 2_000_000)),
                clockSkewToleranceMs = 30_000,
            )
        assertEquals(1, atEdge.skipped.size)

        val pastEdge =
            plan(
                listOf(source("file.txt", modificationTime = 1_970_001)),
                listOf(target("file.txt", modificationTime = 2_000_000)),
                clockSkewToleranceMs = 30_000,
            )
        assertEquals(1, pastEdge.toCopy.size)
    }

    @Test
    fun `missing target entry is copied`() {
        val result = plan(listOf(source("file.txt")), listOf(target("other.txt")))

        assertEquals(1, result.toCopy.size)
    }

    @Test
    fun `temp entries are excluded from the target index`() {
        val orphan = target("db/${TempFiles.PREFIX}7-file.parquet")
        val result = plan(listOf(source("db/${TempFiles.PREFIX}7-file.parquet")), listOf(orphan))

        assertEquals(1, result.toCopy.size)
    }

    @Test
    fun `keying is relative, so trailing separators on either root do not matter`() {
        val result =
            plan(
                listOf(source("db/t/file.parquet")),
                listOf(target("db/t/file.parquet")),
                sourceRoot = "$sourceRoot/",
                targetRoot = "$targetRoot/",
            )

        assertEquals(1, result.skipped.size)
    }

    @Test
    fun `an empty target index copies everything`() {
        val result = plan(listOf(source("a.txt"), source("b.txt")), emptyList())

        assertEquals(2, result.toCopy.size)
        assertEquals(emptyList(), result.skipped)
    }

    private fun listing(list: () -> List<FileEntry>) = listTargetWithRetries(retryDelayMs = 0, list = list)

    @Test
    fun `a transient listing failure is retried until it succeeds`() {
        var attempts = 0
        val entries = listOf(target("file.txt"))

        val result =
            listing {
                attempts++
                if (attempts < 3) throw IOException("namenode failing over")
                entries
            }

        assertEquals(entries, result)
        assertEquals(3, attempts)
    }

    @Test
    fun `exhausted listing retries degrade to an empty index`() {
        var attempts = 0

        val result =
            listing {
                attempts++
                throw IOException("still down")
            }

        assertEquals(emptyList(), result)
        assertEquals(RetryPolicy.LISTING_MAX_ATTEMPTS, attempts)
    }

    @Test
    fun `an absent target root is not retried`() {
        var attempts = 0

        val result =
            listing {
                attempts++
                throw FileNotFoundException(targetRoot)
            }

        assertEquals(emptyList(), result)
        assertEquals(1, attempts)
    }

    @Test
    fun `permission denied is not retried`() {
        var attempts = 0

        val result =
            listing {
                attempts++
                throw AccessControlException("denied")
            }

        assertEquals(emptyList(), result)
        assertEquals(1, attempts)
    }
}
