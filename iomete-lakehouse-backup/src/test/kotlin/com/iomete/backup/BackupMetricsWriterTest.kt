package com.iomete.backup

import com.iomete.backup.copy.CopyResult
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.net.URI
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BackupMetricsWriterTest {
    private lateinit var tempDir: File

    @BeforeEach
    fun setup() {
        tempDir = Files.createTempDirectory("backup-metrics-test").toFile()
    }

    @AfterEach
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `maps copy results to file metrics`() {
        val results = listOf(
            CopyResult(
                sourcePath = "s3a://source/a",
                targetPath = "s3a://target/a",
                success = true,
                bytesCopied = 100,
                attemptsUsed = 1
            ),
            CopyResult(
                sourcePath = "s3a://source/b",
                targetPath = "s3a://target/b",
                success = false,
                error = "RuntimeException: test",
                attemptsUsed = 3
            )
        )

        val mapped = BackupMetricsWriter.toFileMetrics(results)

        assertEquals(2, mapped.size)
        assertEquals(true, mapped[0].success)
        assertEquals(1, mapped[0].attemptsUsed)
        assertEquals(false, mapped[1].success)
        assertEquals("RuntimeException: test", mapped[1].error)
        assertEquals(3, mapped[1].attemptsUsed)
    }

    @Test
    fun `writes metrics json to target metrics directory`() {
        val targetRoot = tempDir.toURI().toString().trimEnd('/')
        val metrics = BackupMetrics(
            status = "partial",
            filesTotal = 2,
            filesCopied = 1,
            filesSkipped = 0,
            filesFailed = 1,
            bytesTotal = 200,
            bytesCopied = 100,
            startTime = "2026-02-10T10:00:00Z",
            endTime = "2026-02-10T10:05:00Z",
            errors = listOf("s3a://source/b: RuntimeException: test"),
            fileResults = listOf(
                FileCopyMetric(
                    sourcePath = "s3a://source/a",
                    targetPath = "s3a://target/a",
                    success = true,
                    bytesCopied = 100,
                    attemptsUsed = 1
                ),
                FileCopyMetric(
                    sourcePath = "s3a://source/b",
                    targetPath = "s3a://target/b",
                    success = false,
                    bytesCopied = 0,
                    attemptsUsed = 3,
                    error = "RuntimeException: test"
                )
            )
        )

        val outputPath = BackupMetricsWriter.write(
            targetRoot = targetRoot,
            targetConfMap = emptyMap(),
            metrics = metrics
        )

        val outputFile = File(URI(outputPath))
        assertTrue(outputFile.exists())
        assertTrue(outputFile.absolutePath.contains("_backup_metrics"))

        val content = outputFile.readText()
        assertTrue(content.contains("\"status\" : \"partial\""))
        assertTrue(content.contains("\"attemptsUsed\" : 3"))
        assertTrue(content.contains("\"error\" : \"RuntimeException: test\""))
    }
}
