package com.iomete.backup.copy

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Tests [FileCopier] using the local filesystem via Hadoop's LocalFileSystem.
 * No mocking needed -- files are actually copied on disk.
 */
class FileCopierTest {

    private lateinit var tempDir: File
    private lateinit var sourceDir: File
    private lateinit var targetDir: File

    @BeforeEach
    fun setup() {
        tempDir = Files.createTempDirectory("file-copier-test").toFile()
        sourceDir = File(tempDir, "source").apply { mkdirs() }
        targetDir = File(tempDir, "target").apply { mkdirs() }
    }

    @AfterEach
    fun tearDown() {
        tempDir.deleteRecursively()
    }

    @Test
    fun `copies a single file successfully`() {
        // Create a source file
        val sourceFile = File(sourceDir, "data/file.txt").apply {
            parentFile.mkdirs()
            writeText("hello world")
        }

        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')
        val sourceFilePath = sourceFile.toURI().toString()

        val copier = FileCopier(
            sourceConfMap = emptyMap(),
            targetConfMap = emptyMap(),
            sourceRoot = sourceRoot,
            targetRoot = targetRoot
        )

        val result = copier.copySingleFile(sourceFilePath)

        assertTrue(result.success)
        assertEquals(sourceFilePath, result.sourcePath)
        assertEquals(11L, result.bytesCopied) // "hello world" = 11 bytes
        assertNull(result.error)

        // Verify the file was actually written to target
        val expectedTargetFile = File(targetDir, "data/file.txt")
        assertTrue(expectedTargetFile.exists())
        assertEquals("hello world", expectedTargetFile.readText())
    }

    @Test
    fun `creates parent directories on target`() {
        val sourceFile = File(sourceDir, "a/b/c/deep.txt").apply {
            parentFile.mkdirs()
            writeText("deep content")
        }

        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier = FileCopier(
            sourceConfMap = emptyMap(),
            targetConfMap = emptyMap(),
            sourceRoot = sourceRoot,
            targetRoot = targetRoot
        )

        val result = copier.copySingleFile(sourceFile.toURI().toString())

        assertTrue(result.success)
        val targetFile = File(targetDir, "a/b/c/deep.txt")
        assertTrue(targetFile.exists())
        assertEquals("deep content", targetFile.readText())
    }

    @Test
    fun `returns failure result for non-existent source file`() {
        val nonExistentPath = File(sourceDir, "does-not-exist.txt").toURI().toString()
        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier = FileCopier(
            sourceConfMap = emptyMap(),
            targetConfMap = emptyMap(),
            sourceRoot = sourceRoot,
            targetRoot = targetRoot
        )

        val result = copier.copySingleFile(nonExistentPath)

        assertFalse(result.success)
        assertEquals(nonExistentPath, result.sourcePath)
        assertEquals(0L, result.bytesCopied)
        assertTrue(result.error!!.contains("FileNotFoundException") || result.error!!.contains("No such file"))
    }

    @Test
    fun `copies multiple files preserving relative structure`() {
        val files = listOf(
            "db/table1/part-0001.parquet" to "content-1",
            "db/table1/part-0002.parquet" to "content-2",
            "db/table2/part-0001.parquet" to "content-3"
        )
        files.forEach { (relativePath, content) ->
            File(sourceDir, relativePath).apply {
                parentFile.mkdirs()
                writeText(content)
            }
        }

        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier = FileCopier(
            sourceConfMap = emptyMap(),
            targetConfMap = emptyMap(),
            sourceRoot = sourceRoot,
            targetRoot = targetRoot
        )

        val results = files.map { (relativePath, _) ->
            val sourceFilePath = File(sourceDir, relativePath).toURI().toString()
            copier.copySingleFile(sourceFilePath)
        }

        assertTrue(results.all { it.success })

        // Verify all files exist at target
        files.forEach { (relativePath, content) ->
            val targetFile = File(targetDir, relativePath)
            assertTrue(targetFile.exists(), "Expected $relativePath to exist at target")
            assertEquals(content, targetFile.readText())
        }
    }

    @Test
    fun `result contains correct target path`() {
        val sourceFile = File(sourceDir, "file.txt").apply {
            writeText("test")
        }

        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier = FileCopier(
            sourceConfMap = emptyMap(),
            targetConfMap = emptyMap(),
            sourceRoot = sourceRoot,
            targetRoot = targetRoot
        )

        val result = copier.copySingleFile(sourceFile.toURI().toString())

        assertTrue(result.success)
        assertTrue(result.targetPath.contains("target/file.txt"))
    }

    @Test
    fun `copier is serializable`() {
        val copier = FileCopier(
            sourceConfMap = mapOf("fs.s3a.access.key" to "key"),
            targetConfMap = mapOf("fs.s3a.access.key" to "key2"),
            sourceRoot = "s3a://source/root",
            targetRoot = "s3a://target/root"
        )

        // Serialize and deserialize
        val baos = java.io.ByteArrayOutputStream()
        java.io.ObjectOutputStream(baos).use { it.writeObject(copier) }
        val bytes = baos.toByteArray()

        val deserialized = java.io.ObjectInputStream(
            java.io.ByteArrayInputStream(bytes)
        ).use { it.readObject() as FileCopier }

        // The deserialized copier should be usable (logger re-created lazily)
        // We can't easily test copy on a deserialized instance without real S3,
        // but at least verify it deserializes without error
        assertTrue(bytes.isNotEmpty())
    }
}
