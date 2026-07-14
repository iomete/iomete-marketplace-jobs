package com.iomete.backup.copy.internal

import com.iomete.backup.config.S3Config
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.runs
import io.mockk.unmockkStatic
import io.mockk.verify
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileUtil
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.net.URI
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FileCopierTest {
    private val dummyConfig = S3Config(bucket = "bucket", accessKey = "key", secretKey = "secret")

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
        val sourceFile =
            File(sourceDir, "data/file.txt").apply {
                parentFile.mkdirs()
                writeText("hello world")
            }

        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')
        val sourceFilePath = sourceFile.toURI().toString()

        val copier =
            FileCopier(
                sourceConfig = dummyConfig,
                targetConfig = dummyConfig,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
            )

        val result = copier.copySingleFile(sourceFilePath)

        assertTrue(result.success)
        assertEquals(sourceFilePath, result.sourcePath)
        assertEquals(11L, result.bytesCopied) // "hello world" = 11 bytes
        assertNull(result.error)
        assertEquals(1, result.attemptsUsed)

        // Verify the file was actually written to target
        val expectedTargetFile = File(targetDir, "data/file.txt")
        assertTrue(expectedTargetFile.exists())
        assertEquals("hello world", expectedTargetFile.readText())
    }

    @Test
    fun `creates parent directories on target`() {
        val sourceFile =
            File(sourceDir, "a/b/c/deep.txt").apply {
                parentFile.mkdirs()
                writeText("deep content")
            }

        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier =
            FileCopier(
                sourceConfig = dummyConfig,
                targetConfig = dummyConfig,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
            )

        val result = copier.copySingleFile(sourceFile.toURI().toString())

        assertTrue(result.success)
        assertEquals(1, result.attemptsUsed)
        val targetFile = File(targetDir, "a/b/c/deep.txt")
        assertTrue(targetFile.exists())
        assertEquals("deep content", targetFile.readText())
    }

    @Test
    fun `terminal error (missing source) fails fast without retry`() {
        val nonExistentPath = File(sourceDir, "does-not-exist.txt").toURI().toString()
        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier =
            FileCopier(
                sourceConfig = dummyConfig,
                targetConfig = dummyConfig,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
            )

        val result = copier.copySingleFile(nonExistentPath)

        assertFalse(result.success)
        assertEquals(nonExistentPath, result.sourcePath)
        assertEquals(0L, result.bytesCopied)
        assertTrue(result.error!!.contains("FileNotFoundException") || result.error!!.contains("No such file"))
        assertEquals(1, result.attemptsUsed)
    }

    @Test
    fun `records path outside source root as a failure without throwing`() {
        val copier =
            FileCopier(
                sourceConfig = dummyConfig,
                targetConfig = dummyConfig,
                sourceRoot = "s3a://bucket/root",
                targetRoot = "s3a://backup/dest",
            )

        val result = copier.copySingleFile("s3a://other-bucket/somewhere/file.csv")

        assertFalse(result.success)
        assertEquals("s3a://other-bucket/somewhere/file.csv", result.sourcePath)
        assertTrue(result.error!!.contains("IllegalArgumentException"))
        assertEquals(0, result.attemptsUsed)
    }

    @Test
    fun `retries transient errors up to maxAttempts`() {
        val sourcePathString = "s3a://bucket/in/file.txt"
        val sourcePath = Path(sourcePathString)
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI("s3a://bucket/out/file.txt"), any<Configuration>()) } returns targetFs
            every { targetFs.exists(any()) } returns true
            every { sourceFs.getFileStatus(sourcePath) } throws java.io.IOException("transient boom")
            every { sourceFs.close() } just runs
            every { targetFs.close() } just runs

            val copier =
                FileCopier(
                    sourceConfig = dummyConfig,
                    targetConfig = dummyConfig,
                    sourceRoot = "s3a://bucket/in",
                    targetRoot = "s3a://bucket/out",
                    maxAttempts = 3,
                    retryDelayMs = 0,
                )

            val result = copier.copySingleFile(sourcePathString)

            assertFalse(result.success)
            assertEquals(3, result.attemptsUsed)
            verify(exactly = 3) { sourceFs.getFileStatus(sourcePath) }
        } finally {
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `uses configured max attempts for failed copy`() {
        val nonExistentPath = File(sourceDir, "does-not-exist.txt").toURI().toString()
        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier =
            FileCopier(
                sourceConfig = dummyConfig,
                targetConfig = dummyConfig,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
                maxAttempts = 1,
                retryDelayMs = 0,
            )

        val result = copier.copySingleFile(nonExistentPath)

        assertFalse(result.success)
        assertEquals(1, result.attemptsUsed)
    }

    @Test
    fun `interrupt during retry backoff stops retrying and preserves interrupt flag`() {
        val sourcePathString = "s3a://bucket/in/file.txt"
        val sourcePath = Path(sourcePathString)
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI("s3a://bucket/out/file.txt"), any<Configuration>()) } returns targetFs
            every { targetFs.exists(any()) } returns true
            every { sourceFs.getFileStatus(sourcePath) } throws java.io.IOException("transient boom")
            every { sourceFs.close() } just runs
            every { targetFs.close() } just runs

            val copier =
                FileCopier(
                    sourceConfig = dummyConfig,
                    targetConfig = dummyConfig,
                    sourceRoot = "s3a://bucket/in",
                    targetRoot = "s3a://bucket/out",
                    maxAttempts = 3,
                    retryDelayMs = 10_000,
                )

            Thread.currentThread().interrupt()
            val result = copier.copySingleFile(sourcePathString)

            assertTrue(Thread.interrupted(), "interrupt flag should be restored")
            assertFalse(result.success)
            assertEquals(1, result.attemptsUsed)
        } finally {
            Thread.interrupted()
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `copies multiple files preserving relative structure`() {
        val files =
            listOf(
                "db/table1/part-0001.parquet" to "content-1",
                "db/table1/part-0002.parquet" to "content-2",
                "db/table2/part-0001.parquet" to "content-3",
            )
        files.forEach { (relativePath, content) ->
            File(sourceDir, relativePath).apply {
                parentFile.mkdirs()
                writeText(content)
            }
        }

        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier =
            FileCopier(
                sourceConfig = dummyConfig,
                targetConfig = dummyConfig,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
            )

        val results =
            files.map { (relativePath, _) ->
                val sourceFilePath = File(sourceDir, relativePath).toURI().toString()
                copier.copySingleFile(sourceFilePath)
            }

        assertTrue(results.all { it.success })
        assertTrue(results.all { it.attemptsUsed == 1 })

        // Verify all files exist at target
        files.forEach { (relativePath, content) ->
            val targetFile = File(targetDir, relativePath)
            assertTrue(targetFile.exists(), "Expected $relativePath to exist at target")
            assertEquals(content, targetFile.readText())
        }
    }

    @Test
    fun `result contains correct target path`() {
        val sourceFile =
            File(sourceDir, "file.txt").apply {
                writeText("test")
            }

        val sourceRoot = sourceDir.toURI().toString().trimEnd('/')
        val targetRoot = targetDir.toURI().toString().trimEnd('/')

        val copier =
            FileCopier(
                sourceConfig = dummyConfig,
                targetConfig = dummyConfig,
                sourceRoot = sourceRoot,
                targetRoot = targetRoot,
            )

        val result = copier.copySingleFile(sourceFile.toURI().toString())

        assertTrue(result.success)
        assertEquals(1, result.attemptsUsed)
        assertTrue(result.targetPath.contains("target/file.txt"))
    }

    @Test
    fun `copier is serializable`() {
        val copier =
            FileCopier(
                sourceConfig = dummyConfig,
                targetConfig = dummyConfig,
                sourceRoot = "s3a://source/root",
                targetRoot = "s3a://target/root",
            )

        // Serialize and deserialize
        val baos = ByteArrayOutputStream()
        ObjectOutputStream(baos).use { it.writeObject(copier) }
        val bytes = baos.toByteArray()

        val deserialized =
            ObjectInputStream(
                ByteArrayInputStream(bytes),
            ).use { it.readObject() as FileCopier }

        // The deserialized copier should be usable (logger re-created lazily)
        // We can't easily test copy on a deserialized instance without real S3,
        // but at least verify it deserializes without error
        assertTrue(bytes.isNotEmpty())
    }

    @Test
    fun `uses isolated filesystem instances for same bucket source and target`() {
        val sourcePathString = "s3a://shared-bucket/warehouse/in/file.txt"
        val targetPathString = "s3a://shared-bucket/warehouse/out/file.txt"
        val sourcePath = Path(sourcePathString)
        val targetPath = Path(targetPathString)
        val targetParent = Path("s3a://shared-bucket/warehouse/out")
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)
        mockkStatic(FileUtil::class)

        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) } returns targetFs
            every { targetFs.exists(targetParent) } returns true
            every { sourceFs.getFileStatus(sourcePath) } returns FileStatus(19L, false, 1, 1024L, 0L, sourcePath)
            every { FileUtil.copy(sourceFs, sourcePath, targetFs, targetPath, false, true, any()) } returns true
            every { sourceFs.close() } just runs
            every { targetFs.close() } just runs

            val copier =
                FileCopier(
                    sourceConfig =
                        S3Config(
                            bucket = "shared-bucket",
                            accessKey = "source-key",
                            secretKey = "source-secret",
                        ),
                    targetConfig =
                        S3Config(
                            bucket = "shared-bucket",
                            accessKey = "target-key",
                            secretKey = "target-secret",
                        ),
                    sourceRoot = "s3a://shared-bucket/warehouse/in",
                    targetRoot = "s3a://shared-bucket/warehouse/out",
                )

            val result = copier.copySingleFile(sourcePathString)

            assertTrue(result.success)
            assertEquals(targetPathString, result.targetPath)
            assertEquals(19L, result.bytesCopied)
            verify(exactly = 1) { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) }
            verify(exactly = 1) { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) }
            verify(exactly = 0) { FileSystem.get(any<URI>(), any<Configuration>()) }
            verify(exactly = 1) { targetFs.close() }
            verify(exactly = 1) { sourceFs.close() }
        } finally {
            unmockkStatic(FileUtil::class)
            unmockkStatic(FileSystem::class)
        }
    }
}
