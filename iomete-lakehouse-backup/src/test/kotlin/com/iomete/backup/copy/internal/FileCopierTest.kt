package com.iomete.backup.copy.internal

import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.copy.TempFiles
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.runs
import io.mockk.slot
import io.mockk.unmockkStatic
import io.mockk.verify
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FSDataOutputStream
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.OutputStream
import java.net.URI
import java.nio.file.Files
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class FileCopierTest {
    private val dummyConfig = S3Config(bucket = "bucket", accessKey = "key", secretKey = "secret")

    private lateinit var tempDir: File
    private lateinit var sourceDir: File
    private lateinit var targetDir: File
    private lateinit var localFs: FileSystem

    @BeforeEach
    fun setup() {
        tempDir = Files.createTempDirectory("file-copier-test").toFile()
        sourceDir = File(tempDir, "source").apply { mkdirs() }
        targetDir = File(tempDir, "target").apply { mkdirs() }
        localFs = FileSystem.getLocal(Configuration())
    }

    /** A real stream over a real file, so a mocked FileSystem can serve the copier's reads. */
    private fun sourceStream(size: Int): FSDataInputStream {
        val file = File(tempDir, "stream-${UUID.randomUUID()}").apply { writeBytes(ByteArray(size)) }
        return localFs.open(Path(file.toURI()))
    }

    private fun sinkStream(): FSDataOutputStream = FSDataOutputStream(ByteArrayOutputStream(), null)

    /** A sink that accepts the first chunk and then fails, so the pump is interrupted with bytes already written. */
    private fun failingSinkStream(onWrite: () -> Int): FSDataOutputStream =
        FSDataOutputStream(
            object : OutputStream() {
                override fun write(b: Int) = Unit

                override fun write(
                    b: ByteArray,
                    off: Int,
                    len: Int,
                ) {
                    if (onWrite() > 0) throw java.io.IOException("mid-write boom")
                }
            },
            null,
        )

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
    fun `recovers on a later attempt after transient failures`() {
        val sourcePathString = "s3a://bucket/in/file.txt"
        val targetPathString = "s3a://bucket/out/file.txt"
        val sourcePath = Path(sourcePathString)
        val targetPath = Path(targetPathString)
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) } returns targetFs
            every { targetFs.exists(any()) } returns true
            every { targetFs.delete(any(), any()) } returns true
            every { targetFs.rename(any(), targetPath) } returns true
            every { targetFs.getFileStatus(any()) } returns FileStatus(42L, false, 1, 1024L, 0L, targetPath)
            every { sourceFs.getFileStatus(sourcePath) } throws
                java.io.IOException("transient 1") andThenThrows
                java.io.IOException("transient 2") andThen
                FileStatus(42L, false, 1, 1024L, 0L, sourcePath)
            every { sourceFs.open(sourcePath, any()) } answers { sourceStream(42) }
            every { targetFs.create(any(), any<Boolean>(), any<Int>()) } answers { sinkStream() }
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

            assertTrue(result.success)
            assertEquals(3, result.attemptsUsed)
            assertEquals(42L, result.bytesCopied)
            verify(exactly = 3) { sourceFs.getFileStatus(sourcePath) }
            verify(exactly = 1) { sourceFs.open(sourcePath, any()) }
            verify(exactly = 1) { targetFs.rename(any(), targetPath) }
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

        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) } returns targetFs
            every { targetFs.exists(targetParent) } returns true
            every { targetFs.exists(targetPath) } returns false
            every { targetFs.rename(any(), targetPath) } returns true
            every { targetFs.getFileStatus(any()) } returns FileStatus(19L, false, 1, 1024L, 0L, targetPath)
            every { sourceFs.getFileStatus(sourcePath) } returns FileStatus(19L, false, 1, 1024L, 0L, sourcePath)
            every { sourceFs.open(sourcePath, any()) } answers { sourceStream(19) }
            every { targetFs.create(any(), any<Boolean>(), any<Int>()) } answers { sinkStream() }
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
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `HDFS target filesystem is built with the configured user`() {
        val sourcePathString = "s3a://source-bucket/in/file.txt"
        val targetPathString = "hdfs://isilon.example.com:8020/out/file.txt"
        val sourcePath = Path(sourcePathString)
        val targetPath = Path(targetPathString)
        val targetParent = Path("hdfs://isilon.example.com:8020/out")
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)

        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every {
                FileSystem.newInstance(URI(targetPathString), any<Configuration>(), "isilon-user")
            } returns targetFs
            every { targetFs.exists(targetParent) } returns true
            every { targetFs.exists(targetPath) } returns false
            every { targetFs.rename(any(), targetPath) } returns true
            every { targetFs.getFileStatus(any()) } returns FileStatus(11L, false, 1, 1024L, 0L, targetPath)
            every { sourceFs.getFileStatus(sourcePath) } returns FileStatus(11L, false, 1, 1024L, 0L, sourcePath)
            every { sourceFs.open(sourcePath, any()) } answers { sourceStream(11) }
            every { targetFs.create(any(), any<Boolean>(), any<Int>()) } answers { sinkStream() }
            every { sourceFs.close() } just runs
            every { targetFs.close() } just runs

            val copier =
                FileCopier(
                    sourceConfig =
                        S3Config(
                            bucket = "source-bucket",
                            accessKey = "key",
                            secretKey = "secret",
                        ),
                    targetConfig =
                        HdfsConfig(
                            namenode = "isilon.example.com:8020",
                            path = "out",
                            user = "isilon-user",
                        ),
                    sourceRoot = "s3a://source-bucket/in",
                    targetRoot = "hdfs://isilon.example.com:8020/out",
                )

            val result = copier.copySingleFile(sourcePathString)

            assertTrue(result.success)
            assertEquals(targetPathString, result.targetPath)
            assertEquals(11L, result.bytesCopied)
            verify(exactly = 1) {
                FileSystem.newInstance(URI(targetPathString), any<Configuration>(), "isilon-user")
            }
            verify(exactly = 0) { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) }
            verify(exactly = 1) { targetFs.close() }
            verify(exactly = 1) { sourceFs.close() }
        } finally {
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `length mismatch deletes temp and fails without renaming`() {
        val sourcePathString = "s3a://bucket/in/file.txt"
        val targetPathString = "s3a://bucket/out/file.txt"
        val sourcePath = Path(sourcePathString)
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()
        val tempSlot = slot<Path>()

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) } returns targetFs
            every { targetFs.exists(any()) } returns true
            every { targetFs.delete(any(), any()) } returns true
            every { sourceFs.getFileStatus(sourcePath) } returns FileStatus(100L, false, 1, 1024L, 0L, sourcePath)
            every { targetFs.getFileStatus(any()) } returns FileStatus(50L, false, 1, 1024L, 0L, Path(targetPathString))
            every { sourceFs.open(sourcePath, any()) } answers { sourceStream(100) }
            every { targetFs.create(capture(tempSlot), any<Boolean>(), any<Int>()) } answers { sinkStream() }
            every { sourceFs.close() } just runs
            every { targetFs.close() } just runs

            val copier =
                FileCopier(
                    sourceConfig = dummyConfig,
                    targetConfig = dummyConfig,
                    sourceRoot = "s3a://bucket/in",
                    targetRoot = "s3a://bucket/out",
                    maxAttempts = 2,
                    retryDelayMs = 0,
                )

            val result = copier.copySingleFile(sourcePathString)

            assertFalse(result.success)
            assertEquals(2, result.attemptsUsed)
            assertTrue(result.error!!.contains("Length verification failed"))
            assertTrue(tempSlot.captured.name.startsWith(TempFiles.PREFIX))
            verify(atLeast = 1) { targetFs.delete(any(), any()) }
            verify(exactly = 0) { targetFs.rename(any(), any()) }
        } finally {
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `rename failure deletes temp and fails`() {
        val sourcePathString = "s3a://bucket/in/file.txt"
        val targetPathString = "s3a://bucket/out/file.txt"
        val sourcePath = Path(sourcePathString)
        val targetPath = Path(targetPathString)
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) } returns targetFs
            every { targetFs.exists(any()) } returns false
            every { targetFs.mkdirs(any()) } returns true
            every { targetFs.delete(any(), any()) } returns true
            every { sourceFs.getFileStatus(sourcePath) } returns FileStatus(11L, false, 1, 1024L, 0L, sourcePath)
            every { targetFs.getFileStatus(any()) } returns FileStatus(11L, false, 1, 1024L, 0L, targetPath)
            every { sourceFs.open(sourcePath, any()) } answers { sourceStream(11) }
            every { targetFs.create(any(), any<Boolean>(), any<Int>()) } answers { sinkStream() }
            every { targetFs.rename(any(), targetPath) } returns false
            every { sourceFs.close() } just runs
            every { targetFs.close() } just runs

            val copier =
                FileCopier(
                    sourceConfig = dummyConfig,
                    targetConfig = dummyConfig,
                    sourceRoot = "s3a://bucket/in",
                    targetRoot = "s3a://bucket/out",
                    maxAttempts = 1,
                    retryDelayMs = 0,
                )

            val result = copier.copySingleFile(sourcePathString)

            assertFalse(result.success)
            assertTrue(result.error!!.contains("Rename failed"))
            verify(exactly = 1) { targetFs.delete(any(), any()) }
        } finally {
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `best-effort temp deletion when the target stream cannot be opened`() {
        val sourcePathString = "s3a://bucket/in/file.txt"
        val targetPathString = "s3a://bucket/out/file.txt"
        val sourcePath = Path(sourcePathString)
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) } returns targetFs
            every { targetFs.exists(any()) } returns true
            every { targetFs.delete(any(), any()) } returns true
            every { sourceFs.getFileStatus(sourcePath) } returns FileStatus(11L, false, 1, 1024L, 0L, sourcePath)
            every { sourceFs.open(sourcePath, any()) } answers { sourceStream(11) }
            every { targetFs.create(any(), any<Boolean>(), any<Int>()) } throws java.io.IOException("mid-stream boom")
            every { sourceFs.close() } just runs
            every { targetFs.close() } just runs

            val copier =
                FileCopier(
                    sourceConfig = dummyConfig,
                    targetConfig = dummyConfig,
                    sourceRoot = "s3a://bucket/in",
                    targetRoot = "s3a://bucket/out",
                    maxAttempts = 1,
                    retryDelayMs = 0,
                )

            val result = copier.copySingleFile(sourcePathString)

            assertFalse(result.success)
            verify(exactly = 1) { targetFs.delete(any(), any()) }
            verify(exactly = 0) { targetFs.rename(any(), any()) }
        } finally {
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `best-effort temp deletion when the copy fails mid-write`() {
        val sourcePathString = "s3a://bucket/in/file.txt"
        val targetPathString = "s3a://bucket/out/file.txt"
        val sourcePath = Path(sourcePathString)
        val sourceLen = 96L * 1024
        val sourceFs = mockk<FileSystem>()
        val targetFs = mockk<FileSystem>()
        var chunksWritten = 0

        mockkStatic(FileSystem::class)
        try {
            every { FileSystem.newInstance(URI(sourcePathString), any<Configuration>()) } returns sourceFs
            every { FileSystem.newInstance(URI(targetPathString), any<Configuration>()) } returns targetFs
            every { targetFs.exists(any()) } returns true
            every { targetFs.delete(any(), any()) } returns true
            every { sourceFs.getFileStatus(sourcePath) } returns
                FileStatus(sourceLen, false, 1, 1024L, 0L, sourcePath)
            every { sourceFs.open(sourcePath, any()) } answers { sourceStream(sourceLen.toInt()) }
            every { targetFs.create(any(), any<Boolean>(), any<Int>()) } answers { failingSinkStream { chunksWritten++ } }
            every { sourceFs.close() } just runs
            every { targetFs.close() } just runs

            val copier =
                FileCopier(
                    sourceConfig = dummyConfig,
                    targetConfig = dummyConfig,
                    sourceRoot = "s3a://bucket/in",
                    targetRoot = "s3a://bucket/out",
                    maxAttempts = 1,
                    retryDelayMs = 0,
                )

            val result = copier.copySingleFile(sourcePathString)

            assertFalse(result.success)
            assertTrue(chunksWritten > 1, "expected bytes in flight before the failure, wrote $chunksWritten chunk(s)")
            verify(exactly = 1) { targetFs.delete(any(), any()) }
            verify(exactly = 0) { targetFs.rename(any(), any()) }
        } finally {
            unmockkStatic(FileSystem::class)
        }
    }

    @Test
    fun `leaves no temp residue at target after successful copy`() {
        val sourceFile =
            File(sourceDir, "data/file.txt").apply {
                parentFile.mkdirs()
                writeText("hello world")
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

        assertTrue(copier.copySingleFile(sourceFile.toURI().toString()).success)

        val residue =
            File(targetDir, "data").listFiles()?.filter { it.name.startsWith(TempFiles.PREFIX) } ?: emptyList()
        assertTrue(residue.isEmpty(), "expected no temp residue, found: $residue")
        assertEquals("hello world", File(targetDir, "data/file.txt").readText())
    }
}
