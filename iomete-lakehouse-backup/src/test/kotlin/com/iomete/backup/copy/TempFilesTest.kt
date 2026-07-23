package com.iomete.backup.copy

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TempFilesTest {
    private lateinit var dir: File
    private lateinit var fs: FileSystem

    @BeforeEach
    fun setup() {
        dir = Files.createTempDirectory("temp-file-test").toFile()
        fs = FileSystem.getLocal(Configuration())
    }

    @AfterEach
    fun tearDown() {
        dir.deleteRecursively()
    }

    @Test
    fun `pathFor produces a temp sibling that preserves parent and final name`() {
        val temp = TempFiles.pathFor(Path("file:/warehouse/db/part-0001.parquet"))

        assertTrue(TempFiles.isTemp(temp.name))
        assertTrue(temp.name.endsWith("-part-0001.parquet"))
        assertEquals(Path("file:/warehouse/db"), temp.parent)
    }

    @Test
    fun `isTemp matches only the reserved prefix`() {
        assertTrue(TempFiles.isTemp("${TempFiles.PREFIX}abc-data.parquet"))
        assertFalse(TempFiles.isTemp("data.parquet"))
        assertFalse(TempFiles.isTemp(".other-hidden"))
    }

    @Test
    fun `sweep deletes only temp files and returns the count`() {
        File(dir, "sub").mkdirs()
        val real = File(dir, "sub/data.parquet").apply { writeText("keep") }
        val temp1 = File(dir, "sub/${TempFiles.PREFIX}a-data.parquet").apply { writeText("x") }
        val temp2 = File(dir, "${TempFiles.PREFIX}b-top.parquet").apply { writeText("x") }

        val deleted = TempFiles.sweep(fs, Path(dir.toURI().toString()))

        assertEquals(2, deleted)
        assertTrue(real.exists())
        assertFalse(temp1.exists())
        assertFalse(temp2.exists())
    }

    @Test
    fun `sweep on a missing root is a no-op`() {
        val missing = Path(File(dir, "does-not-exist").toURI().toString())
        assertEquals(0, TempFiles.sweep(fs, missing))
    }
}
