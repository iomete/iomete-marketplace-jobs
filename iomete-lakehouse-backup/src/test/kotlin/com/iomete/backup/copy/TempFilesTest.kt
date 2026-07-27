package com.iomete.backup.copy

import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class TempFilesTest {
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
}
