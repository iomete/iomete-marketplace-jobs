package com.iomete.cleanup.untrackedtablefolders.storage

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.zip.GZIPOutputStream
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class MetadataHeadTest {
    private val folder = "s3a://bucket/lake/db.db/orphan"

    private fun verdict(
        json: String,
        gzip: Boolean = false,
    ): MetadataVerdict {
        val bytes = json.toByteArray()
        val content = if (gzip) ByteArrayOutputStream().also { out -> GZIPOutputStream(out).use { it.write(bytes) } }.toByteArray() else bytes
        return MetadataHead.verdict(ByteArrayInputStream(content), gzip, folder)
    }

    @Test
    fun `a table whose location is this folder is its own table`() {
        assertEquals(
            MetadataVerdict.OWN_TABLE,
            verdict("""{"format-version":2,"table-uuid":"9c12d441","location":"$folder/","schemas":[{"fields":[]}]}"""),
        )
        assertEquals(MetadataVerdict.OWN_TABLE, verdict("""{"location":"$folder","table-uuid":"9c12d441"}""", gzip = true))
    }

    @Test
    fun `a view, a missing uuid and another location hold the folder`() {
        assertEquals(MetadataVerdict.VIEW, verdict("""{"view-uuid":"1","format-version":1,"location":"$folder"}"""))
        assertEquals(MetadataVerdict.NO_UUID, verdict("""{"format-version":1,"location":"$folder"}"""))
        assertEquals(MetadataVerdict.NO_UUID, verdict("""{"table-uuid":" ","location":"$folder"}"""))
        assertEquals(MetadataVerdict.NO_UUID, verdict("""{"table-uuid":7,"location":"$folder"}"""))
        assertEquals(MetadataVerdict.LOCATION_MISMATCH, verdict("""{"table-uuid":"1","location":"s3a://bucket/lake/db.db/orders"}"""))
        assertEquals(MetadataVerdict.LOCATION_MISMATCH, verdict("""{"table-uuid":"1","location":"$folder/sub"}"""))
        assertEquals(MetadataVerdict.LOCATION_MISMATCH, verdict("""{"table-uuid":"1"}"""))
        assertEquals(MetadataVerdict.LOCATION_MISMATCH, verdict("""{"table-uuid":"1","location":null}"""))
        assertEquals(
            MetadataVerdict.LOCATION_MISMATCH,
            verdict("""{"location":{"location":"$folder"},"table-uuid":{"table-uuid":"1"},"properties":{"table-uuid":"2"},"table-uuid":"3"}"""),
        )
    }

    @Test
    fun `a file that is not a JSON object is unreadable`() {
        assertEquals(MetadataVerdict.UNREADABLE, verdict("""["table-uuid"]"""))
        assertThrows(IOException::class.java) { verdict("""{"table-uuid":"1",""") }
        assertThrows(IOException::class.java) { verdict("not json") }
        assertThrows(IOException::class.java) { MetadataHead.verdict(ByteArrayInputStream("{}".toByteArray()), true, folder) }
    }

    @Test
    fun `the head is read only up to its limit`() {
        val padding = "x".repeat(MetadataHead.MAX_HEAD_BYTES.toInt())
        assertThrows(IOException::class.java) { verdict("""{"snapshots":"$padding","table-uuid":"1","location":"$folder"}""") }
        assertThrows(IOException::class.java) { verdict("""{"snapshots":"$padding","table-uuid":"1","location":"$folder"}""", gzip = true) }
        assertEquals(MetadataVerdict.OWN_TABLE, verdict("""{"table-uuid":"1","location":"$folder","snapshots":"$padding"}"""))
    }
}
