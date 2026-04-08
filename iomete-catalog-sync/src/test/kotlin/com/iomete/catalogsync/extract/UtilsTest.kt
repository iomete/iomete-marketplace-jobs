package com.iomete.catalogsync.extract

import com.iomete.catalogsync.mockRow
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.Option
import java.sql.Timestamp
import java.time.Instant

class UtilsTest {

    @Test
    fun `Option getOrNull returns value when defined`() {
        val option = Option.apply("test")
        assertEquals("test", option.getOrNull())
    }

    @Test
    fun `Option getOrNull returns null when empty`() {
        val option = Option.empty<String>()
        assertNull(option.getOrNull())
    }

    @Test
    fun `Row getTimestamp returns epoch millis for valid timestamp`() {
        val instant = Instant.parse("2025-01-15T10:30:00Z")
        val ts = Timestamp.from(instant)
        val row = mockRow(mapOf("committed_at" to ts))
        assertEquals(instant.toEpochMilli(), row.getTimestamp("committed_at"))
    }

    @Test
    fun `Row getTimestamp returns null for null field`() {
        val row = mockRow(mapOf("committed_at" to null))
        assertNull(row.getTimestamp("committed_at"))
    }

    @Test
    fun `Row getLong returns value for valid long`() {
        val row = mockRow(mapOf("count" to 42L))
        assertEquals(42L, row.getLong("count"))
    }

    @Test
    fun `Row getLong returns null for null field`() {
        val row = mockRow(mapOf("count" to null))
        assertNull(row.getLong("count"))
    }
}
