package com.iomete.catalogsync.extract

import io.mockk.every
import io.mockk.mockk
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import scala.Option
import java.sql.Timestamp
import java.time.Instant

class UtilsTest {

    private fun mockRow(fields: Map<String, Any?>): Row {
        val row = mockk<Row>()
        val schema = mockk<StructType>()
        every { row.schema() } returns schema

        val fieldNames = fields.keys.toList()
        fieldNames.forEachIndexed { index, name ->
            every { schema.fieldIndex(name) } returns index
            val value = fields[name]
            every { row.get(index) } returns value
            when (value) {
                is Timestamp -> every { row.getTimestamp(index) } returns value
                is Long -> every { row.getLong(index) } returns value
                null -> {
                    every { row.getTimestamp(index) } returns null
                    every { row.getLong(index) } throws NullPointerException()
                }
            }
        }
        return row
    }

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
