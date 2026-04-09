package com.iomete.catalogsync

import io.mockk.every
import io.mockk.mockk
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import java.sql.Timestamp

/**
 * Creates a mocked [Row] whose fields are accessible by name (via schema.fieldIndex)
 * and by positional index. Supports [Timestamp], [Long], and null values with
 * appropriate typed accessor stubs.
 */
fun mockRow(fields: Map<String, Any?>): Row {
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
