package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.utils.ColumnTagExtractor
import io.mockk.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class ViewExtractorTest {

    private lateinit var mockColumnTagExtractor: ColumnTagExtractor
    private lateinit var extractor: ViewExtractor

    @BeforeEach
    fun setup() {
        mockColumnTagExtractor = mockk()
        extractor = ViewExtractor(
            columnTagExtractor = mockColumnTagExtractor,
            catalog = "test_catalog",
            schema = "test_schema",
            table = "test_view"
        )
    }

    @Test
    fun `getTableType should return VIEW`() {
        assertEquals("VIEW", extractor.getTableType)
    }

    @Test
    fun `extractColumnTags should delegate to ColumnTagExtractor with correct fullName`() {
        val columns = listOf("col1", "col2")
        val expectedTags = mapOf("col1" to listOf("DETECTED_EMAIL_ADDRESS"), "col2" to emptyList())
        every { mockColumnTagExtractor.extract(any(), columns) } returns expectedTags

        val result = extractor.extractColumnTags(columns)

        assertEquals(expectedTags, result)
        verify { mockColumnTagExtractor.extract("`test_catalog`.`test_schema`.`test_view`", columns) }
    }
}
