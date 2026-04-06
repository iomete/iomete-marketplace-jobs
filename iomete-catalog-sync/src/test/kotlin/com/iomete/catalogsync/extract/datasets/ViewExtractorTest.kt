package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.SupportTableStatistics
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class ViewExtractorTest {

    @Test
    fun `getTableType returns VIEW`() {
        val extractor = ViewExtractor(catalog = "cat", schema = "sch", table = "v1")
        assertEquals("VIEW", extractor.getTableType)
    }

    @Test
    fun `implements SupportColumnTags`() {
        val extractor = ViewExtractor(catalog = "cat", schema = "sch", table = "v1")
        assertTrue(extractor is SupportColumnTags)
    }

    @Test
    fun `does not implement SupportTableStatistics`() {
        val extractor = ViewExtractor(catalog = "cat", schema = "sch", table = "v1")
        assertFalse(extractor is SupportTableStatistics)
    }
}
