package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.SupportTableStatistics
import io.mockk.mockk
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class GenericTableExtractorTest {

    private val mockSparkSession: SparkSession = mockk(relaxed = true)

    @Test
    fun `getTableType returns UNKNOWN`() {
        val extractor = GenericTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertEquals("UNKNOWN", extractor.getTableType)
    }

    @Test
    fun `does not implement SupportTableStatistics`() {
        val extractor = GenericTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertFalse(extractor is SupportTableStatistics)
    }

    @Test
    fun `does not implement SupportColumnTags`() {
        val extractor = GenericTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertFalse(extractor is SupportColumnTags)
    }
}
