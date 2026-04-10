package com.iomete.catalogsync.extract.datasets

import io.mockk.mockk
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class GenericTableExtractorTest {

    @Test
    fun `getTableType should return UNKNOWN`() {
        val mockSpark = mockk<SparkSession>()
        val extractor = GenericTableExtractor(
            spark = mockSpark,
            catalog = "test_catalog",
            schema = "test_schema",
            tableName = "test_table"
        )

        assertEquals("UNKNOWN", extractor.getTableType)
    }
}
