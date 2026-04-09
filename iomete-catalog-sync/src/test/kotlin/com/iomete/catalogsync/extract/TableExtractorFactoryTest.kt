package com.iomete.catalogsync.extract

import com.iomete.catalogsync.extract.datasets.DatasourceV1LikeTableExtractor
import com.iomete.catalogsync.extract.datasets.GenericTableExtractor
import com.iomete.catalogsync.extract.datasets.IcebergTableExtractor
import com.iomete.catalogsync.extract.datasets.ViewExtractor
import io.mockk.mockk
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TableExtractorFactoryTest {

    private lateinit var mockSparkSession: SparkSession
    private lateinit var factory: TableExtractorFactory

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk(relaxed = true)
        factory = TableExtractorFactory()
    }

    @Test
    fun `isView true returns ViewExtractor`() {
        val extractor = factory.extractorFor(mockSparkSession, provider = "iceberg", isView = true, catalog = "cat", schema = "sch", table = "v1")
        assertTrue(extractor is ViewExtractor)
    }

    @Test
    fun `provider iceberg returns IcebergTableExtractor`() {
        val extractor = factory.extractorFor(mockSparkSession, provider = "iceberg", isView = false, catalog = "cat", schema = "sch", table = "tbl")
        assertTrue(extractor is IcebergTableExtractor)
    }

    @Test
    fun `provider parquet returns DatasourceV1LikeTableExtractor`() {
        val extractor = factory.extractorFor(mockSparkSession, provider = "parquet", isView = false, catalog = "cat", schema = "sch", table = "tbl")
        assertTrue(extractor is DatasourceV1LikeTableExtractor)
    }

    @Test
    fun `provider orc returns DatasourceV1LikeTableExtractor`() {
        val extractor = factory.extractorFor(mockSparkSession, provider = "orc", isView = false, catalog = "cat", schema = "sch", table = "tbl")
        assertTrue(extractor is DatasourceV1LikeTableExtractor)
    }

    @Test
    fun `provider hive returns DatasourceV1LikeTableExtractor`() {
        val extractor = factory.extractorFor(mockSparkSession, provider = "hive", isView = false, catalog = "cat", schema = "sch", table = "tbl")
        assertTrue(extractor is DatasourceV1LikeTableExtractor)
    }

    @Test
    fun `provider csv returns GenericTableExtractor`() {
        val extractor = factory.extractorFor(mockSparkSession, provider = "csv", isView = false, catalog = "cat", schema = "sch", table = "tbl")
        assertTrue(extractor is GenericTableExtractor)
    }

    @Test
    fun `empty provider returns GenericTableExtractor`() {
        val extractor = factory.extractorFor(mockSparkSession, provider = "", isView = false, catalog = "cat", schema = "sch", table = "tbl")
        assertTrue(extractor is GenericTableExtractor)
    }

    @Test
    fun `isView true with provider iceberg returns ViewExtractor - view takes precedence`() {
        val extractor = factory.extractorFor(mockSparkSession, provider = "iceberg", isView = true, catalog = "cat", schema = "sch", table = "tbl")
        assertTrue(extractor is ViewExtractor)
        assertEquals("VIEW", extractor.getTableType)
    }
}
