package com.iomete.catalogsync.extract

import com.iomete.catalogsync.PresidioClient
import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.extract.datasets.DatasourceV1LikeTableExtractor
import com.iomete.catalogsync.extract.datasets.GenericTableExtractor
import com.iomete.catalogsync.extract.datasets.IcebergTableExtractor
import com.iomete.catalogsync.extract.datasets.ViewExtractor
import io.mockk.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.catalyst.TableIdentifier
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.Option

class TableExtractorFactoryTest {

    private lateinit var mockPresidioClient: PresidioClient
    private lateinit var mockSparkSessionProvider: SparkSessionProvider
    private lateinit var mockSparkSession: SparkSession
    private lateinit var factory: TableExtractorFactory

    @BeforeEach
    fun setup() {
        mockPresidioClient = mockk()
        mockSparkSessionProvider = mockk()
        mockSparkSession = mockk()

        every { mockSparkSessionProvider.sparkSession } returns mockSparkSession

        factory = TableExtractorFactory(mockPresidioClient, mockSparkSessionProvider)
    }

    @Test
    fun `extractorFor should return ViewExtractor when isView is true`() {
        val result = factory.extractorFor(
            provider = "iceberg", isView = true,
            catalog = "c", schema = "s", table = "t"
        )

        assertTrue(result is ViewExtractor)
    }

    @Test
    fun `extractorFor should return IcebergTableExtractor for iceberg provider`() {
        val result = factory.extractorFor(
            provider = "iceberg", isView = false,
            catalog = "c", schema = "s", table = "t"
        )

        assertTrue(result is IcebergTableExtractor)
    }

    @Test
    fun `extractorFor should return DatasourceV1LikeTableExtractor for parquet provider`() {
        setupSparkCatalogMocks()

        val result = factory.extractorFor(
            provider = "parquet", isView = false,
            catalog = "c", schema = "s", table = "t"
        )

        assertTrue(result is DatasourceV1LikeTableExtractor)
    }

    @Test
    fun `extractorFor should return DatasourceV1LikeTableExtractor for orc provider`() {
        setupSparkCatalogMocks()

        val result = factory.extractorFor(
            provider = "orc", isView = false,
            catalog = "c", schema = "s", table = "t"
        )

        assertTrue(result is DatasourceV1LikeTableExtractor)
    }

    @Test
    fun `extractorFor should return DatasourceV1LikeTableExtractor for hive provider`() {
        setupSparkCatalogMocks()

        val result = factory.extractorFor(
            provider = "hive", isView = false,
            catalog = "c", schema = "s", table = "t"
        )

        assertTrue(result is DatasourceV1LikeTableExtractor)
    }

    @Test
    fun `extractorFor should return GenericTableExtractor for unknown provider`() {
        val result = factory.extractorFor(
            provider = "jdbc", isView = false,
            catalog = "c", schema = "s", table = "t"
        )

        assertTrue(result is GenericTableExtractor)
    }

    @Test
    fun `extractorFor should prioritize isView over provider`() {
        val result = factory.extractorFor(
            provider = "parquet", isView = true,
            catalog = "c", schema = "s", table = "t"
        )

        assertTrue(result is ViewExtractor)
    }

    private fun setupSparkCatalogMocks() {
        val mockSessionState = mockk<SessionState>()
        val mockSessionCatalog = mockk<SessionCatalog>()
        val mockCatalog = mockk<Catalog>()
        val mockCatalogTable = mockk<CatalogTable>(relaxed = true)
        val mockTable = mockk<Table>(relaxed = true)

        every { mockSparkSession.sessionState() } returns mockSessionState
        every { mockSessionState.catalog() } returns mockSessionCatalog
        every { mockSessionCatalog.getTempViewOrPermanentTableMetadata(any<TableIdentifier>()) } returns mockCatalogTable
        every { mockSparkSession.catalog() } returns mockCatalog
        every { mockCatalog.getTable(any<String>(), any<String>()) } returns mockTable
    }
}
