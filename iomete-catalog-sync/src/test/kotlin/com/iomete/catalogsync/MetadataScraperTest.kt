package com.iomete.catalogsync

import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.ExclusionRules
import com.iomete.catalogsync.extract.TableExtractorFactory
import com.iomete.catalogsync.presidio.PIIDetectionService
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MetadataScraperTest {
    private lateinit var mockSparkSession: SparkSession
    private lateinit var mockDataset: Dataset<Row>
    private lateinit var mockTableExtractorFactory: TableExtractorFactory
    private lateinit var mockCatalogServiceClient: CatalogClient
    private lateinit var mockSparkSessionProvider: SparkSessionProvider
    private lateinit var mockApplicationConfig: ApplicationConfig
    private lateinit var mockMeterRegistry: MeterRegistry
    private lateinit var mockCoreServiceClient: CoreClient
    private lateinit var mockPiiDetectionService: PIIDetectionService
    private lateinit var extractor: MetadataScraper

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk()
        mockDataset = mockk()
        mockTableExtractorFactory = mockk()
        mockCatalogServiceClient = mockk()
        mockSparkSessionProvider = mockk()
        mockApplicationConfig = mockk()
        mockMeterRegistry = mockk()
        mockCoreServiceClient = mockk()
        mockPiiDetectionService = mockk()

        every { mockSparkSessionProvider.getSession(any()) } returns mockSparkSession
        every { mockApplicationConfig.exclusionRules } returns ExclusionRules()

        extractor =
            MetadataScraper(
                mockSparkSessionProvider,
                mockApplicationConfig,
                mockTableExtractorFactory,
                mockPiiDetectionService,
                mockCoreServiceClient,
                mockCatalogServiceClient,
            )
    }

    @Test
    fun `getTables should combine tables and views and remove duplicates`() {
        val mockTableRow1 = mockk<Row>()
        val mockTableRow2 = mockk<Row>()
        val mockViewRow1 = mockk<Row>()
        val mockViewRow2 = mockk<Row>()

        every { mockTableRow1.getString(1) } returns "table1"
        every { mockTableRow1.getBoolean(2) } returns false
        every { mockTableRow2.getString(1) } returns "table2"
        every { mockTableRow2.getBoolean(2) } returns false
        every { mockViewRow1.getString(1) } returns "view1"
        every { mockViewRow1.getBoolean(2) } returns false
        every { mockViewRow2.getString(1) } returns "table1" // duplicate name
        every { mockViewRow2.getBoolean(2) } returns false

        val tablesDataset = mockk<Dataset<Row>>()
        val viewsDataset = mockk<Dataset<Row>>()

        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } returns viewsDataset
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow1, mockTableRow2)
        every { viewsDataset.collectAsList() } returns listOf(mockViewRow1, mockViewRow2)

        val catalog = CatalogDetails(name = "catalog1", type = listOf("iceberg"))
        val result = extractor.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(3, result.size) // Should have 3 unique items (table1, table2, view1)
        assertTrue(result.contains("table1"))
        assertTrue(result.contains("table2"))
        assertTrue(result.contains("view1"))
    }

    @Test
    fun `getTables should work when tables fail but views succeed`() {
        val mockViewRow = mockk<Row>()
        every { mockViewRow.getString(1) } returns "view1"
        every { mockViewRow.getBoolean(2) } returns false

        val viewsDataset = mockk<Dataset<Row>>()

        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } throws RuntimeException("Tables query failed")
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } returns viewsDataset
        every { viewsDataset.collectAsList() } returns listOf(mockViewRow)

        val catalog = CatalogDetails(name = "catalog1", type = listOf("iceberg"))
        val result = extractor.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(1, result.size)
        assertTrue(result.contains("view1"))
    }

    @Test
    fun `getTables should work when views fail but tables succeed`() {
        val mockTableRow = mockk<Row>()
        every { mockTableRow.getString(1) } returns "table1"
        every { mockTableRow.getBoolean(2) } returns false

        val tablesDataset = mockk<Dataset<Row>>()

        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } throws RuntimeException("Views query failed")
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow)

        val catalog = CatalogDetails(name = "catalog1", type = listOf("iceberg"))
        val result = extractor.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(1, result.size)
        assertTrue(result.contains("table1"))
    }

    @Test
    fun `getTables should return empty list when both queries fail`() {
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } throws RuntimeException("Tables query failed")
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } throws RuntimeException("Views query failed")

        val catalog = CatalogDetails(name = "catalog1", type = listOf("iceberg"))
        val result = extractor.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(emptyList<String>(), result)
    }

    @Test
    fun `should skip view fetching for unsupported catalog types`() {
        val mockTableRow = mockk<Row>()
        every { mockTableRow.getString(1) } returns "table1"
        every { mockTableRow.getBoolean(2) } returns false

        val tablesDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow)

        val catalog = CatalogDetails(name = "catalog1", type = listOf("jdbc"))
        val result = extractor.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(1, result.size)
        assertTrue(result.contains("table1"))

        verify(exactly = 0) { mockSparkSession.sql("show views from `catalog1`.`schema1`") }
    }

    @Test
    fun `should check for combined supported and non-supported catalog types`() {
        val mockTableRow = mockk<Row>()
        every { mockTableRow.getString(1) } returns "table1"
        every { mockTableRow.getBoolean(2) } returns false

        val tablesDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { mockSparkSession.sql("show tables from `catalog1`.`schema2`") } returns tablesDataset
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow)

        val catalog1 = CatalogDetails(name = "catalog1", type = listOf("jdbc"))
        val catalog2 = CatalogDetails(name = "catalog1", type = listOf("iceberg"))

        val result1 = extractor.getTables(mockSparkSession, catalog1, "schema1").map { it.name }
        assertEquals(1, result1.size)

        val result2 = extractor.getTables(mockSparkSession, catalog1, "schema2").map { it.name }
        assertEquals(1, result2.size)

        every { mockSparkSession.sql("show tables from `catalog2`.`schema1`") } returns tablesDataset
        val result3 = extractor.getTables(mockSparkSession, catalog2, "schema1").map { it.name }
        assertEquals(1, result3.size)

        verify(exactly = 1) { mockSparkSession.sql(match { it.contains("show views") }) }
    }
}
