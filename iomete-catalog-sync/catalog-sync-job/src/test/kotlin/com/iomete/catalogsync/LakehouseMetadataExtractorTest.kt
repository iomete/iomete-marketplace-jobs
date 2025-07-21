package com.iomete.catalogsync

import com.iomete.catalogsync.extract.TableExtractorFactory
import io.micrometer.core.instrument.MeterRegistry
import io.mockk.*
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class LakehouseMetadataExtractorTest {

    private lateinit var mockSparkSession: SparkSession
    private lateinit var mockDataset: Dataset<Row>
    private lateinit var mockTableExtractorFactory: TableExtractorFactory
    private lateinit var mockDataSync: DataSync
    private lateinit var mockSparkSessionProvider: SparkSessionProvider
    private lateinit var mockApplicationConfig: ApplicationConfig
    private lateinit var mockMeterRegistry: MeterRegistry
    private lateinit var mockCoreServiceClient: CoreServiceClient
    private lateinit var extractor: LakehouseMetadataExtractor

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk()
        mockDataset = mockk()
        mockTableExtractorFactory = mockk()
        mockDataSync = mockk()
        mockSparkSessionProvider = mockk()
        mockApplicationConfig = mockk()
        mockMeterRegistry = mockk()
        mockCoreServiceClient = mockk()

        every { mockSparkSessionProvider.sparkSession } returns mockSparkSession
        every { mockApplicationConfig.excludeSchemas() } returns java.util.Optional.of(setOf())

        extractor = LakehouseMetadataExtractor(
            mockTableExtractorFactory,
            mockDataSync,
            mockSparkSessionProvider,
            mockApplicationConfig,
            mockMeterRegistry,
            mockCoreServiceClient
        )
    }

    @Test
    fun `getTables should combine tables and views and remove duplicates`() {
        val mockTableRow1 = mockk<Row>()
        val mockTableRow2 = mockk<Row>()
        val mockViewRow1 = mockk<Row>()
        val mockViewRow2 = mockk<Row>()

        every { mockTableRow1.getString(1) } returns "table1"
        every { mockTableRow2.getString(1) } returns "table2"
        every { mockViewRow1.getString(1) } returns "view1"
        every { mockViewRow2.getString(1) } returns "table1" // duplicate name

        val tablesDataset = mockk<Dataset<Row>>()
        val viewsDataset = mockk<Dataset<Row>>()

        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } returns viewsDataset
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow1, mockTableRow2)
        every { viewsDataset.collectAsList() } returns listOf(mockViewRow1, mockViewRow2)

        val result = extractor.getTables("catalog1", "schema1", listOf("iceberg"))

        assertEquals(3, result.size) // Should have 3 unique items (table1, table2, view1)
        assertTrue(result.contains(mockTableRow1))
        assertTrue(result.contains(mockTableRow2))
        assertTrue(result.contains(mockViewRow1))
    }

    @Test
    fun `getTables should work when tables fail but views succeed`() {
        val mockViewRow = mockk<Row>()
        every { mockViewRow.getString(1) } returns "view1"

        val viewsDataset = mockk<Dataset<Row>>()

        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } throws RuntimeException("Tables query failed")
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } returns viewsDataset
        every { viewsDataset.collectAsList() } returns listOf(mockViewRow)

        val result = extractor.getTables("catalog1", "schema1", listOf("iceberg"))

        assertEquals(1, result.size)
        assertTrue(result.contains(mockViewRow))
    }

    @Test
    fun `getTables should work when views fail but tables succeed`() {
        val mockTableRow = mockk<Row>()
        every { mockTableRow.getString(1) } returns "table1"

        val tablesDataset = mockk<Dataset<Row>>()

        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } throws RuntimeException("Views query failed")
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow)

        val result = extractor.getTables("catalog1", "schema1", listOf("iceberg"))

        assertEquals(1, result.size)
        assertTrue(result.contains(mockTableRow))
    }

    @Test
    fun `getTables should return empty list when both queries fail`() {
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } throws RuntimeException("Tables query failed")
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } throws RuntimeException("Views query failed")

        val result = extractor.getTables("catalog1", "schema1", listOf("iceberg"))

        assertEquals(emptyList<Row>(), result)
    }
    
    @Test
    fun `should skip view fetching for unsupported catalog types`() {
        val mockTableRow = mockk<Row>()
        every { mockTableRow.getString(1) } returns "table1"

        val tablesDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow)

        val result = extractor.getTables("catalog1", "schema1", listOf("jdbc"))

        assertEquals(1, result.size)
        assertTrue(result.contains(mockTableRow))
        
        verify(exactly = 0) { mockSparkSession.sql("show views from `catalog1`.`schema1`") }
    }
    
    @Test
    fun `should check for combined supported and non-supported catalog types`() {
        val mockTableRow = mockk<Row>()
        every { mockTableRow.getString(1) } returns "table1"

        val tablesDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { mockSparkSession.sql("show tables from `catalog1`.`schema2`") } returns tablesDataset
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow)

        val result1 = extractor.getTables("catalog1", "schema1", listOf("jdbc"))
        assertEquals(1, result1.size)
        
        val result2 = extractor.getTables("catalog1", "schema2", listOf("jdbc"))
        assertEquals(1, result2.size)
        
        every { mockSparkSession.sql("show tables from `catalog2`.`schema1`") } returns tablesDataset
        val result3 = extractor.getTables("catalog2", "schema1", listOf("iceberg"))
        assertEquals(1, result3.size)

        verify(exactly = 1) { mockSparkSession.sql(match { it.contains("show views") }) }
    }
}