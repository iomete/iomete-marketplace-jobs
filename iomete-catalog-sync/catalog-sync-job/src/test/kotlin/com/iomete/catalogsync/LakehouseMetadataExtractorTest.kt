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
    
    // Helper to create a mock Row with 3 string columns for processTableColumns
    private fun mockDescribeRow(colName: String, dataType: String, comment: String?): Row {
        val row = mockk<Row>()
        every { row.getString(0) } returns colName
        every { row.getString(1) } returns dataType
        every { row.getString(2) } returns comment
        return row
    }

    @Test
    fun `processTableColumns should parse basic columns correctly`() {
        val rows = listOf(
            mockDescribeRow("id", "int", "primary key"),
            mockDescribeRow("name", "string", "user name"),
            mockDescribeRow("age", "int", null)
        )

        val result = extractor.processTableColumns(rows)

        assertEquals(3, result.columns.size)
        assertEquals("id", result.columns[0].name)
        assertEquals("int", result.columns[0].dataType)
        assertEquals("primary key", result.columns[0].description)
        assertEquals(0, result.columns[0].sortOrder)
        assertFalse(result.columns[0].isPartitionKey)

        assertEquals("name", result.columns[1].name)
        assertEquals("string", result.columns[1].dataType)
        assertEquals(1, result.columns[1].sortOrder)

        assertEquals("age", result.columns[2].name)
        assertNull(result.columns[2].description)
    }

    @Test
    fun `processTableColumns should handle partition information section`() {
        val rows = listOf(
            mockDescribeRow("id", "int", null),
            mockDescribeRow("region", "string", null),
            mockDescribeRow("# Partition Information", "", ""),
            mockDescribeRow("# col_name", "data_type", "comment"),
            mockDescribeRow("region", "string", null)
        )

        val result = extractor.processTableColumns(rows)

        assertEquals(2, result.columns.size)
        assertFalse(result.columns[0].isPartitionKey) // id
        assertTrue(result.columns[1].isPartitionKey) // region
    }

    @Test
    fun `processTableColumns should handle iceberg-style partitioning with Part prefix`() {
        val rows = listOf(
            mockDescribeRow("dt", "date", null),
            mockDescribeRow("# Partitioning", "", ""),
            mockDescribeRow("Part 0", "dt", null)
        )

        val result = extractor.processTableColumns(rows)

        assertEquals(1, result.columns.size)
        assertTrue(result.columns[0].isPartitionKey) // dt marked via dataType
    }

    @Test
    fun `processTableColumns should extract table info metadata`() {
        val rows = listOf(
            mockDescribeRow("id", "int", null),
            mockDescribeRow("# Detailed Table Information", "", ""),
            mockDescribeRow("Type", "MANAGED", null),
            mockDescribeRow("Provider", "iceberg", null),
            mockDescribeRow("Owner", "admin", null),
            mockDescribeRow("Location", "s3://bucket/path", null)
        )

        val result = extractor.processTableColumns(rows)

        assertEquals(1, result.columns.size)
        assertEquals("MANAGED", result.metadata["Type"])
        assertEquals("iceberg", result.metadata["Provider"])
        assertEquals("admin", result.metadata["Owner"])
        assertEquals("s3://bucket/path", result.metadata["Location"])
    }

    @Test
    fun `processTableColumns should set Type to view for view info section`() {
        val rows = listOf(
            mockDescribeRow("id", "int", null),
            mockDescribeRow("# Detailed View Information", "", ""),
            mockDescribeRow("View Text", "SELECT * FROM t", null)
        )

        val result = extractor.processTableColumns(rows)

        assertEquals("view", result.metadata["Type"])
        assertEquals("SELECT * FROM t", result.metadata["View Text"])
    }

    @Test
    fun `processTableColumns should skip blank and hash-prefixed rows`() {
        val rows = listOf(
            mockDescribeRow("id", "int", null),
            mockDescribeRow("", "", ""),
            mockDescribeRow("# some comment", "", ""),
            mockDescribeRow("name", "string", null)
        )

        val result = extractor.processTableColumns(rows)

        assertEquals(2, result.columns.size)
        assertEquals("id", result.columns[0].name)
        assertEquals("name", result.columns[1].name)
    }

    @Test
    fun `processTableColumns should return empty columns for empty input`() {
        val result = extractor.processTableColumns(emptyList())

        assertTrue(result.columns.isEmpty())
        assertTrue(result.metadata.isEmpty())
    }

    @Test
    fun `processTableColumns should handle metadata section without processing`() {
        val rows = listOf(
            mockDescribeRow("id", "int", null),
            mockDescribeRow("# Metadata Columns", "", ""),
            mockDescribeRow("_file", "string", "internal metadata"),
            mockDescribeRow("_pos", "long", "internal position")
        )

        val result = extractor.processTableColumns(rows)

        assertEquals(1, result.columns.size)
        assertEquals("id", result.columns[0].name)
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