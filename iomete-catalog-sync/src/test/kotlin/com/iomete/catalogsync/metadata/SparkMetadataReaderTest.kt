package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CoreClient
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

class SparkMetadataReaderTest {
    private lateinit var mockSparkSession: SparkSession
    private lateinit var reader: SparkMetadataReader

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk(relaxed = true)
        reader = SparkMetadataReader()
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

        val catalog = CoreClient.CatalogDetails(name = "catalog1", type = listOf("iceberg"))
        val result = reader.getTables(mockSparkSession, catalog, "schema1").map { it.name }

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

        val catalog = CoreClient.CatalogDetails(name = "catalog1", type = listOf("iceberg"))
        val result = reader.getTables(mockSparkSession, catalog, "schema1").map { it.name }

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

        val catalog = CoreClient.CatalogDetails(name = "catalog1", type = listOf("iceberg"))
        val result = reader.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(1, result.size)
        assertTrue(result.contains("table1"))
    }

    @Test
    fun `getTables should return empty list when both queries fail`() {
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } throws RuntimeException("Tables query failed")
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } throws RuntimeException("Views query failed")

        val catalog = CoreClient.CatalogDetails(name = "catalog1", type = listOf("iceberg"))
        val result = reader.getTables(mockSparkSession, catalog, "schema1").map { it.name }

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

        val catalog = CoreClient.CatalogDetails(name = "catalog1", type = listOf("jdbc"))
        val result = reader.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(1, result.size)
        assertTrue(result.contains("table1"))

        verify(exactly = 0) { mockSparkSession.sql("show views from `catalog1`.`schema1`") }
    }
}
