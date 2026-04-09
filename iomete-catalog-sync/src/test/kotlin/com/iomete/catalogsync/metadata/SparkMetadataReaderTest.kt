package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CoreClient
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
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

    /** Creates a mocked Row matching the 3-column shape of DESCRIBE EXTENDED output. */
    private fun describeRow(name: String, type: String, comment: String? = null): Row {
        val row = mockk<Row>()
        every { row.getString(0) } returns name
        every { row.getString(1) } returns type
        every { row.getString(2) } returns comment
        return row
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


    @Test
    fun `getSchemas returns schema names from Spark SQL result`() {
        val row1 = mockk<Row>()
        val row2 = mockk<Row>()
        every { row1.getString(0) } returns "schema1"
        every { row2.getString(0) } returns "schema2"

        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show databases in `catalog1`") } returns dataset
        every { dataset.collectAsList() } returns listOf(row1, row2)

        val result = reader.getSchemas(mockSparkSession, "catalog1")

        assertEquals(listOf("schema1", "schema2"), result)
    }

    @Test
    fun `getSchemas returns empty list when Spark throws`() {
        every { mockSparkSession.sql("show databases in `catalog1`") } throws RuntimeException("Spark error")

        val result = reader.getSchemas(mockSparkSession, "catalog1")

        assertEquals(emptyList<String>(), result)
    }

    @Test
    fun `getSchemas returns empty list for empty result`() {
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show databases in `catalog1`") } returns dataset
        every { dataset.collectAsList() } returns emptyList()

        val result = reader.getSchemas(mockSparkSession, "catalog1")

        assertEquals(emptyList<String>(), result)
    }


    @Test
    fun `getSchemaProperties parses properties from DESC DATABASE EXTENDED output`() {
        val row1 = mockk<Row>()
        every { row1.getString(0) } returns "Database Name"
        every { row1.getString(1) } returns "test_schema"

        val row2 = mockk<Row>()
        every { row2.getString(0) } returns "Properties"
        every { row2.getString(1) } returns "((key1,value1),(key2,value2))"

        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("DESC DATABASE EXTENDED `catalog1`.`test_schema`") } returns dataset
        every { dataset.collectAsList() } returns listOf(row1, row2)

        val result = reader.getSchemaProperties(mockSparkSession, "catalog1", "test_schema")

        assertEquals(mapOf("key1" to "value1", "key2" to "value2"), result)
    }

    @Test
    fun `getSchemaProperties returns empty map when no properties row`() {
        val row1 = mockk<Row>()
        every { row1.getString(0) } returns "Database Name"
        every { row1.getString(1) } returns "test_schema"

        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("DESC DATABASE EXTENDED `catalog1`.`test_schema`") } returns dataset
        every { dataset.collectAsList() } returns listOf(row1)

        val result = reader.getSchemaProperties(mockSparkSession, "catalog1", "test_schema")

        assertEquals(emptyMap<String, String>(), result)
    }

    @Test
    fun `getSchemaProperties returns empty map for malformed properties string`() {
        val row = mockk<Row>()
        every { row.getString(0) } returns "Properties"
        every { row.getString(1) } returns "malformed_no_commas"

        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("DESC DATABASE EXTENDED `catalog1`.`test_schema`") } returns dataset
        every { dataset.collectAsList() } returns listOf(row)

        val result = reader.getSchemaProperties(mockSparkSession, "catalog1", "test_schema")

        assertEquals(emptyMap<String, String>(), result)
    }

    @Test
    fun `getSchemaProperties returns empty map on Spark SQL failure`() {
        every { mockSparkSession.sql("DESC DATABASE EXTENDED `catalog1`.`test_schema`") } throws RuntimeException("SQL error")

        val result = reader.getSchemaProperties(mockSparkSession, "catalog1", "test_schema")

        assertEquals(emptyMap<String, String>(), result)
    }

    @Test
    fun `getSchemaProperties handles nested brackets in values`() {
        val row = mockk<Row>()
        every { row.getString(0) } returns "Properties"
        every { row.getString(1) } returns "((key1,value with spaces))"

        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("DESC DATABASE EXTENDED `catalog1`.`test_schema`") } returns dataset
        every { dataset.collectAsList() } returns listOf(row)

        val result = reader.getSchemaProperties(mockSparkSession, "catalog1", "test_schema")

        assertEquals(mapOf("key1" to "value with spaces"), result)
    }


    @Test
    fun `getTables fetches views for glue catalog type`() {
        val mockTableRow = mockk<Row>()
        every { mockTableRow.getString(1) } returns "table1"
        every { mockTableRow.getBoolean(2) } returns false

        val mockViewRow = mockk<Row>()
        every { mockViewRow.getString(1) } returns "view1"
        every { mockViewRow.getBoolean(2) } returns false

        val tablesDataset = mockk<Dataset<Row>>()
        val viewsDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } returns viewsDataset
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow)
        every { viewsDataset.collectAsList() } returns listOf(mockViewRow)

        val catalog = CoreClient.CatalogDetails(name = "catalog1", type = listOf("glue"))
        val result = reader.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(2, result.size)
        assertTrue(result.contains("table1"))
        assertTrue(result.contains("view1"))
    }

    @Test
    fun `getTables fetches views for rest catalog type`() {
        val mockTableRow = mockk<Row>()
        every { mockTableRow.getString(1) } returns "table1"
        every { mockTableRow.getBoolean(2) } returns false

        val mockViewRow = mockk<Row>()
        every { mockViewRow.getString(1) } returns "view1"
        every { mockViewRow.getBoolean(2) } returns false

        val tablesDataset = mockk<Dataset<Row>>()
        val viewsDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { mockSparkSession.sql("show views from `catalog1`.`schema1`") } returns viewsDataset
        every { tablesDataset.collectAsList() } returns listOf(mockTableRow)
        every { viewsDataset.collectAsList() } returns listOf(mockViewRow)

        val catalog = CoreClient.CatalogDetails(name = "catalog1", type = listOf("rest"))
        val result = reader.getTables(mockSparkSession, catalog, "schema1").map { it.name }

        assertEquals(2, result.size)
        assertTrue(result.contains("view1"))
    }

    @Test
    fun `getTables preserves isTemp flag`() {
        val mockRow = mockk<Row>()
        every { mockRow.getString(1) } returns "temp_table"
        every { mockRow.getBoolean(2) } returns true

        val tablesDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show tables from `catalog1`.`schema1`") } returns tablesDataset
        every { tablesDataset.collectAsList() } returns listOf(mockRow)

        val catalog = CoreClient.CatalogDetails(name = "catalog1", type = listOf("jdbc"))
        val result = reader.getTables(mockSparkSession, catalog, "schema1")

        assertEquals(1, result.size)
        assertTrue(result[0].isTemp)
    }


    @Test
    fun `describeTable parses columns section correctly`() {
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("describe extended `cat`.`sch`.`tbl`") } returns dataset
        every { dataset.collectAsList() } returns listOf(
            describeRow("id", "int", "primary key"),
            describeRow("name", "string"),
        )

        val result = reader.describeTable(mockSparkSession, "cat", "sch", "tbl")

        assertEquals(2, result.columns.size)
        assertEquals("id", result.columns[0].name)
        assertEquals("int", result.columns[0].dataType)
        assertEquals("primary key", result.columns[0].description)
        assertEquals("name", result.columns[1].name)
        assertEquals("string", result.columns[1].dataType)
        assertNull(result.columns[1].description)
    }

    @Test
    fun `describeTable parses partition columns`() {
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("describe extended `cat`.`sch`.`tbl`") } returns dataset
        every { dataset.collectAsList() } returns listOf(
            describeRow("region", "string"),
            describeRow("", ""),
            describeRow("# Partition Information", ""),
            describeRow("region", "string"),
        )

        val result = reader.describeTable(mockSparkSession, "cat", "sch", "tbl")

        assertEquals(1, result.columns.size)
        assertTrue(result.columns[0].isPartitionKey)
    }

    @Test
    fun `describeTable parses table metadata section`() {
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("describe extended `cat`.`sch`.`tbl`") } returns dataset
        every { dataset.collectAsList() } returns listOf(
            describeRow("id", "int"),
            describeRow("", ""),
            describeRow("# Detailed Table Information", ""),
            describeRow("Type", "MANAGED"),
            describeRow("Provider", "iceberg"),
            describeRow("Owner", "admin"),
        )

        val result = reader.describeTable(mockSparkSession, "cat", "sch", "tbl")

        assertEquals("MANAGED", result.metadata["Type"])
        assertEquals("iceberg", result.metadata["Provider"])
        assertEquals("admin", result.metadata["Owner"])
    }

    @Test
    fun `describeTable assigns sequential sort orders to columns`() {
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("describe extended `cat`.`sch`.`tbl`") } returns dataset
        every { dataset.collectAsList() } returns listOf(
            describeRow("id", "int"),
            describeRow("name", "string"),
            describeRow("email", "string"),
        )

        val result = reader.describeTable(mockSparkSession, "cat", "sch", "tbl")

        assertEquals(0, result.columns[0].sortOrder)
        assertEquals(1, result.columns[1].sortOrder)
        assertEquals(2, result.columns[2].sortOrder)
    }


    @Test
    fun `describeTable with no partitions all columns have isPartitionKey false`() {
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("describe extended `cat`.`sch`.`tbl`") } returns dataset
        every { dataset.collectAsList() } returns listOf(
            describeRow("id", "int"),
            describeRow("name", "string"),
        )

        val result = reader.describeTable(mockSparkSession, "cat", "sch", "tbl")

        result.columns.forEach { assertFalse(it.isPartitionKey) }
    }

    @Test
    fun `describeTable with only columns and no metadata returns empty metadata map`() {
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("describe extended `cat`.`sch`.`tbl`") } returns dataset
        every { dataset.collectAsList() } returns listOf(describeRow("id", "int"))

        val result = reader.describeTable(mockSparkSession, "cat", "sch", "tbl")

        assertTrue(result.metadata.isEmpty())
        assertEquals(1, result.columns.size)
    }
}
