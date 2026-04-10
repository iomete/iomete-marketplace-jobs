package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.PresidioClient
import com.iomete.catalogsync.extract.utils.ColumnTagExtractor
import io.mockk.*
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.sql.Timestamp

class IcebergTableExtractorTest {

    private lateinit var mockSpark: SparkSession
    private lateinit var mockColumnTagExtractor: ColumnTagExtractor
    private lateinit var extractor: IcebergTableExtractor

    @BeforeEach
    fun setup() {
        mockSpark = mockk()
        mockColumnTagExtractor = mockk()

        extractor = IcebergTableExtractor(
            spark = mockSpark,
            columnTagExtractor = mockColumnTagExtractor,
            catalog = "test_catalog",
            schema = "test_schema",
            table = "test_table"
        )
    }

    @Test
    fun `getTableType should return MANAGED`() {
        assertEquals("MANAGED", extractor.getTableType)
    }

    @Test
    fun `extractTableStatistics should return stats from snapshots and data files`() {
        val snapshotRow = mockk<Row>()
        val snapshotSchema = mockk<StructType>()
        every { snapshotRow.schema() } returns snapshotSchema
        every { snapshotSchema.fieldIndex("committed_at") } returns 0
        every { snapshotSchema.fieldIndex("total_files_sizes") } returns 1
        every { snapshotSchema.fieldIndex("total_records") } returns 2
        every { snapshotSchema.fieldIndex("total_data_files") } returns 3
        every { snapshotRow.getTimestamp(0) } returns Timestamp(1700000000000L)
        every { snapshotRow.get(1) } returns 1024L
        every { snapshotRow.getLong(1) } returns 1024L
        every { snapshotRow.get(2) } returns 100L
        every { snapshotRow.getLong(2) } returns 100L
        every { snapshotRow.get(3) } returns 5L
        every { snapshotRow.getLong(3) } returns 5L

        val dataFilesRow = mockk<Row>()
        val dataFilesSchema = mockk<StructType>()
        every { dataFilesRow.schema() } returns dataFilesSchema
        every { dataFilesSchema.fieldIndex("total_table_num_files") } returns 0
        every { dataFilesSchema.fieldIndex("total_table_size_in_bytes") } returns 1
        every { dataFilesRow.get(0) } returns 10L
        every { dataFilesRow.getLong(0) } returns 10L
        every { dataFilesRow.get(1) } returns 2048L
        every { dataFilesRow.getLong(1) } returns 2048L

        val snapshotDataset = mockk<Dataset<Row>>()
        val dataFilesDataset = mockk<Dataset<Row>>()

        every { mockSpark.sql(match { it.contains("snapshots") }) } returns snapshotDataset
        every { snapshotDataset.collectAsList() } returns listOf(snapshotRow)
        every { mockSpark.sql(match { it.contains("all_data_files") }) } returns dataFilesDataset
        every { dataFilesDataset.collectAsList() } returns listOf(dataFilesRow)

        val result = extractor.extractTableStatistics()

        assertNotNull(result)
        assertEquals(1700000000000L, result!!.lastModified)
        assertEquals(1024L, result.sizeInBytes)
        assertEquals(100L, result.totalRecords)
        assertEquals(5L, result.numFiles)
        assertEquals(10L, result.totalTableNumFiles)
        assertEquals(2048L, result.totalTableSizeInBytes)
    }

    @Test
    fun `extractTableStatistics should return null when no snapshots exist`() {
        val snapshotDataset = mockk<Dataset<Row>>()
        every { mockSpark.sql(match { it.contains("snapshots") }) } returns snapshotDataset
        every { snapshotDataset.collectAsList() } returns emptyList()

        val result = extractor.extractTableStatistics()

        assertNull(result)
    }

    @Test
    fun `extractTableStatistics should return null when all_data_files returns empty`() {
        val snapshotRow = mockk<Row>()
        val snapshotSchema = mockk<StructType>()
        every { snapshotRow.schema() } returns snapshotSchema
        every { snapshotSchema.fieldIndex("committed_at") } returns 0
        every { snapshotSchema.fieldIndex("total_files_sizes") } returns 1
        every { snapshotSchema.fieldIndex("total_records") } returns 2
        every { snapshotSchema.fieldIndex("total_data_files") } returns 3
        every { snapshotRow.getTimestamp(0) } returns Timestamp(1700000000000L)
        every { snapshotRow.get(1) } returns 1024L
        every { snapshotRow.getLong(1) } returns 1024L
        every { snapshotRow.get(2) } returns 100L
        every { snapshotRow.getLong(2) } returns 100L
        every { snapshotRow.get(3) } returns 5L
        every { snapshotRow.getLong(3) } returns 5L

        val snapshotDataset = mockk<Dataset<Row>>()
        val dataFilesDataset = mockk<Dataset<Row>>()

        every { mockSpark.sql(match { it.contains("snapshots") }) } returns snapshotDataset
        every { snapshotDataset.collectAsList() } returns listOf(snapshotRow)
        every { mockSpark.sql(match { it.contains("all_data_files") }) } returns dataFilesDataset
        every { dataFilesDataset.collectAsList() } returns emptyList()

        val result = extractor.extractTableStatistics()

        assertNull(result)
    }

    @Test
    fun `extractColumnTags should delegate to ColumnTagExtractor`() {
        val columns = listOf("col1", "col2")
        val expectedTags = mapOf("col1" to listOf("DETECTED_PERSON"), "col2" to emptyList())
        every { mockColumnTagExtractor.extract(any(), columns) } returns expectedTags

        val result = extractor.extractColumnTags(columns)

        assertEquals(expectedTags, result)
        verify { mockColumnTagExtractor.extract("`test_catalog`.`test_schema`.`test_table`", columns) }
    }
}
