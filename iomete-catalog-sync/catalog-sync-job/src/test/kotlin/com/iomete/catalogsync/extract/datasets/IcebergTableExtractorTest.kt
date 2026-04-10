package com.iomete.catalogsync.extract.datasets

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

    private fun mockAggregationRow(
        lastCommittedAt: Timestamp?,
        lastTotalDataFiles: Long?,
        lastTotalFilesSize: Long?,
        lastTotalRecords: Long?,
        firstTotalDataFiles: Long?,
        firstTotalFilesSize: Long?,
        totalAddedDataFiles: Long?,
        totalAddedFilesSize: Long?,
        firstAddedDataFiles: Long?,
        firstAddedFilesSize: Long?
    ): Row {
        val row = mockk<Row>()
        val schema = mockk<StructType>()
        every { row.schema() } returns schema

        // Map field names to indices
        val fields = listOf(
            "last_committed_at", "last_total_data_files", "last_total_files_size",
            "last_total_records", "first_total_data_files", "first_total_files_size",
            "total_added_data_files", "total_added_files_size",
            "first_added_data_files", "first_added_files_size"
        )
        fields.forEachIndexed { index, name -> every { schema.fieldIndex(name) } returns index }

        // getTimestamp for last_committed_at (index 0)
        every { row.getTimestamp(0) } returns lastCommittedAt

        // get/getLong for each Long? field
        val longValues = listOf(
            lastTotalDataFiles, lastTotalFilesSize, lastTotalRecords,
            firstTotalDataFiles, firstTotalFilesSize,
            totalAddedDataFiles, totalAddedFilesSize,
            firstAddedDataFiles, firstAddedFilesSize
        )
        longValues.forEachIndexed { i, value ->
            val idx = i + 1
            every { row.get(idx) } returns value
            if (value != null) {
                every { row.getLong(idx) } returns value
            }
        }

        return row
    }

    @Test
    fun `extractTableStatistics should return stats from aggregation query`() {
        val row = mockAggregationRow(
            lastCommittedAt = Timestamp(1700000000000L),
            lastTotalDataFiles = 5L,
            lastTotalFilesSize = 1024L,
            lastTotalRecords = 100L,
            firstTotalDataFiles = 2L,
            firstTotalFilesSize = 512L,
            totalAddedDataFiles = 8L,
            totalAddedFilesSize = 1536L,
            firstAddedDataFiles = 3L,
            firstAddedFilesSize = 500L
        )

        val dataset = mockk<Dataset<Row>>()
        every { mockSpark.sql(match { it.contains("snapshots") }) } returns dataset
        every { dataset.first() } returns row

        val result = extractor.extractTableStatistics()

        assertNotNull(result)
        assertEquals(1700000000000L, result!!.lastModified)
        assertEquals(1024L, result.sizeInBytes)
        assertEquals(100L, result.totalRecords)
        assertEquals(5L, result.numFiles)
        // totalTableNumFiles = firstTotalDataFiles + (totalAddedDataFiles - firstAddedDataFiles) = 2 + (8 - 3) = 7
        assertEquals(7L, result.totalTableNumFiles)
        // totalTableSizeInBytes = firstTotalFilesSize + (totalAddedFilesSize - firstAddedFilesSize) = 512 + (1536 - 500) = 1548
        assertEquals(1548L, result.totalTableSizeInBytes)
    }

    @Test
    fun `extractTableStatistics should return null when no snapshots exist`() {
        // Aggregation on empty input returns one row of nulls
        val row = mockAggregationRow(
            lastCommittedAt = null,
            lastTotalDataFiles = null,
            lastTotalFilesSize = null,
            lastTotalRecords = null,
            firstTotalDataFiles = null,
            firstTotalFilesSize = null,
            totalAddedDataFiles = null,
            totalAddedFilesSize = null,
            firstAddedDataFiles = null,
            firstAddedFilesSize = null
        )

        val dataset = mockk<Dataset<Row>>()
        every { mockSpark.sql(match { it.contains("snapshots") }) } returns dataset
        every { dataset.first() } returns row

        val result = extractor.extractTableStatistics()

        assertNull(result)
    }

    @Test
    fun `extractTableStatistics should return null when currentSnapshotId is none`() {
        val extractorWithNoSnapshots = IcebergTableExtractor(
            spark = mockSpark,
            columnTagExtractor = mockColumnTagExtractor,
            catalog = "test_catalog",
            schema = "test_schema",
            table = "test_table",
            currentSnapshotId = "none"
        )

        val result = extractorWithNoSnapshots.extractTableStatistics()

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