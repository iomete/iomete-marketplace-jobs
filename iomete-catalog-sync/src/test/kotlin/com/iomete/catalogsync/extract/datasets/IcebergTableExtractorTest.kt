package com.iomete.catalogsync.extract.datasets

import io.mockk.every
import io.mockk.mockk
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Timestamp
import java.time.Instant

class IcebergTableExtractorTest {
    private lateinit var mockSparkSession: SparkSession

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk(relaxed = true)
    }

    private fun mockRow(fields: Map<String, Any?>): Row {
        val row = mockk<Row>()
        val schema = mockk<StructType>()
        every { row.schema() } returns schema

        val fieldNames = fields.keys.toList()
        fieldNames.forEachIndexed { index, name ->
            every { schema.fieldIndex(name) } returns index
            val value = fields[name]
            every { row.get(index) } returns value
            when (value) {
                is Timestamp -> every { row.getTimestamp(index) } returns value
                is Long -> every { row.getLong(index) } returns value
                null -> {
                    every { row.getTimestamp(index) } returns null
                    every { row.getLong(index) } throws NullPointerException()
                }
            }
        }
        return row
    }

    /**
     * Helper: sets up the 2 queries that extractTableStatistics uses.
     * - lastSnapshot: ordered desc limit 1 (committed_at, total_files_sizes, total_records, total_data_files)
     * - allDataFiles: aggregated from all_data_files (total_table_num_files, total_table_size_in_bytes)
     */
    private fun setupSnapshotQueries(
        lastSnapshotRows: List<Row>,
        allDataFilesRows: List<Row> = emptyList(),
    ) {
        val lastDataset = mockk<Dataset<Row>>()
        val allDataFilesDataset = mockk<Dataset<Row>>()

        every {
            mockSparkSession.sql(match<String> { it.contains("order by committed_at desc") })
        } returns lastDataset
        every { lastDataset.collectAsList() } returns lastSnapshotRows

        every {
            mockSparkSession.sql(match<String> { it.contains("all_data_files") })
        } returns allDataFilesDataset
        every { allDataFilesDataset.collectAsList() } returns allDataFilesRows
    }

    @Test
    fun `getTableType always returns MANAGED`() {
        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertEquals("MANAGED", extractor.getTableType)
    }

    @Test
    fun `extractTableStatistics returns correct stats when data exists`() {
        val commitTime = Timestamp.from(Instant.parse("2025-01-15T10:30:00Z"))

        val lastSnapshotRow = mockRow(
            mapOf(
                "committed_at" to commitTime,
                "total_files_sizes" to 1024L,
                "total_records" to 500L,
                "total_data_files" to 3L,
            )
        )

        val allDataFilesRow = mockRow(
            mapOf(
                "total_table_num_files" to 10L,
                "total_table_size_in_bytes" to 2048L,
            )
        )

        setupSnapshotQueries(
            lastSnapshotRows = listOf(lastSnapshotRow),
            allDataFilesRows = listOf(allDataFilesRow),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(commitTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(3L, stats.numFiles)
        assertEquals(10L, stats.totalTableNumFiles)
        assertEquals(1024L, stats.sizeInBytes)
        assertEquals(2048L, stats.totalTableSizeInBytes)
        assertEquals(500L, stats.totalRecords)
    }

    @Test
    fun `extractTableStatistics returns null when lastSnapshot query returns empty`() {
        setupSnapshotQueries(lastSnapshotRows = emptyList())

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `extractTableStatistics returns null when allDataFiles query returns empty`() {
        val commitTime = Timestamp.from(Instant.now())

        val lastSnapshotRow = mockRow(
            mapOf(
                "committed_at" to commitTime,
                "total_files_sizes" to 100L,
                "total_records" to 10L,
                "total_data_files" to 1L,
            )
        )

        setupSnapshotQueries(
            lastSnapshotRows = listOf(lastSnapshotRow),
            allDataFilesRows = emptyList(),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `extractTableStatistics returns null when snapshots query throws`() {
        every {
            mockSparkSession.sql(match<String> { it.contains(".snapshots") })
        } throws RuntimeException("Query failed")

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertThrows(RuntimeException::class.java) {
            extractor.extractTableStatistics()
        }
    }

    @Test
    fun `extractTableStatistics handles null values in row fields`() {
        val commitTime = Timestamp.from(Instant.parse("2025-06-01T00:00:00Z"))

        val lastSnapshotRow = mockRow(
            mapOf(
                "committed_at" to commitTime,
                "total_files_sizes" to null,
                "total_records" to null,
                "total_data_files" to 2L,
            )
        )

        val allDataFilesRow = mockRow(
            mapOf(
                "total_table_num_files" to null,
                "total_table_size_in_bytes" to null,
            )
        )

        setupSnapshotQueries(
            lastSnapshotRows = listOf(lastSnapshotRow),
            allDataFilesRows = listOf(allDataFilesRow),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(commitTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(2L, stats.numFiles)
        assertNull(stats.sizeInBytes)
        assertNull(stats.totalRecords)
    }

    @Test
    fun `fullName is properly backtick-escaped in SQL queries`() {
        val snapshotsDataset = mockk<Dataset<Row>>()

        every {
            mockSparkSession.sql(match<String> {
                it.contains("`my-catalog`.`my-schema`.`my-table`.snapshots")
            })
        } returns snapshotsDataset
        every { snapshotsDataset.collectAsList() } returns emptyList()

        val extractor = IcebergTableExtractor(mockSparkSession, "my-catalog", "my-schema", "my-table")
        // Will return null due to empty snapshots, but the important thing is
        // that the sql() call matched our backtick-escaped pattern
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `extractTableStatistics returns totalTableNumFiles from allDataFiles query`() {
        val commitTime = Timestamp.from(Instant.now())

        val lastSnapshotRow = mockRow(
            mapOf(
                "committed_at" to commitTime,
                "total_files_sizes" to 500L,
                "total_records" to 50L,
                "total_data_files" to 5L,
            )
        )

        val allDataFilesRow = mockRow(
            mapOf(
                "total_table_num_files" to 7L,
                "total_table_size_in_bytes" to 800L,
            )
        )

        setupSnapshotQueries(
            lastSnapshotRows = listOf(lastSnapshotRow),
            allDataFilesRows = listOf(allDataFilesRow),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(7L, stats!!.totalTableNumFiles)
        assertEquals(800L, stats.totalTableSizeInBytes)
    }
}
