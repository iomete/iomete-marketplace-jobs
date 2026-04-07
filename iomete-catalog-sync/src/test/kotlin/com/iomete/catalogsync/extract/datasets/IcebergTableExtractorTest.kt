package com.iomete.catalogsync.extract.datasets

import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
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
     * Helper: sets up the single snapshot query that extractTableStatistics uses.
     * All snapshots are fetched in one query ordered by committed_at ASC.
     */
    private fun setupSnapshotQuery(rows: List<Row>) {
        val dataset = mockk<Dataset<Row>>()
        every {
            mockSparkSession.sql(match<String> { it.contains(".snapshots") })
        } returns dataset
        every { dataset.collectAsList() } returns rows
    }

    @Test
    fun `getTableType always returns MANAGED`() {
        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertEquals("MANAGED", extractor.getTableType)
    }

    @Test
    fun `extractTableStatistics short-circuits when currentSnapshotId is none`() {
        val extractor = IcebergTableExtractor(
            mockSparkSession, "cat", "sch", "tbl",
            currentSnapshotId = "none"
        )
        val stats = extractor.extractTableStatistics()

        assertNull(stats)
        // Verify no Spark SQL was executed at all
        verify(exactly = 0) { mockSparkSession.sql(any<String>()) }
    }

    @Test
    fun `extractTableStatistics queries spark when currentSnapshotId is null`() {
        setupSnapshotQuery(emptyList())

        val extractor = IcebergTableExtractor(
            mockSparkSession, "cat", "sch", "tbl",
            currentSnapshotId = null
        )
        val stats = extractor.extractTableStatistics()

        assertNull(stats)
        // Should have executed the query even though result is empty
        verify(exactly = 1) { mockSparkSession.sql(any<String>()) }
    }

    @Test
    fun `extractTableStatistics queries spark when currentSnapshotId is a valid id`() {
        setupSnapshotQuery(emptyList())

        val extractor = IcebergTableExtractor(
            mockSparkSession, "cat", "sch", "tbl",
            currentSnapshotId = "1234567890"
        )
        val stats = extractor.extractTableStatistics()

        assertNull(stats)
        verify(exactly = 1) { mockSparkSession.sql(any<String>()) }
    }

    @Test
    fun `extractTableStatistics returns correct stats with single snapshot`() {
        val commitTime = Timestamp.from(Instant.parse("2025-01-15T10:30:00Z"))

        val snapshotRow = mockRow(
            mapOf(
                "snapshot_id" to 1L,
                "committed_at" to commitTime,
                "total_files_size" to 1024L,
                "total_records" to 500L,
                "total_data_files" to 3L,
                "added_data_files" to 3L,
                "added_files_size" to 1024L,
            )
        )

        setupSnapshotQuery(listOf(snapshotRow))

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(commitTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(3L, stats.numFiles)
        assertEquals(3L, stats.totalTableNumFiles) // only first snapshot, no rest
        assertEquals(1024L, stats.sizeInBytes)
        assertEquals(1024L, stats.totalTableSizeInBytes)
        assertEquals(500L, stats.totalRecords)
    }

    @Test
    fun `extractTableStatistics returns correct stats with multiple snapshots`() {
        val firstCommitTime = Timestamp.from(Instant.parse("2025-01-10T10:00:00Z"))
        val lastCommitTime = Timestamp.from(Instant.parse("2025-01-15T10:30:00Z"))

        val firstRow = mockRow(
            mapOf(
                "snapshot_id" to 1L,
                "committed_at" to firstCommitTime,
                "total_files_size" to 512L,
                "total_records" to 200L,
                "total_data_files" to 2L,
                "added_data_files" to 2L,
                "added_files_size" to 512L,
            )
        )

        val secondRow = mockRow(
            mapOf(
                "snapshot_id" to 2L,
                "committed_at" to lastCommitTime,
                "total_files_size" to 1024L,
                "total_records" to 500L,
                "total_data_files" to 5L,
                "added_data_files" to 3L,
                "added_files_size" to 512L,
            )
        )

        setupSnapshotQuery(listOf(firstRow, secondRow))

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(lastCommitTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(5L, stats.numFiles) // from last snapshot's total_data_files
        assertEquals(5L, stats.totalTableNumFiles) // first(2) + rest(3)
        assertEquals(1024L, stats.sizeInBytes) // from last snapshot
        assertEquals(1024L, stats.totalTableSizeInBytes) // first(512) + rest(512)
        assertEquals(500L, stats.totalRecords) // from last snapshot
    }

    @Test
    fun `extractTableStatistics returns null when query returns empty`() {
        setupSnapshotQuery(emptyList())

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `extractTableStatistics throws when snapshots query throws`() {
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

        val snapshotRow = mockRow(
            mapOf(
                "snapshot_id" to 1L,
                "committed_at" to commitTime,
                "total_files_size" to null,
                "total_records" to null,
                "total_data_files" to 2L,
                "added_data_files" to 2L,
                "added_files_size" to null,
            )
        )

        setupSnapshotQuery(listOf(snapshotRow))

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
}
