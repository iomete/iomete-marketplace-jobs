package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.mockRow
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
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

    /**
     * Helper: sets up the snapshots query that extractTableStatistics uses.
     * The implementation runs a single query on $fullName.snapshots ordered by committed_at asc.
     */
    private fun setupSnapshotsQuery(rows: List<Row>) {
        val dataset = mockk<Dataset<Row>>()
        every {
            mockSparkSession.sql(match<String> { it.contains(".snapshots") })
        } returns dataset
        every { dataset.collectAsList() } returns rows
    }

    private fun snapshotRow(
        committedAt: Timestamp,
        totalFilesSize: Long?,
        totalRecords: Long?,
        totalDataFiles: Long?,
        addedDataFiles: Long? = null,
        addedFilesSize: Long? = null,
    ): Row = mockRow(
        mapOf(
            "committed_at" to committedAt,
            "total_files_size" to totalFilesSize,
            "total_records" to totalRecords,
            "total_data_files" to totalDataFiles,
            "added_data_files" to addedDataFiles,
            "added_files_size" to addedFilesSize,
        )
    )

    @Test
    fun `getTableType always returns MANAGED`() {
        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertEquals("MANAGED", extractor.getTableType)
    }

    @Test
    fun `extractTableStatistics returns correct stats when data exists`() {
        val firstTime = Timestamp.from(Instant.parse("2025-01-10T00:00:00Z"))
        val lastTime = Timestamp.from(Instant.parse("2025-01-15T10:30:00Z"))

        setupSnapshotsQuery(
            listOf(
                snapshotRow(
                    committedAt = firstTime,
                    totalFilesSize = 500L,
                    totalRecords = 200L,
                    totalDataFiles = 3L,
                    addedDataFiles = 3L,
                    addedFilesSize = 500L,
                ),
                snapshotRow(
                    committedAt = lastTime,
                    totalFilesSize = 1024L,
                    totalRecords = 500L,
                    totalDataFiles = 5L,
                    addedDataFiles = 2L,
                    addedFilesSize = 524L,
                ),
            )
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(lastTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(5L, stats.numFiles)
        // totalTableNumFiles = first.total_data_files + sum of rest's added_data_files = 3 + 2 = 5
        assertEquals(5L, stats.totalTableNumFiles)
        assertEquals(1024L, stats.sizeInBytes)
        // totalTableSizeInBytes = first.total_files_size + sum of rest's added_files_size = 500 + 524 = 1024
        assertEquals(1024L, stats.totalTableSizeInBytes)
        assertEquals(500L, stats.totalRecords)
    }

    @Test
    fun `extractTableStatistics returns null when snapshots query returns empty`() {
        setupSnapshotsQuery(emptyList())

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

        setupSnapshotsQuery(
            listOf(
                snapshotRow(
                    committedAt = commitTime,
                    totalFilesSize = null,
                    totalRecords = null,
                    totalDataFiles = 2L,
                    addedDataFiles = null,
                    addedFilesSize = null,
                ),
            )
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
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `extractTableStatistics computes totalTableNumFiles from all snapshots`() {
        val firstTime = Timestamp.from(Instant.parse("2025-01-01T00:00:00Z"))
        val secondTime = Timestamp.from(Instant.parse("2025-01-02T00:00:00Z"))
        val thirdTime = Timestamp.from(Instant.parse("2025-01-03T00:00:00Z"))

        setupSnapshotsQuery(
            listOf(
                snapshotRow(
                    committedAt = firstTime,
                    totalFilesSize = 200L,
                    totalRecords = 10L,
                    totalDataFiles = 2L,
                    addedDataFiles = 2L,
                    addedFilesSize = 200L,
                ),
                snapshotRow(
                    committedAt = secondTime,
                    totalFilesSize = 500L,
                    totalRecords = 30L,
                    totalDataFiles = 5L,
                    addedDataFiles = 3L,
                    addedFilesSize = 300L,
                ),
                snapshotRow(
                    committedAt = thirdTime,
                    totalFilesSize = 800L,
                    totalRecords = 50L,
                    totalDataFiles = 7L,
                    addedDataFiles = 4L,
                    addedFilesSize = 500L,
                ),
            )
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(7L, stats!!.numFiles)
        // totalTableNumFiles = first.total_data_files(2) + second.added_data_files(3) + third.added_data_files(4) = 9
        assertEquals(9L, stats.totalTableNumFiles)
        // totalTableSizeInBytes = first.total_files_size(200) + second.added_files_size(300) + third.added_files_size(500) = 1000
        assertEquals(1000L, stats.totalTableSizeInBytes)
        assertEquals(800L, stats.sizeInBytes)
        assertEquals(50L, stats.totalRecords)
    }

    @Test
    fun `extractTableStatistics returns null without querying when currentSnapshotId is none`() {
        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl", currentSnapshotId = "none")
        val stats = extractor.extractTableStatistics()

        assertNull(stats)
        verify(exactly = 0) { mockSparkSession.sql(any<String>()) }
    }

    @Test
    fun `extractTableStatistics with single snapshot uses first as both first and last`() {
        val commitTime = Timestamp.from(Instant.now())

        setupSnapshotsQuery(
            listOf(
                snapshotRow(
                    committedAt = commitTime,
                    totalFilesSize = 500L,
                    totalRecords = 50L,
                    totalDataFiles = 5L,
                    addedDataFiles = 5L,
                    addedFilesSize = 500L,
                ),
            )
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(5L, stats!!.numFiles)
        // Single snapshot: totalTableNumFiles = first.total_data_files(5) + no rest = 5
        assertEquals(5L, stats.totalTableNumFiles)
        assertEquals(500L, stats.sizeInBytes)
        assertEquals(500L, stats.totalTableSizeInBytes)
        assertEquals(50L, stats.totalRecords)
    }
}