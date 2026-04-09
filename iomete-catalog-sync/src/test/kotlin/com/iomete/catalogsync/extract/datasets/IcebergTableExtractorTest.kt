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
     * Helper: sets up the snapshots aggregation query that extractTableStatistics uses.
     * The implementation runs a single aggregation query returning one row.
     */
    private fun setupSnapshotsQuery(row: Row) {
        val dataset = mockk<Dataset<Row>>()
        every {
            mockSparkSession.sql(match<String> { it.contains(".snapshots") })
        } returns dataset
        every { dataset.first() } returns row
    }

    /**
     * Builds a mock Row representing the aggregated result of the snapshots query.
     */
    private fun aggregatedRow(
        lastCommittedAt: Timestamp?,
        lastTotalDataFiles: Long?,
        lastTotalFilesSize: Long?,
        lastTotalRecords: Long?,
        firstTotalDataFiles: Long?,
        firstTotalFilesSize: Long?,
        totalAddedDataFiles: Long?,
        totalAddedFilesSize: Long?,
        firstAddedDataFiles: Long?,
        firstAddedFilesSize: Long?,
    ): Row = mockRow(
        mapOf(
            "last_committed_at" to lastCommittedAt,
            "last_total_data_files" to lastTotalDataFiles,
            "last_total_files_size" to lastTotalFilesSize,
            "last_total_records" to lastTotalRecords,
            "first_total_data_files" to firstTotalDataFiles,
            "first_total_files_size" to firstTotalFilesSize,
            "total_added_data_files" to totalAddedDataFiles,
            "total_added_files_size" to totalAddedFilesSize,
            "first_added_data_files" to firstAddedDataFiles,
            "first_added_files_size" to firstAddedFilesSize,
        )
    )

    @Test
    fun `getTableType always returns MANAGED`() {
        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertEquals("MANAGED", extractor.getTableType)
    }

    @Test
    fun `extractTableStatistics returns correct stats when data exists`() {
        val lastTime = Timestamp.from(Instant.parse("2025-01-15T10:30:00Z"))

        // Two snapshots: first(totalDataFiles=3, totalFilesSize=500, addedDataFiles=3, addedFilesSize=500)
        //                last (totalDataFiles=5, totalFilesSize=1024, addedDataFiles=2, addedFilesSize=524)
        // totalAddedDataFiles = 3+2=5, totalAddedFilesSize = 500+524=1024
        // firstAddedDataFiles=3, firstAddedFilesSize=500
        setupSnapshotsQuery(
            aggregatedRow(
                lastCommittedAt = lastTime,
                lastTotalDataFiles = 5L,
                lastTotalFilesSize = 1024L,
                lastTotalRecords = 500L,
                firstTotalDataFiles = 3L,
                firstTotalFilesSize = 500L,
                totalAddedDataFiles = 5L,
                totalAddedFilesSize = 1024L,
                firstAddedDataFiles = 3L,
                firstAddedFilesSize = 500L,
            )
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(lastTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(5L, stats.numFiles)
        // totalTableNumFiles = first_total_data_files(3) + (total_added(5) - first_added(3)) = 5
        assertEquals(5L, stats.totalTableNumFiles)
        assertEquals(1024L, stats.sizeInBytes)
        // totalTableSizeInBytes = first_total_files_size(500) + (total_added(1024) - first_added(500)) = 1024
        assertEquals(1024L, stats.totalTableSizeInBytes)
        assertEquals(500L, stats.totalRecords)
    }

    @Test
    fun `extractTableStatistics returns null when snapshots query returns empty`() {
        // Aggregation on empty table returns one row of all nulls
        setupSnapshotsQuery(
            aggregatedRow(
                lastCommittedAt = null,
                lastTotalDataFiles = null,
                lastTotalFilesSize = null,
                lastTotalRecords = null,
                firstTotalDataFiles = null,
                firstTotalFilesSize = null,
                totalAddedDataFiles = 0L,
                totalAddedFilesSize = 0L,
                firstAddedDataFiles = 0L,
                firstAddedFilesSize = 0L,
            )
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

        // Single snapshot with null totalFilesSize, totalRecords, addedDataFiles, addedFilesSize
        // COALESCE in SQL makes added values 0
        setupSnapshotsQuery(
            aggregatedRow(
                lastCommittedAt = commitTime,
                lastTotalDataFiles = 2L,
                lastTotalFilesSize = null,
                lastTotalRecords = null,
                firstTotalDataFiles = 2L,
                firstTotalFilesSize = null,
                totalAddedDataFiles = 0L,
                totalAddedFilesSize = 0L,
                firstAddedDataFiles = 0L,
                firstAddedFilesSize = 0L,
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

        // Return all-null aggregated row to simulate empty table
        val emptyRow = aggregatedRow(
            lastCommittedAt = null,
            lastTotalDataFiles = null,
            lastTotalFilesSize = null,
            lastTotalRecords = null,
            firstTotalDataFiles = null,
            firstTotalFilesSize = null,
            totalAddedDataFiles = 0L,
            totalAddedFilesSize = 0L,
            firstAddedDataFiles = 0L,
            firstAddedFilesSize = 0L,
        )
        every { snapshotsDataset.first() } returns emptyRow

        val extractor = IcebergTableExtractor(mockSparkSession, "my-catalog", "my-schema", "my-table")
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `extractTableStatistics computes totalTableNumFiles from all snapshots`() {
        val thirdTime = Timestamp.from(Instant.parse("2025-01-03T00:00:00Z"))

        // Three snapshots:
        //   first:  totalDataFiles=2, totalFilesSize=200, addedDataFiles=2, addedFilesSize=200
        //   second: totalDataFiles=5, totalFilesSize=500, addedDataFiles=3, addedFilesSize=300
        //   third:  totalDataFiles=7, totalFilesSize=800, addedDataFiles=4, addedFilesSize=500
        // totalAddedDataFiles = 2+3+4=9, totalAddedFilesSize = 200+300+500=1000
        // firstAddedDataFiles=2, firstAddedFilesSize=200
        setupSnapshotsQuery(
            aggregatedRow(
                lastCommittedAt = thirdTime,
                lastTotalDataFiles = 7L,
                lastTotalFilesSize = 800L,
                lastTotalRecords = 50L,
                firstTotalDataFiles = 2L,
                firstTotalFilesSize = 200L,
                totalAddedDataFiles = 9L,
                totalAddedFilesSize = 1000L,
                firstAddedDataFiles = 2L,
                firstAddedFilesSize = 200L,
            )
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(7L, stats!!.numFiles)
        // totalTableNumFiles = first_total(2) + (total_added(9) - first_added(2)) = 9
        assertEquals(9L, stats.totalTableNumFiles)
        // totalTableSizeInBytes = first_total(200) + (total_added(1000) - first_added(200)) = 1000
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

        // Single snapshot: totalDataFiles=5, totalFilesSize=500, addedDataFiles=5, addedFilesSize=500
        // totalAddedDataFiles=5, totalAddedFilesSize=500
        // firstAddedDataFiles=5, firstAddedFilesSize=500
        setupSnapshotsQuery(
            aggregatedRow(
                lastCommittedAt = commitTime,
                lastTotalDataFiles = 5L,
                lastTotalFilesSize = 500L,
                lastTotalRecords = 50L,
                firstTotalDataFiles = 5L,
                firstTotalFilesSize = 500L,
                totalAddedDataFiles = 5L,
                totalAddedFilesSize = 500L,
                firstAddedDataFiles = 5L,
                firstAddedFilesSize = 500L,
            )
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(5L, stats!!.numFiles)
        // totalTableNumFiles = first_total(5) + (total_added(5) - first_added(5)) = 5
        assertEquals(5L, stats.totalTableNumFiles)
        assertEquals(500L, stats.sizeInBytes)
        assertEquals(500L, stats.totalTableSizeInBytes)
        assertEquals(50L, stats.totalRecords)
    }
}
