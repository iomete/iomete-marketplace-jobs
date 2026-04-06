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
     * Helper: sets up the 3 snapshot queries that extractTableStatistics uses.
     * - lastSnapshot: ordered desc limit 1 (committed_at, total_files_sizes, total_records, total_data_files)
     * - firstSnapshot: ordered asc limit 1 (snapshot_id, num_files, size_in_bytes)
     * - restSnapshots: aggregated where snapshot_id != firstSnapshotId (num_files, size_in_bytes)
     */
    private fun setupSnapshotQueries(
        lastSnapshotRows: List<Row>,
        firstSnapshotRows: List<Row> = emptyList(),
        restSnapshotRows: List<Row> = emptyList(),
    ) {
        // The source code issues 3 SQL calls, all containing `.snapshots`.
        // We differentiate by matching on "order by committed_at desc" vs "order by committed_at asc" vs "snapshot_id !="
        val lastDataset = mockk<Dataset<Row>>()
        val firstDataset = mockk<Dataset<Row>>()
        val restDataset = mockk<Dataset<Row>>()

        every {
            mockSparkSession.sql(match<String> { it.contains("order by committed_at desc") })
        } returns lastDataset
        every { lastDataset.collectAsList() } returns lastSnapshotRows

        every {
            mockSparkSession.sql(match<String> { it.contains("order by committed_at asc") })
        } returns firstDataset
        every { firstDataset.collectAsList() } returns firstSnapshotRows

        every {
            mockSparkSession.sql(match<String> { it.contains("snapshot_id !=") })
        } returns restDataset
        every { restDataset.collectAsList() } returns restSnapshotRows
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

        val firstSnapshotRow = mockRow(
            mapOf(
                "snapshot_id" to 1L,
                "num_files" to 4L,
                "size_in_bytes" to 512L,
            )
        )

        val restSnapshotRow = mockRow(
            mapOf(
                "num_files" to 6L,
                "size_in_bytes" to 1536L,
            )
        )

        setupSnapshotQueries(
            lastSnapshotRows = listOf(lastSnapshotRow),
            firstSnapshotRows = listOf(firstSnapshotRow),
            restSnapshotRows = listOf(restSnapshotRow),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(commitTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(3L, stats.numFiles)
        assertEquals(10L, stats.totalTableNumFiles) // 4 + 6
        assertEquals(1024L, stats.sizeInBytes)
        assertEquals(2048L, stats.totalTableSizeInBytes) // 512 + 1536
        assertEquals(500L, stats.totalRecords)
    }

    @Test
    fun `extractTableStatistics returns null when lastSnapshot query returns empty`() {
        setupSnapshotQueries(lastSnapshotRows = emptyList())

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `extractTableStatistics returns null when firstSnapshot query returns empty`() {
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
            firstSnapshotRows = emptyList(),
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

        val firstSnapshotRow = mockRow(
            mapOf(
                "snapshot_id" to 1L,
                "num_files" to null,
                "size_in_bytes" to null,
            )
        )

        val restSnapshotRow = mockRow(
            mapOf(
                "num_files" to 0L,
                "size_in_bytes" to 0L,
            )
        )

        setupSnapshotQueries(
            lastSnapshotRows = listOf(lastSnapshotRow),
            firstSnapshotRows = listOf(firstSnapshotRow),
            restSnapshotRows = listOf(restSnapshotRow),
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
    fun `extractTableStatistics calculates totalTableNumFiles from first plus rest snapshots`() {
        val commitTime = Timestamp.from(Instant.now())

        val lastSnapshotRow = mockRow(
            mapOf(
                "committed_at" to commitTime,
                "total_files_sizes" to 500L,
                "total_records" to 50L,
                "total_data_files" to 5L,
            )
        )

        val firstSnapshotRow = mockRow(
            mapOf(
                "snapshot_id" to 42L,
                "num_files" to 2L,
                "size_in_bytes" to 200L,
            )
        )

        val restSnapshotRow = mockRow(
            mapOf(
                "num_files" to 3L,
                "size_in_bytes" to 300L,
            )
        )

        setupSnapshotQueries(
            lastSnapshotRows = listOf(lastSnapshotRow),
            firstSnapshotRows = listOf(firstSnapshotRow),
            restSnapshotRows = listOf(restSnapshotRow),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(5L, stats!!.totalTableNumFiles) // 2 + 3
        assertEquals(500L, stats.totalTableSizeInBytes) // 200 + 300
    }
}
