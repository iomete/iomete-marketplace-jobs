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

    private fun snapshotRow(
        snapshotId: Long,
        committedAt: Timestamp,
        totalFilesSizes: Long?,
        totalRecords: Long?,
        totalDataFiles: Long?,
        addedDataFiles: Long?,
        addedFilesSize: Long?,
    ): Row =
        mockRow(
            mapOf(
                "snapshot_id" to snapshotId,
                "committed_at" to committedAt,
                "total_files_sizes" to totalFilesSizes,
                "total_records" to totalRecords,
                "total_data_files" to totalDataFiles,
                "added_data_files" to addedDataFiles,
                "added_files_size" to addedFilesSize,
            ),
        )

    private fun setupSnapshotsQuery(rows: List<Row>) {
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
    fun `extractTableStatistics returns correct stats when data exists`() {
        val firstTime = Timestamp.from(Instant.parse("2025-01-01T00:00:00Z"))
        val lastTime = Timestamp.from(Instant.parse("2025-01-15T10:30:00Z"))

        setupSnapshotsQuery(
            listOf(
                snapshotRow(1L, firstTime, 512L, 100L, 4L, 4L, 512L),
                snapshotRow(2L, lastTime, 1024L, 500L, 3L, 6L, 1536L),
            ),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(lastTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(3L, stats.numFiles)
        assertEquals(10L, stats.totalTableNumFiles) // first total_data_files(4) + rest added_data_files(6)
        assertEquals(1024L, stats.sizeInBytes)
        assertEquals(2048L, stats.totalTableSizeInBytes) // first total_files_sizes(512) + rest added_files_size(1536)
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
                snapshotRow(1L, commitTime, null, null, 2L, null, null),
            ),
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
            mockSparkSession.sql(
                match<String> {
                    it.contains("`my-catalog`.`my-schema`.`my-table`.snapshots")
                },
            )
        } returns snapshotsDataset
        every { snapshotsDataset.collectAsList() } returns emptyList()

        val extractor = IcebergTableExtractor(mockSparkSession, "my-catalog", "my-schema", "my-table")
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `extractTableStatistics calculates totalTableNumFiles from first plus rest snapshots`() {
        val t1 = Timestamp.from(Instant.parse("2025-01-01T00:00:00Z"))
        val t2 = Timestamp.from(Instant.parse("2025-01-02T00:00:00Z"))
        val t3 = Timestamp.from(Instant.parse("2025-01-03T00:00:00Z"))

        setupSnapshotsQuery(
            listOf(
                snapshotRow(42L, t1, 200L, 10L, 2L, 2L, 200L),
                snapshotRow(43L, t2, 400L, 30L, 4L, 1L, 100L),
                snapshotRow(44L, t3, 500L, 50L, 5L, 2L, 200L),
            ),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(5L, stats!!.numFiles)
        assertEquals(5L, stats.totalTableNumFiles) // first total_data_files(2) + rest added(1+2)
        assertEquals(500L, stats.sizeInBytes)
        assertEquals(500L, stats.totalTableSizeInBytes) // first total_files_sizes(200) + rest added(100+200)
    }

    @Test
    fun `extractTableStatistics handles single snapshot correctly`() {
        val commitTime = Timestamp.from(Instant.parse("2025-03-01T12:00:00Z"))

        setupSnapshotsQuery(
            listOf(
                snapshotRow(1L, commitTime, 256L, 25L, 1L, 1L, 256L),
            ),
        )

        val extractor = IcebergTableExtractor(mockSparkSession, "cat", "sch", "tbl")
        val stats = extractor.extractTableStatistics()

        assertNotNull(stats)
        assertEquals(commitTime.toInstant().toEpochMilli(), stats!!.lastModified)
        assertEquals(1L, stats.numFiles)
        assertEquals(256L, stats.sizeInBytes)
        assertEquals(256L, stats.totalTableSizeInBytes) // only first snapshot, no rest
        assertEquals(1L, stats.totalTableNumFiles)
        assertEquals(25L, stats.totalRecords)
    }
}
