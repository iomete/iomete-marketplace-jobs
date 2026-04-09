package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.SupportTableStatistics
import com.iomete.catalogsync.extract.TableExtractor
import com.iomete.catalogsync.extract.TableStatistics
import com.iomete.catalogsync.extract.utils.ColumnTagExtractor
import com.iomete.catalogsync.getLong
import com.iomete.catalogsync.getTimestamp
import org.apache.spark.sql.SparkSession

class IcebergTableExtractor(
    private val spark: SparkSession,
    private val columnTagExtractor: ColumnTagExtractor,
    catalog: String,
    schema: String,
    table: String,
    private val currentSnapshotId: String? = null,
) : TableExtractor,
    SupportTableStatistics,
    SupportColumnTags {
    private val fullName = "`$catalog`.`$schema`.`$table`"

    override val getTableType: String
        get() = "MANAGED"

    override fun extractColumnTags(columns: List<String>): Map<String, List<String>> =
        columnTagExtractor.extract(fullName, columns)

    /**
     * Returns true if the table metadata indicates no snapshots exist,
     * allowing us to skip the Spark SQL query entirely.
     */
    private fun hasNoSnapshots(): Boolean {
        return currentSnapshotId != null && currentSnapshotId == "none"
    }

    override fun extractTableStatistics(): TableStatistics? {
        if (hasNoSnapshots()) return null

        val row =
            spark
                .sql(
                    """
                    SELECT
                        max_by(committed_at, committed_at) AS last_committed_at,
                        max_by(total_data_files, committed_at) AS last_total_data_files,
                        max_by(total_files_size, committed_at) AS last_total_files_size,
                        max_by(total_records, committed_at) AS last_total_records,
                        min_by(total_data_files, committed_at) AS first_total_data_files,
                        min_by(total_files_size, committed_at) AS first_total_files_size,
                        COALESCE(SUM(COALESCE(added_data_files, 0)), 0) AS total_added_data_files,
                        COALESCE(SUM(COALESCE(added_files_size, 0)), 0) AS total_added_files_size,
                        COALESCE(min_by(COALESCE(added_data_files, 0), committed_at), 0) AS first_added_data_files,
                        COALESCE(min_by(COALESCE(added_files_size, 0), committed_at), 0) AS first_added_files_size
                    FROM (
                        SELECT
                            committed_at,
                            CAST(summary['total-files-size'] AS LONG) AS total_files_size,
                            CAST(summary['total-records'] AS LONG) AS total_records,
                            CAST(summary['total-data-files'] AS LONG) AS total_data_files,
                            CAST(summary['added-data-files'] AS LONG) AS added_data_files,
                            CAST(summary['added-files-size'] AS LONG) AS added_files_size
                        FROM $fullName.snapshots
                    )
                    """.trimIndent(),
                ).first()

        // Aggregation on empty input returns one row of nulls
        val lastCommittedAt = row.getTimestamp("last_committed_at") ?: return null

        val lastTotalDataFiles = row.getLong("last_total_data_files")
        val lastTotalFilesSize = row.getLong("last_total_files_size")
        val lastTotalRecords = row.getLong("last_total_records")
        val firstTotalDataFiles = row.getLong("first_total_data_files") ?: 0L
        val firstTotalFilesSize = row.getLong("first_total_files_size") ?: 0L
        val totalAddedDataFiles = row.getLong("total_added_data_files") ?: 0L
        val totalAddedFilesSize = row.getLong("total_added_files_size") ?: 0L
        val firstAddedDataFiles = row.getLong("first_added_data_files") ?: 0L
        val firstAddedFilesSize = row.getLong("first_added_files_size") ?: 0L

        return TableStatistics(
            lastModified = lastCommittedAt,
            numFiles = lastTotalDataFiles,
            totalTableNumFiles = firstTotalDataFiles + (totalAddedDataFiles - firstAddedDataFiles),
            sizeInBytes = lastTotalFilesSize,
            totalTableSizeInBytes = firstTotalFilesSize + (totalAddedFilesSize - firstAddedFilesSize),
            totalRecords = lastTotalRecords,
        )
    }
}