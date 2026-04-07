package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.SupportTableStatistics
import com.iomete.catalogsync.extract.TableExtractor
import com.iomete.catalogsync.extract.TableStatistics
import com.iomete.catalogsync.extract.getLong
import com.iomete.catalogsync.extract.getTimestamp
import org.apache.spark.sql.SparkSession

class IcebergTableExtractor(
    private val spark: SparkSession,
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

    /**
     * Returns true if the table metadata indicates no snapshots exist,
     * allowing us to skip the Spark SQL query entirely.
     */
    private fun hasNoSnapshots(): Boolean {
        return currentSnapshotId != null && currentSnapshotId == "none"
    }

    override fun extractTableStatistics(): TableStatistics? {
        if (hasNoSnapshots()) return null

        val allSnapshots =
            spark
                .sql(
                    """
                    select
                        snapshot_id,
                        committed_at,
                        cast(summary['total-files-size'] as long) as total_files_size,
                        cast(summary['total-records'] as long) as total_records,
                        cast(summary['total-data-files'] as long) as total_data_files,
                        cast(summary['added-data-files'] as long) as added_data_files,
                        cast(summary['added-files-size'] as long) as added_files_size
                    from $fullName.snapshots
                    order by committed_at asc
                    """.trimIndent(),
                ).collectAsList()

        if (allSnapshots.isEmpty()) return null

        val firstSnapshot = allSnapshots.first()
        val lastSnapshot = allSnapshots.last()

        var restNumFiles = 0L
        var restSizeInBytes = 0L
        for (i in 1 until allSnapshots.size) {
            restNumFiles += allSnapshots[i].getLong("added_data_files") ?: 0L
            restSizeInBytes += allSnapshots[i].getLong("added_files_size") ?: 0L
        }

        return TableStatistics(
            lastModified = lastSnapshot.getTimestamp("committed_at"),
            numFiles = lastSnapshot.getLong("total_data_files"),
            totalTableNumFiles = (firstSnapshot.getLong("total_data_files") ?: 0L) + restNumFiles,
            sizeInBytes = lastSnapshot.getLong("total_files_size"),
            totalTableSizeInBytes = (firstSnapshot.getLong("total_files_size") ?: 0L) + restSizeInBytes,
            totalRecords = lastSnapshot.getLong("total_records"),
        )
    }
}
