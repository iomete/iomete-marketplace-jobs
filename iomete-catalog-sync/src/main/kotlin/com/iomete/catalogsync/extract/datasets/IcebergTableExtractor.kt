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
) : TableExtractor,
    SupportTableStatistics,
    SupportColumnTags {
    private val fullName = "`$catalog`.`$schema`.`$table`"

    override val getTableType: String
        get() = "MANAGED"

    override fun extractTableStatistics(): TableStatistics? {
        // spark.sql("REFRESH TABLE $fullName")
        val lastSnapshot =
            spark
                .sql(
                    """
                    select 
                       committed_at, 
                       cast(summary['total-files-size'] as long) as total_files_sizes, 
                       cast(summary['total-records'] as long) as total_records,
                       cast(summary['total-data-files'] as long) as total_data_files
                    from $fullName.snapshots
                    order by committed_at desc limit 1
                    """.trimIndent(),
                ).collectAsList()
                .firstOrNull() ?: return null

        val firstSnapshot =
            spark
                .sql(
                    """
                    select
                        snapshot_id,
                        CAST(summary['total-data-files'] AS LONG) as num_files,
                        CAST(summary['total-files-size'] AS LONG) as size_in_bytes
                    from $fullName.snapshots
                    order by committed_at asc limit 1
                    """.trimIndent(),
                ).collectAsList()
                .firstOrNull() ?: return null

        val firstSnapshotId = firstSnapshot.getLong("snapshot_id")

        val restSnapshots =
            spark
                .sql(
                    """
                    select
                        COALESCE(SUM(CAST(summary['added-data-files'] AS LONG)), 0) as num_files,
                        COALESCE(SUM(CAST(summary['added-files-size'] AS LONG)), 0) as size_in_bytes
                    from $fullName.snapshots
                    where snapshot_id != $firstSnapshotId
                    """.trimIndent(),
                ).collectAsList()
                .firstOrNull()

        val restNumFiles = restSnapshots?.getLong("num_files") ?: 0L
        val restSizeInBytes = restSnapshots?.getLong("size_in_bytes") ?: 0L

        return TableStatistics(
            lastModified = lastSnapshot.getTimestamp("committed_at"),
            numFiles = lastSnapshot.getLong("total_data_files"),
            totalTableNumFiles = (firstSnapshot.getLong("num_files") ?: 0L) + restNumFiles,
            sizeInBytes = lastSnapshot.getLong("total_files_sizes"),
            totalTableSizeInBytes = (firstSnapshot.getLong("size_in_bytes") ?: 0L) + restSizeInBytes,
            totalRecords = lastSnapshot.getLong("total_records"),
        )
    }
}
