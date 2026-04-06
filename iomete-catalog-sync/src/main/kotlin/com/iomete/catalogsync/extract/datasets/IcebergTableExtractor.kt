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
        val allSnapshots =
            spark
                .sql(
                    """
                    select
                        snapshot_id,
                        committed_at,
                        cast(summary['total-files-size'] as long) as total_files_sizes,
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

        val firstNumFiles = firstSnapshot.getLong("total_data_files") ?: 0L
        val firstSizeInBytes = firstSnapshot.getLong("total_files_sizes") ?: 0L

        val restSnapshots = if (allSnapshots.size > 1) allSnapshots.subList(1, allSnapshots.size) else emptyList()
        val restNumFiles = restSnapshots.sumOf { it.getLong("added_data_files") ?: 0L }
        val restSizeInBytes = restSnapshots.sumOf { it.getLong("added_files_size") ?: 0L }

        return TableStatistics(
            lastModified = lastSnapshot.getTimestamp("committed_at"),
            numFiles = lastSnapshot.getLong("total_data_files"),
            totalTableNumFiles = firstNumFiles + restNumFiles,
            sizeInBytes = lastSnapshot.getLong("total_files_sizes"),
            totalTableSizeInBytes = firstSizeInBytes + restSizeInBytes,
            totalRecords = lastSnapshot.getLong("total_records"),
        )
    }
}
