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

        val allDataFiles =
            spark
                .sql(
                    """
                    select
                        count(*) as total_table_num_files,
                        sum(file_size_in_bytes) as total_table_size_in_bytes
                    from $fullName.all_data_files
                    """.trimIndent(),
                ).collectAsList()
                .firstOrNull() ?: return null

        return TableStatistics(
            lastModified = lastSnapshot.getTimestamp("committed_at"),
            numFiles = lastSnapshot.getLong("total_data_files"),
            totalTableNumFiles = allDataFiles.getLong("total_table_num_files"),
            sizeInBytes = lastSnapshot.getLong("total_files_sizes"),
            totalTableSizeInBytes = allDataFiles.getLong("total_table_size_in_bytes"),
            totalRecords = lastSnapshot.getLong("total_records"),
        )
    }
}
