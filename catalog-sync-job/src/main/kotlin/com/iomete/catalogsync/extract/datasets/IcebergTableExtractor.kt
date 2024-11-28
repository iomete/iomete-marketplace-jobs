package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.SupportTableStatistics
import com.iomete.catalogsync.extract.TableExtractor
import com.iomete.catalogsync.extract.TableStatistics
import com.iomete.catalogsync.extract.utils.ColumnTagExtractor
import com.iomete.catalogsync.getLong
import com.iomete.catalogsync.getTimestamp
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

class IcebergTableExtractor(
    private val spark: SparkSession,
    private val columnTagExtractor: ColumnTagExtractor,
    schema: String,
    table: String
) : TableExtractor, SupportTableStatistics, SupportColumnTags {
    private val fullName = "$schema.$table"

    override val getTableType: String
        get() = "MANAGED"

    override fun extractTableStatistics(): TableStatistics? {
        // spark.sql("REFRESH TABLE $fullName")
        val lastSnapshot = spark.sql(
            """
                select 
                   committed_at, 
                   cast(summary['total-files-size'] as long) as total_files_sizes, 
                   cast(summary['total-records'] as long) as total_records,
                   cast(summary['total-data-files'] as long) as total_data_files
                from $fullName.snapshots
                order by committed_at desc limit 1
            """.trimIndent()
        ).collectAsList().firstOrNull() ?: return null

        return TableStatistics(
            lastModified = lastSnapshot.getTimestamp("committed_at"),
            numFiles = lastSnapshot.getLong("total_data_files"),
            sizeInBytes = lastSnapshot.getLong("total_files_sizes"),
            totalRecords = lastSnapshot.getLong("total_records")
        )
    }

    override fun extractColumnTags(columns: List<String>): Map<String, List<String>> =
        columnTagExtractor.extract(fullName, columns)

    companion object {
        private val logger = LoggerFactory.getLogger(IcebergTableExtractor::class.java)
    }
}
