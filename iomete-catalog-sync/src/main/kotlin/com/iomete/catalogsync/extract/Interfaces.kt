package com.iomete.catalogsync.extract

interface TableExtractor {
    val getTableType: String
}

interface SupportTableStatistics {
    fun extractTableStatistics(): TableStatistics?
}

interface SupportColumnTags

interface SupportColumnStatistics {
    fun extractColumnStatistics(columns: List<String>): Map<String, List<ColumnStat>>
}

data class TableStatistics(
    val lastModified: Long? = null,
    val numFiles: Long? = null,
    val totalTableNumFiles: Long? = null,
    val sizeInBytes: Long? = null,
    val totalTableSizeInBytes: Long? = null,
    val totalRecords: Long? = null,
)

data class ColumnStat(
    val name: String,
    val statValue: String,
)
