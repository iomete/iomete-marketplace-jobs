package com.iomete.catalogsync.extract

data class TableStatistics(
    val lastModified: Long? = null,
    val numFiles: Long? = null,
    val sizeInBytes: Long? = null,
    val totalRecords: Long? = null
)

data class ColumnStat(
    val name: String,
    val statValue: String
)