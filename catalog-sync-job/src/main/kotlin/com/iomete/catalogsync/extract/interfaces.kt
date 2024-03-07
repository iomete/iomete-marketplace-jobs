package com.iomete.catalogsync.extract

interface TableExtractor {
    val getTableType: String
}

interface SupportTableStatistics {
    fun extractTableStatistics(): TableStatistics?
}

interface SupportColumnTags {
    fun extractColumnTags(columns: List<String>): Map<String, List<String>>
}

interface SupportColumnStatistics {
    fun extractColumnStatistics(columns: List<String>): Map<String, List<ColumnStat>>
}

