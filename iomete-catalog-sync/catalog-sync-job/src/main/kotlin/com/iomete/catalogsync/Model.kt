package com.iomete.catalogsync

import com.iomete.catalogsync.extract.ColumnStat

data class CatalogMetadata(
    val catalog: String,
    val type: Set<String>, // ex. ["INTERNAL", "ICEBERG"] or ["EXTERNAL", "JDBC"]
    val location: String?,
    val storageEndpoint: String?,
    val totalSchemaCount: Int,
    val totalTableCount: Int,
    val totalSizeInBytes: Long,
    val totalFiles: Long,
    val domainsAllowed: Set<String> = setOf(),
    val discoveryFailed: Boolean = false,
)

data class SchemaMetadata(
    val catalog: String,
    val schema: String,
    val totalTableCount: Int,
    val totalViewCount: Int,
    val totalSizeInBytes: Long,
    val totalDbSizeInBytes: Long,
    val totalFiles: Long,
    val failedTableCount: Int,
    val syncFailedCount: Int = 0,
    val discoveryFailed: Boolean = false,
)

data class TableMetadata(
    val catalog: String,
    val schema: String,
    val name: String,
    val description: String?,
    val tableType: String, // MANAGED, EXTERNAL, VIEW
    val isView: Boolean,
    val isTemporary: Boolean,

    val owner: String,
    // Managed providers: hive,delta
    // External providers: org.apache.spark.sql.json,com.databricks.spark.csv,...
    val provider: String?,
    val viewText: String?,
    val createdAt: Long? = null,

    val lastModified: Long? = null,
    val numFiles: Long? = null,
    val totalTableNumFiles : Long? = null,
    val sizeInBytes: Long? = null,
    val totalTableSizeInBytes: Long? = null,
    val totalRecords: Long? = null,

    val columns: List<ColumnMetadata>,
    val tags: List<String> = listOf(),

    val syncTime: Long
)

data class ColumnMetadata(
    val name: String,
    val dataType: String,
    val description: String?,
    val sortOrder: Int,
    val isPartitionKey: Boolean,
    val stats: List<ColumnStat> = listOf(),
    val tags: List<String> = listOf()
)

enum class TableColumnSection {
    COLUMNS,
    PARTITIONS,
    METADATA,
    TABLE_INFO,
    VIEW_INFO
}
