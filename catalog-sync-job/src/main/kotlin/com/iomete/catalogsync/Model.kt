package com.iomete.catalogsync

import com.iomete.catalogsync.extract.ColumnStat

data class CatalogMetadata(
    val catalog: String,
    val totalSchemaCount: Int,
    val totalTableCount: Int,
    val totalSizeInBytes: Long,
    val totalFiles: Long
)

data class SchemaMetadata(
    val catalog: String,
    val schema: String,
    val totalTableCount: Int,
    val totalViewCount: Int,
    val totalSizeInBytes: Long,
    val totalFiles: Long,
    val failedTableCount: Int,
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
    val sizeInBytes: Long? = null,
    val totalRecords: Long? = null,

    val columns: List<ColumnMetadata>,
    var tags: List<String> = listOf(),

    val syncTime: Long
)

data class ColumnMetadata(
    val name: String,
    val dataType: String,
    val description: String?,
    val sortOrder: Int,
    var isPartitionKey: Boolean,
    var stats: List<ColumnStat> = listOf(),
    var tags: List<String> = listOf()
)
