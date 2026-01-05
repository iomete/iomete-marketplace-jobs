package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CoreClient.CatalogDetails
import org.slf4j.Logger
import org.slf4j.LoggerFactory

private val logger: Logger = LoggerFactory.getLogger(MetadataScraper::class.java)

data class CatalogMetadata(
    val catalog: String,
    val type: Set<String>, // ex. ["INTERNAL", "ICEBERG"] or ["EXTERNAL", "JDBC"]
    val location: String?,
    val storageEndpoint: String?,
    val totalSchemaCount: Int,
    val totalTableCount: Int,
    val totalSizeInBytes: Long,
    val totalFiles: Long,
    val sparkApplicationId: String,
) {
    companion object {
        fun build(
            catalog: CatalogDetails,
            schemas: List<SchemaMetadata>,
            sparkApplicationId: String,
        ) = CatalogMetadata(
            catalog = catalog.name,
            type = catalog.type.toSet(),
            location = catalog.location,
            storageEndpoint = catalog.storageEndpoint,
            totalSchemaCount = schemas.size,
            totalTableCount = schemas.sumOf { it.totalTableCount },
            totalSizeInBytes = schemas.sumOf { it.totalSizeInBytes },
            totalFiles = schemas.sumOf { it.totalFiles },
            sparkApplicationId = sparkApplicationId,
        )
    }
}

data class SchemaMetadata(
    val catalog: String,
    val schema: String,
    val totalTableCount: Int,
    val totalViewCount: Int,
    val totalSizeInBytes: Long,
    val totalDbSizeInBytes: Long,
    val totalFiles: Long,
    val failedTableCount: Int,
    val sparkApplicationId: String,
) {
    companion object {
        fun build(
            catalog: String,
            schema: String,
            tables: List<TableMetadata>,
            failuresSize: Int,
            sparkApplicationId: String,
        ) = SchemaMetadata(
            catalog = catalog,
            schema = schema,
            totalTableCount = tables.filterNot { it.isView }.size,
            totalViewCount = tables.filter { it.isView }.size,
            totalSizeInBytes = tables.sumOf { it.sizeInBytes ?: 0L },
            totalDbSizeInBytes = tables.sumOf { it.totalTableSizeInBytes ?: 0L },
            totalFiles = tables.sumOf { it.numFiles ?: 0L },
            failedTableCount = failuresSize,
            sparkApplicationId = sparkApplicationId,
        )
    }
}

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
    val totalTableNumFiles: Long? = null,
    val sizeInBytes: Long? = null,
    val totalTableSizeInBytes: Long? = null,
    val totalRecords: Long? = null,
    val columns: List<ColumnMetadata>,
    var tags: List<String> = listOf(),
    val syncTime: Long,
    val sparkApplicationId: String,
)

fun CatalogMetadata.log() {
    logger.info(
        "Processing catalog: {} finished! Total Schemas: {}, Total Tables: {}, Total Size: {} bytes, Total Files: {}",
        catalog,
        totalSchemaCount,
        totalTableCount,
        totalSizeInBytes,
        totalFiles,
    )
}

fun SchemaMetadata.log() {
    logger.info(
        "Processing schema: {} finished! Total Tables: {}, Views: {}, Total Size: {} bytes, Total Files: {}, Failed Tables: {}",
        schema,
        totalTableCount,
        totalViewCount,
        totalSizeInBytes,
        totalFiles,
        failedTableCount,
    )
}

fun TableMetadata.log() {
    logger.info(
        "Processing finished for table: {}.{}.{}",
        catalog,
        schema,
        name,
    )
}
