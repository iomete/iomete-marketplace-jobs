package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CoreClient.CatalogDetails
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test

class DtoTest {

    private fun tableMetadata(
        catalog: String = "cat",
        schema: String = "sch",
        name: String = "tbl",
        isView: Boolean = false,
        sizeInBytes: Long? = null,
        totalTableSizeInBytes: Long? = null,
        numFiles: Long? = null,
    ) = TableMetadata(
        catalog = catalog,
        schema = schema,
        name = name,
        description = null,
        tableType = if (isView) "VIEW" else "MANAGED",
        isView = isView,
        isTemporary = false,
        owner = "owner",
        provider = "iceberg",
        viewText = null,
        sizeInBytes = sizeInBytes,
        totalTableSizeInBytes = totalTableSizeInBytes,
        numFiles = numFiles,
        columns = emptyList(),
        syncTime = System.currentTimeMillis(),
        sparkApplicationId = "app-123",
    )


    @Test
    fun `CatalogMetadata build aggregates counts from multiple schemas`() {
        val schemas = listOf(
            SchemaMetadata("cat", "s1", totalTableCount = 3, totalViewCount = 1, totalSizeInBytes = 100L, totalDbSizeInBytes = 200L, totalFiles = 5L, failedTableCount = 0, sparkApplicationId = "app-1"),
            SchemaMetadata("cat", "s2", totalTableCount = 2, totalViewCount = 0, totalSizeInBytes = 50L, totalDbSizeInBytes = 100L, totalFiles = 3L, failedTableCount = 1, sparkApplicationId = "app-1"),
        )
        val catalog = CatalogDetails(name = "cat", type = listOf("iceberg"), location = "/loc", storageEndpoint = "s3://bucket")

        val result = CatalogMetadata.build(catalog, schemas, "app-1")

        assertEquals("cat", result.catalog)
        assertEquals(2, result.totalSchemaCount)
        assertEquals(5, result.totalTableCount)
        assertEquals(150L, result.totalSizeInBytes)
        assertEquals(8L, result.totalFiles)
        assertEquals("app-1", result.sparkApplicationId)
    }

    @Test
    fun `CatalogMetadata build from empty schema list returns zero counts`() {
        val catalog = CatalogDetails(name = "cat", type = listOf("iceberg"))

        val result = CatalogMetadata.build(catalog, emptyList(), "app-1")

        assertEquals(0, result.totalSchemaCount)
        assertEquals(0, result.totalTableCount)
        assertEquals(0L, result.totalSizeInBytes)
        assertEquals(0L, result.totalFiles)
    }

    @Test
    fun `CatalogMetadata build propagates type and location`() {
        val catalog = CatalogDetails(name = "cat", type = listOf("iceberg", "rest"), location = "/data", storageEndpoint = "s3://ep")

        val result = CatalogMetadata.build(catalog, emptyList(), "app-1")

        assertEquals(setOf("iceberg", "rest"), result.type)
        assertEquals("/data", result.location)
        assertEquals("s3://ep", result.storageEndpoint)
    }


    @Test
    fun `SchemaMetadata build from multiple tables returns correct counts and sums`() {
        val tables = listOf(
            tableMetadata(name = "t1", sizeInBytes = 100L, totalTableSizeInBytes = 200L, numFiles = 5L),
            tableMetadata(name = "t2", sizeInBytes = 50L, totalTableSizeInBytes = 100L, numFiles = 3L),
            tableMetadata(name = "v1", isView = true, sizeInBytes = 0L, numFiles = 0L),
        )

        val result = SchemaMetadata.build("cat", "sch", tables, failuresSize = 1, sparkApplicationId = "app-1")

        assertEquals(2, result.totalTableCount)
        assertEquals(1, result.totalViewCount)
        assertEquals(150L, result.totalSizeInBytes)
        assertEquals(300L, result.totalDbSizeInBytes)
        assertEquals(8L, result.totalFiles)
        assertEquals(1, result.failedTableCount)
    }

    @Test
    fun `SchemaMetadata build from empty table list returns zero counts`() {
        val result = SchemaMetadata.build("cat", "sch", emptyList(), failuresSize = 0, sparkApplicationId = "app-1")

        assertEquals(0, result.totalTableCount)
        assertEquals(0, result.totalViewCount)
        assertEquals(0L, result.totalSizeInBytes)
        assertEquals(0L, result.totalDbSizeInBytes)
        assertEquals(0L, result.totalFiles)
        assertEquals(0, result.failedTableCount)
    }

    @Test
    fun `SchemaMetadata build counts views vs tables separately`() {
        val tables = listOf(
            tableMetadata(name = "t1"),
            tableMetadata(name = "v1", isView = true),
            tableMetadata(name = "v2", isView = true),
        )

        val result = SchemaMetadata.build("cat", "sch", tables, failuresSize = 0, sparkApplicationId = "app-1")

        assertEquals(1, result.totalTableCount)
        assertEquals(2, result.totalViewCount)
    }

    @Test
    fun `SchemaMetadata build passes through failed table count`() {
        val result = SchemaMetadata.build("cat", "sch", emptyList(), failuresSize = 5, sparkApplicationId = "app-1")
        assertEquals(5, result.failedTableCount)
    }

    @Test
    fun `SchemaMetadata build handles null sizes safely`() {
        val tables = listOf(
            tableMetadata(name = "t1", sizeInBytes = null, totalTableSizeInBytes = null, numFiles = null),
            tableMetadata(name = "t2", sizeInBytes = 100L, totalTableSizeInBytes = 200L, numFiles = 3L),
        )

        val result = SchemaMetadata.build("cat", "sch", tables, failuresSize = 0, sparkApplicationId = "app-1")

        assertEquals(100L, result.totalSizeInBytes)
        assertEquals(200L, result.totalDbSizeInBytes)
        assertEquals(3L, result.totalFiles)
    }
}
