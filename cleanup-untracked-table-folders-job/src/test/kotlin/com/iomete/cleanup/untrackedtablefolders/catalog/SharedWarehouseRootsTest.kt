package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import io.mockk.every
import io.mockk.mockk
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SharedWarehouseRootsTest {

    private val startupConf = SparkConf(false)
    private val sessionConf = mutableMapOf<String, String>()
    private val runtimeConfig = sessionConfigOf(sessionConf)

    private val catalogDiscoveryService =
        CatalogDiscoveryService().apply {
            sparkSessionProvider =
                mockk<SparkSessionProvider>().also { provider ->
                    val sparkContext = mockk<SparkContext>()
                    val sparkSession = mockk<SparkSession>()
                    every { sparkContext.getConf() } returns startupConf
                    every { sparkSession.sparkContext() } returns sparkContext
                    every { sparkSession.conf() } returns runtimeConfig
                    every { provider.getOrCreate() } returns sparkSession
                }
        }

    @Test
    fun `uses only the selected catalog warehouse for a non-session catalog`() {
        sessionConf["spark.sql.catalog.example_catalog.warehouse"] = "s3a://bucket/lake"
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/session"

        assertEquals(listOf("s3a://bucket/lake"), catalogDiscoveryService.sharedWarehouseRoots("example_catalog"))
    }

    @Test
    fun `adds the session warehouse for spark_catalog`() {
        sessionConf["spark.sql.catalog.spark_catalog.warehouse"] = "s3a://bucket/lake"
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/session"

        assertEquals(
            listOf("s3a://bucket/lake", "s3a://bucket/session"),
            catalogDiscoveryService.sharedWarehouseRoots("spark_catalog"),
        )
    }

    @Test
    fun `returns one root when both spark_catalog warehouses are the same`() {
        sessionConf["spark.sql.catalog.spark_catalog.warehouse"] = "s3a://bucket/lake"
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/lake"

        assertEquals(listOf("s3a://bucket/lake"), catalogDiscoveryService.sharedWarehouseRoots("spark_catalog"))
    }

    @Test
    fun `returns nothing when the catalog warehouse is missing, even for spark_catalog`() {
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/session"

        assertEquals(emptyList<String>(), catalogDiscoveryService.sharedWarehouseRoots("spark_catalog"))
    }

    @Test
    fun `treats a blank catalog warehouse as missing`() {
        sessionConf["spark.sql.catalog.example_catalog.warehouse"] = "  "

        assertEquals(emptyList<String>(), catalogDiscoveryService.sharedWarehouseRoots("example_catalog"))
    }

    @Test
    fun `lists each registered session catalog with its warehouse`() {
        sessionConf["spark.sql.catalog.spark_catalog"] = "org.apache.iceberg.spark.SparkSessionCatalog"
        sessionConf["spark.sql.catalog.spark_catalog.warehouse"] = " s3a://bucket/lake/ "
        sessionConf["spark.sql.catalog.views"] = "org.apache.iceberg.spark.SparkCatalog"
        sessionConf["spark.sql.catalog.views.warehouse"] = "s3://bucket/views"
        sessionConf["spark.sql.catalog.views.io.warehouse"] = "s3://bucket/not-a-catalog"
        sessionConf["spark.sql.catalog.jdbc_source"] = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"
        sessionConf["spark.sql.catalog.unregistered.warehouse"] = "s3://bucket/unregistered"
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/hive"

        assertEquals(
            mapOf("spark_catalog" to listOf("s3a://bucket/lake/", "s3a://bucket/hive"), "views" to listOf("s3://bucket/views")),
            catalogDiscoveryService.sessionCatalogWarehouses(),
        )
    }

    @Test
    fun `lists a storage catalog with no warehouse or a blank one, and leaves out JDBC catalogs`() {
        sessionConf["spark.sql.catalog.remote"] = "org.apache.iceberg.spark.SparkCatalog"
        sessionConf["spark.sql.catalog.remote.type"] = "rest"
        sessionConf["spark.sql.catalog.ent"] = "com.iomete.enterprisecatalog.IomEnterpriseSparkCatalog"
        sessionConf["spark.sql.catalog.ent.warehouse"] = " "
        sessionConf["spark.sql.catalog.jdbc_source"] = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"
        sessionConf["spark.sql.catalog.blank_class"] = " "
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/hive"

        assertEquals(
            mapOf("remote" to emptyList(), "ent" to emptyList(), "spark_catalog" to listOf("s3a://bucket/hive")),
            catalogDiscoveryService.sessionCatalogWarehouses(),
        )
    }

    @Test
    fun `reads catalogs from the session config, which can gain a catalog after startup`() {
        startupConf.set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        startupConf.set("spark.sql.catalog.spark_catalog.warehouse", "s3a://bucket/lake")
        sessionConf["spark.sql.catalog.spark_catalog"] = "org.apache.iceberg.spark.SparkSessionCatalog"
        sessionConf["spark.sql.catalog.spark_catalog.warehouse"] = "s3a://bucket/lake"
        sessionConf["spark.sql.catalog.late"] = "org.apache.iceberg.spark.SparkCatalog"
        sessionConf["spark.sql.catalog.late.warehouse"] = "s3a://bucket/late"
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/lake"

        assertEquals(listOf("s3a://bucket/late"), catalogDiscoveryService.sessionCatalogWarehouses()["late"])
        assertEquals(listOf("s3a://bucket/late"), catalogDiscoveryService.sharedWarehouseRoots("late"))
        assertEquals(listOf("s3a://bucket/lake", "s3a://bucket/late"), catalogDiscoveryService.knownWarehouseRoots())
    }

    @Test
    fun `lists spark_catalog by its session warehouse when it has no catalog settings`() {
        sessionConf["spark.sql.catalog.views"] = "org.apache.iceberg.spark.SparkCatalog"
        sessionConf["spark.sql.catalog.views.warehouse"] = "s3a://bucket/views"
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/hive"

        assertEquals(
            mapOf("views" to listOf("s3a://bucket/views"), "spark_catalog" to listOf("s3a://bucket/hive")),
            catalogDiscoveryService.sessionCatalogWarehouses(),
        )
    }
}
