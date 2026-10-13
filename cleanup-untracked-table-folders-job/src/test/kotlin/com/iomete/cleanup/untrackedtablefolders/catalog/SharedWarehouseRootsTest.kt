package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import io.mockk.every
import io.mockk.mockk
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SharedWarehouseRootsTest {

    private val sparkConf = SparkConf(false)
    private val runtimeConfig = mockk<RuntimeConfig>()

    private val catalogDiscoveryService =
        CatalogDiscoveryService().apply {
            sparkSessionProvider =
                mockk<SparkSessionProvider>().also { provider ->
                    val sparkContext = mockk<SparkContext>()
                    val sparkSession = mockk<SparkSession>()
                    every { sparkContext.getConf() } returns sparkConf
                    every { sparkSession.sparkContext() } returns sparkContext
                    every { sparkSession.conf() } returns runtimeConfig
                    every { provider.getOrCreate() } returns sparkSession
                }
        }

    @Test
    fun `uses only the selected catalog warehouse for a non-session catalog`() {
        sparkConf.set("spark.sql.catalog.example_catalog.warehouse", "s3a://bucket/lake")
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/session"

        assertEquals(listOf("s3a://bucket/lake"), catalogDiscoveryService.sharedWarehouseRoots("example_catalog"))
    }

    @Test
    fun `adds the session warehouse for spark_catalog`() {
        sparkConf.set("spark.sql.catalog.spark_catalog.warehouse", "s3a://bucket/lake")
        every { runtimeConfig.get("spark.sql.warehouse.dir", "") } returns "s3a://bucket/session"

        assertEquals(
            listOf("s3a://bucket/lake", "s3a://bucket/session"),
            catalogDiscoveryService.sharedWarehouseRoots("spark_catalog"),
        )
    }

    @Test
    fun `returns one root when both spark_catalog warehouses are the same`() {
        sparkConf.set("spark.sql.catalog.spark_catalog.warehouse", "s3a://bucket/lake")
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
        sparkConf.set("spark.sql.catalog.example_catalog.warehouse", "  ")

        assertEquals(emptyList<String>(), catalogDiscoveryService.sharedWarehouseRoots("example_catalog"))
    }
}
