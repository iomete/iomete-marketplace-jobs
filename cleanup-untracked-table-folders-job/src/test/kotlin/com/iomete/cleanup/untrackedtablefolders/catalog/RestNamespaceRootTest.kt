package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import io.mockk.every
import io.mockk.mockk
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

private const val MANAGED_URI = "http://iom-rest-catalog.iomete-system.svc.cluster.local/internal/catalogs/spark_catalog"

class RestNamespaceRootTest {

    private val sparkConf = SparkConf(false)

    private val catalogDiscoveryService =
        CatalogDiscoveryService().apply {
            sparkSessionProvider =
                mockk<SparkSessionProvider>().also { provider ->
                    val sparkContext = mockk<SparkContext>()
                    val sparkSession = mockk<SparkSession>()
                    every { sparkContext.getConf() } returns sparkConf
                    every { sparkSession.sparkContext() } returns sparkContext
                    every { provider.getOrCreate() } returns sparkSession
                }
        }

    @Test
    fun `managed session catalog places namespaces under its warehouse`() {
        configure("spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog", "rest", MANAGED_URI, "s3a://bucket/lake/")

        assertEquals("s3a://bucket/lake/sales", catalogDiscoveryService.restNamespaceRoot("spark_catalog", "sales")?.location)
    }

    @Test
    fun `managed REST catalog reached through the control plane is recognized`() {
        configure("analytics", "org.apache.iceberg.spark.SparkCatalog", "rest", "https://cp.example.com/internal/catalogs/analytics", "s3a://bucket/analytics")

        assertEquals("s3a://bucket/analytics/sales", catalogDiscoveryService.restNamespaceRoot("analytics", "sales")?.location)
    }

    @Test
    fun `the secured catalogs route on the in-cluster service is recognized`() {
        configure(
            "spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog",
            "rest",
            "http://iom-rest-catalog.iomete-system.svc.cluster.local/catalogs/spark_catalog",
            "s3a://bucket/lake",
        )

        assertEquals("s3a://bucket/lake/sales", catalogDiscoveryService.restNamespaceRoot("spark_catalog", "sales")?.location)
    }

    @Test
    fun `external REST catalog has no derived root`() {
        configure("external", "org.apache.iceberg.spark.SparkCatalog", "rest", "https://catalog.example.com/api/catalog", "s3a://bucket/lake")

        assertEquals(RestNamespaceRoot(location = null, status = "unrecognized_uri"), catalogDiscoveryService.restNamespaceRoot("external", "sales"))
    }

    @Test
    fun `enterprise catalog has no derived root`() {
        configure("ent", "com.iomete.enterprisecatalog.IomEnterpriseSparkCatalog", "rest", "/internal/catalogs/ent", "s3a://bucket/lake")

        assertNull(catalogDiscoveryService.restNamespaceRoot("ent", "sales"))
    }

    @Test
    fun `missing warehouse gives no derived root`() {
        configure("spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog", "rest", MANAGED_URI, "")

        assertEquals(RestNamespaceRoot(location = null, status = "no_warehouse"), catalogDiscoveryService.restNamespaceRoot("spark_catalog", "sales"))
    }

    @Test
    fun `managed URI must name the same catalog`() {
        assertTrue(isManagedRestCatalogUri("/internal/catalogs/spark_catalog", "spark_catalog"))
        assertTrue(isManagedRestCatalogUri("$MANAGED_URI/", "spark_catalog"))
        assertFalse(isManagedRestCatalogUri(MANAGED_URI, "other_catalog"))
        assertFalse(isManagedRestCatalogUri("https://catalog.example.com/v1", "spark_catalog"))
        assertFalse(isManagedRestCatalogUri("not a uri", "spark_catalog"))
        assertTrue(isManagedRestCatalogUri("http://iom-rest-catalog.iomete-system.svc.cluster.local/catalogs/spark_catalog", "spark_catalog"))
        assertFalse(isManagedRestCatalogUri("https://platform.example.com/catalogs/spark_catalog", "spark_catalog"))
        assertFalse(isManagedRestCatalogUri("http://iom-rest-catalog-proxy.example.com/catalogs/spark_catalog", "spark_catalog"))
        assertFalse(
            isManagedRestCatalogUri("http://iom-rest-catalog.iomete-system.svc.cluster.local/internal/enterprisecatalogs/spark_catalog", "spark_catalog"),
        )
    }

    private fun configure(
        catalog: String,
        catalogClass: String,
        type: String,
        uri: String,
        warehouse: String,
    ) {
        sparkConf.set("spark.sql.catalog.$catalog", catalogClass)
        sparkConf.set("spark.sql.catalog.$catalog.type", type)
        sparkConf.set("spark.sql.catalog.$catalog.uri", uri)
        sparkConf.set("spark.sql.catalog.$catalog.warehouse", warehouse)
    }
}
