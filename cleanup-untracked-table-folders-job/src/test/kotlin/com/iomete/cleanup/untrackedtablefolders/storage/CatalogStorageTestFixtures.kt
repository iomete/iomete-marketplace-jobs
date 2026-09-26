package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import io.mockk.every
import io.mockk.mockk
import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

/** Real SparkConf and real Hadoop Configuration, so tests can assert Spark's own config is never mutated. */
class CatalogStorageTestHarness(
    catalogProperties: Map<String, Map<String, String>>,
    val sparkHadoopConfiguration: Configuration = baseHadoopConfiguration(),
) {
    val sparkConf: SparkConf = SparkConf(false)

    val sparkSessionProvider: SparkSessionProvider = mockk()

    val resolver: CatalogStorageConfigResolver

    val fileSystemProvider: CatalogFileSystemProvider

    init {
        catalogProperties.forEach { (catalog, properties) ->
            sparkConf.set(CatalogStorageProperties.catalogRegistrationKey(catalog), "org.apache.iceberg.spark.SparkCatalog")
            properties.forEach { (key, value) ->
                sparkConf.set(CatalogStorageProperties.catalogPropertyPrefix(catalog) + key, value)
            }
        }

        val sparkContext = mockk<SparkContext>()
        val sparkSession = mockk<SparkSession>()
        every { sparkContext.getConf() } returns sparkConf
        every { sparkContext.hadoopConfiguration() } returns sparkHadoopConfiguration
        every { sparkSession.sparkContext() } returns sparkContext
        every { sparkSessionProvider.getOrCreate() } returns sparkSession

        resolver = CatalogStorageConfigResolver().also { it.sparkSessionProvider = sparkSessionProvider }
        fileSystemProvider =
            CatalogFileSystemProvider().also {
                it.sparkSessionProvider = sparkSessionProvider
                it.catalogStorageConfigResolver = resolver
            }
    }

    fun discoveryService(): ObjectStorageDiscoveryService =
        ObjectStorageDiscoveryService().also { it.catalogFileSystemProvider = fileSystemProvider }

    fun deletionService(): ObjectStorageDeletionService =
        ObjectStorageDeletionService().also { it.catalogFileSystemProvider = fileSystemProvider }

    companion object {
        fun baseHadoopConfiguration(): Configuration =
            Configuration(false).apply {
                set("fs.s3a.impl", InMemoryS3FileSystem::class.java.name)
                set("fs.s3a.endpoint", PLATFORM_ENDPOINT)
                set(CatalogStorageProperties.FS_ACCESS_KEY, "PLATFORM_KEY")
                set(CatalogStorageProperties.FS_SECRET_KEY, "PLATFORM_SECRET")
            }

        /** Loads core-default.xml, so fs.s3a.path.style.access is present with Hadoop's default. */
        fun hadoopConfigurationWithDefaults(): Configuration =
            Configuration().apply {
                set("fs.s3a.impl", InMemoryS3FileSystem::class.java.name)
                set(CatalogStorageProperties.FS_ACCESS_KEY, "PLATFORM_KEY")
                set(CatalogStorageProperties.FS_SECRET_KEY, "PLATFORM_SECRET")
            }

        const val PLATFORM_ENDPOINT = "https://s3.amazonaws.com"

        const val COMPAT_ENDPOINT = "https://objectstore.example.com"

        const val MINIO_ENDPOINT = "http://minio.default:9000"

        fun s3CompatibleCatalogProperties(): Map<String, String> =
            mapOf(
                "io-impl" to "org.apache.iceberg.aws.s3.S3FileIO",
                "warehouse" to "s3://example-bucket/lakehouse",
                "client.region" to "us-east-1",
                "s3.endpoint" to COMPAT_ENDPOINT,
                "s3.path-style-access" to "true",
                "s3.access-key-id" to "ECS_ACCESS_KEY",
                "s3.secret-access-key" to "ECS_SECRET_KEY",
            )

        fun minioCatalogProperties(): Map<String, String> =
            mapOf(
                "warehouse" to "s3a://minio-bkt/lakehouse",
                "s3.endpoint" to MINIO_ENDPOINT,
                "s3.access-key-id" to "MINIO_ACCESS_KEY",
                "s3.secret-access-key" to "MINIO_SECRET_KEY",
            )
    }
}
