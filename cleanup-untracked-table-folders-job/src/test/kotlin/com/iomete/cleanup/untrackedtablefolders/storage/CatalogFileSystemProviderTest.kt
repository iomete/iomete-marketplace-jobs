package com.iomete.cleanup.untrackedtablefolders.storage

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class CatalogFileSystemProviderTest {

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    private fun harness() =
        CatalogStorageTestHarness(
            catalogProperties =
                mapOf(
                    "example_catalog" to CatalogStorageTestHarness.s3CompatibleCatalogProperties(),
                    "minio_catalog" to CatalogStorageTestHarness.minioCatalogProperties(),
                    "spark_catalog" to mapOf("warehouse" to "s3a://platform-bkt/lakehouse"),
                ),
        )

    @Test
    fun `two catalogs with different endpoints get different filesystems in one application`() {
        val harness = harness()
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.COMPAT_ENDPOINT, "s3a://shared-name/db")
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.COMPAT_ENDPOINT, "s3a://shared-name/db/first_only")
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.MINIO_ENDPOINT, "s3a://shared-name/db")
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.MINIO_ENDPOINT, "s3a://shared-name/db/second_only")

        val discovery = harness.discoveryService()

        val first = discovery.listImmediateChildFolders("example_catalog", "s3a://shared-name/db").map { it.path }
        val second = discovery.listImmediateChildFolders("minio_catalog", "s3a://shared-name/db").map { it.path }

        assertEquals(listOf("s3a://shared-name/db/first_only"), first)
        assertEquals(listOf("s3a://shared-name/db/second_only"), second)
    }

    @Test
    fun `platform storage configuration is never mutated`() {
        val harness = harness()
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.COMPAT_ENDPOINT, "s3a://example-bucket/db")

        harness.discoveryService().listImmediateChildFolders("example_catalog", "s3a://example-bucket/db")

        val platform = harness.sparkHadoopConfiguration
        assertEquals(CatalogStorageTestHarness.PLATFORM_ENDPOINT, platform.get(CatalogStorageProperties.FS_ENDPOINT))
        assertEquals("PLATFORM_KEY", platform.get(CatalogStorageProperties.FS_ACCESS_KEY))
        assertEquals("PLATFORM_SECRET", platform.get(CatalogStorageProperties.FS_SECRET_KEY))
        assertEquals(null, platform.get(CatalogStorageProperties.FS_PATH_STYLE_ACCESS))
    }

    @Test
    fun `catalog configuration is a copy, not the Spark configuration object`() {
        val harness = harness()

        val configuration = harness.fileSystemProvider.buildConfiguration("example_catalog", Path("s3a://example-bucket/db"))

        assertNotSame(harness.sparkHadoopConfiguration, configuration)
        assertEquals(CatalogStorageTestHarness.COMPAT_ENDPOINT, configuration.get(CatalogStorageProperties.FS_ENDPOINT))
        assertEquals("true", configuration.get(CatalogStorageProperties.FS_PATH_STYLE_ACCESS))
    }

    @Test
    fun `a custom endpoint, path style access and catalog credentials reach the filesystem`() {
        val harness = harness()
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.COMPAT_ENDPOINT, "s3a://example-bucket/db")

        harness.discoveryService().listImmediateChildFolders("example_catalog", "s3a://example-bucket/db")

        val opened = InMemoryS3FileSystem.opened.single()
        assertEquals(CatalogStorageTestHarness.COMPAT_ENDPOINT, opened.endpoint)
        assertEquals("true", opened.pathStyleAccess)
        assertEquals("ECS_ACCESS_KEY", opened.accessKey)
        assertEquals("us-east-1", opened.region)
        assertEquals("true", opened.sslEnabled)
    }

    @Test
    fun `an inherited session token does not reach the catalog configuration`() {
        val harness = harness()
        harness.sparkHadoopConfiguration.set(CatalogStorageProperties.FS_SESSION_TOKEN, "PLATFORM_SESSION_TOKEN")

        val configuration = harness.fileSystemProvider.buildConfiguration("example_catalog", Path("s3a://example-bucket/db"))

        assertEquals(null, configuration.get(CatalogStorageProperties.FS_SESSION_TOKEN))
        assertEquals("PLATFORM_SESSION_TOKEN", harness.sparkHadoopConfiguration.get(CatalogStorageProperties.FS_SESSION_TOKEN))
    }

    @Test
    fun `a catalog-owned endpoint without catalog credentials fails closed`() {
        val harness =
            CatalogStorageTestHarness(
                catalogProperties = mapOf("no_creds_catalog" to mapOf("s3.endpoint" to CatalogStorageTestHarness.COMPAT_ENDPOINT)),
            )
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.COMPAT_ENDPOINT, "s3a://example-bucket/db")

        assertThrows(CatalogStorageConfigurationException::class.java) {
            harness.discoveryService().listImmediateChildFolders("no_creds_catalog", "s3a://example-bucket/db")
        }

        assertTrue(InMemoryS3FileSystem.opened.isEmpty())
    }

    @Test
    fun `path style survives a base configuration that loaded Hadoop defaults`() {
        val base = CatalogStorageTestHarness.hadoopConfigurationWithDefaults()
        assertEquals("false", base.get(CatalogStorageProperties.FS_PATH_STYLE_ACCESS))

        val harness =
            CatalogStorageTestHarness(
                catalogProperties = mapOf("example_catalog" to CatalogStorageTestHarness.s3CompatibleCatalogProperties().filterKeys { it != "s3.path-style-access" }),
                sparkHadoopConfiguration = base,
            )

        val configuration = harness.fileSystemProvider.buildConfiguration("example_catalog", Path("s3a://example-bucket/db"))

        assertEquals("true", configuration.get(CatalogStorageProperties.FS_PATH_STYLE_ACCESS))
        assertEquals("ECS_ACCESS_KEY", configuration.get(CatalogStorageProperties.FS_ACCESS_KEY))
        assertEquals("false", base.get(CatalogStorageProperties.FS_PATH_STYLE_ACCESS))
    }

    @Test
    fun `an instance cached under the platform configuration is not reused for a catalog`() {
        val harness = harness()
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.PLATFORM_ENDPOINT, "s3a://example-bucket/db")
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.PLATFORM_ENDPOINT, "s3a://example-bucket/db/platform_view")
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.COMPAT_ENDPOINT, "s3a://example-bucket/db")
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.COMPAT_ENDPOINT, "s3a://example-bucket/db/catalog_view")

        // FileSystem.get would hand this cached instance back for the same scheme and authority.
        FileSystem.get(Path("s3a://example-bucket/db").toUri(), harness.sparkHadoopConfiguration)

        val folders = harness.discoveryService().listImmediateChildFolders("example_catalog", "s3a://example-bucket/db")

        assertEquals(listOf("s3a://example-bucket/db/catalog_view"), folders.map { it.path })
    }

    @Test
    fun `the filesystem is closed after use`() {
        val harness = harness()
        InMemoryS3FileSystem.putDirectory(CatalogStorageTestHarness.COMPAT_ENDPOINT, "s3a://example-bucket/db")

        harness.discoveryService().listImmediateChildFolders("example_catalog", "s3a://example-bucket/db")

        assertEquals(listOf(CatalogStorageTestHarness.COMPAT_ENDPOINT), InMemoryS3FileSystem.closedEndpoints.toList())
    }

    @Test
    fun `the filesystem is closed even when the operation fails`() {
        val harness = harness()

        assertThrows(IllegalStateException::class.java) {
            harness.discoveryService().listImmediateChildFolders("example_catalog", "s3a://example-bucket/missing")
        }

        assertEquals(listOf(CatalogStorageTestHarness.COMPAT_ENDPOINT), InMemoryS3FileSystem.closedEndpoints.toList())
    }

    @Test
    fun `fails closed when the catalog is not registered in the Spark session`() {
        val harness = harness()

        val error =
            assertThrows(CatalogStorageConfigurationException::class.java) {
                harness.discoveryService().listImmediateChildFolders("unknown_catalog", "s3a://example-bucket/db")
            }

        assertEquals("unknown_catalog", error.catalog)
        assertTrue(error.message!!.contains("not registered in this Spark session"))
    }

    @Test
    fun `a non S3 location keeps using Spark's configuration unchanged`() {
        val harness = harness()

        val configuration = harness.fileSystemProvider.buildConfiguration("example_catalog", Path("gs://platform-bkt/db"))

        assertNotSame(harness.sparkHadoopConfiguration, configuration)
        assertEquals(CatalogStorageTestHarness.PLATFORM_ENDPOINT, configuration.get(CatalogStorageProperties.FS_ENDPOINT))
        assertFalse(InMemoryS3FileSystem.opened.isNotEmpty())
    }
}
