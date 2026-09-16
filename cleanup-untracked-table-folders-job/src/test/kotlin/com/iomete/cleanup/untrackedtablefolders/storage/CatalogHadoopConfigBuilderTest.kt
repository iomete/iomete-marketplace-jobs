package com.iomete.cleanup.untrackedtablefolders.storage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The mapping mirrors `com.iomete.enterprisecatalog.credential.CredentialUtil`, so these cases are
 * written against the same scenarios that class documents.
 */
class CatalogHadoopConfigBuilderTest {

    private val s3CompatibleCatalogProperties =
        mapOf(
            "io-impl" to "org.apache.iceberg.aws.s3.S3FileIO",
            "warehouse" to "s3://example-bucket/lakehouse",
            "client.region" to "us-east-1",
            "s3.endpoint" to "https://objectstore.example.com",
            "s3.path-style-access" to "true",
            "s3.access-key-id" to "ECS_ACCESS_KEY",
            "s3.secret-access-key" to "ECS_SECRET_KEY",
        )

    @Test
    fun `maps S3-compatible catalog properties to S3A settings`() {
        val config = CatalogHadoopConfigBuilder.build("example_catalog", s3CompatibleCatalogProperties)

        assertEquals("https://objectstore.example.com", config.overrides[CatalogStorageProperties.FS_ENDPOINT])
        assertEquals("true", config.overrides[CatalogStorageProperties.FS_PATH_STYLE_ACCESS])
        assertEquals("ECS_ACCESS_KEY", config.overrides[CatalogStorageProperties.FS_ACCESS_KEY])
        assertEquals("ECS_SECRET_KEY", config.overrides[CatalogStorageProperties.FS_SECRET_KEY])
        assertEquals("us-east-1", config.overrides[CatalogStorageProperties.FS_REGION])
        assertEquals("true", config.overrides[CatalogStorageProperties.FS_SSL_ENABLED])
        assertEquals(
            CatalogStorageProperties.SIMPLE_CREDENTIALS_PROVIDER,
            config.overrides[CatalogStorageProperties.FS_CREDENTIALS_PROVIDER],
        )
    }

    @Test
    fun `binds the legacy s3 and s3n schemes to S3A and disables the filesystem cache`() {
        val config = CatalogHadoopConfigBuilder.build("example_catalog", s3CompatibleCatalogProperties)

        // Catalog table locations are stored with the scheme the operator typed, which is `s3://`
        // for this catalog. Hadoop 3 has no ServiceLoader binding for s3 or s3n.
        assertEquals(CatalogStorageProperties.S3A_FILE_SYSTEM_CLASS, config.overrides["fs.s3.impl"])
        assertEquals(CatalogStorageProperties.S3A_FILE_SYSTEM_CLASS, config.overrides["fs.s3n.impl"])
        assertEquals("true", config.overrides["fs.s3.impl.disable.cache"])
        assertEquals("true", config.overrides["fs.s3a.impl.disable.cache"])
        assertEquals("true", config.overrides["fs.s3n.impl.disable.cache"])
    }

    @Test
    fun `defaults path style access to true when the catalog declares an endpoint`() {
        val config =
            CatalogHadoopConfigBuilder.build(
                "example_catalog",
                mapOf("s3.endpoint" to "https://objectstore.example.com"),
            )

        assertEquals("true", config.overrides[CatalogStorageProperties.FS_PATH_STYLE_ACCESS])
    }

    @Test
    fun `defaults path style access to false when no endpoint is declared`() {
        val config = CatalogHadoopConfigBuilder.build("spark_catalog", mapOf("warehouse" to "s3a://platform/lakehouse"))

        assertEquals("false", config.overrides[CatalogStorageProperties.FS_PATH_STYLE_ACCESS])
        assertNull(config.overrides[CatalogStorageProperties.FS_ENDPOINT])
    }

    @Test
    fun `derives ssl from the endpoint scheme`() {
        val plain =
            CatalogHadoopConfigBuilder.build("minio_catalog", mapOf("s3.endpoint" to "http://minio.default:9000"))

        assertEquals("false", plain.overrides[CatalogStorageProperties.FS_SSL_ENABLED])
    }

    @Test
    fun `explicit catalog values win over the derived defaults`() {
        val config =
            CatalogHadoopConfigBuilder.build(
                "example_catalog",
                mapOf(
                    "s3.endpoint" to "https://objectstore.example.com",
                    "s3.path-style-access" to "false",
                    "s3.connection-ssl-enabled" to "false",
                ),
            )

        assertEquals("false", config.overrides[CatalogStorageProperties.FS_PATH_STYLE_ACCESS])
        assertEquals("false", config.overrides[CatalogStorageProperties.FS_SSL_ENABLED])
    }

    @Test
    fun `an inherited path style setting is preserved when the catalog does not state one`() {
        val fallback = mapOf(CatalogStorageProperties.FS_PATH_STYLE_ACCESS to "true")

        val config = CatalogHadoopConfigBuilder.build("spark_catalog", mapOf("warehouse" to "s3a://platform/lakehouse")) { fallback[it] }

        // Without the fallback the derived default would be false and would silently switch an
        // on-prem data plane from path style to virtual-hosted style.
        assertEquals("true", config.overrides[CatalogStorageProperties.FS_PATH_STYLE_ACCESS])
    }

    @Test
    fun `accepts every region key the platform emits`() {
        CatalogStorageProperties.REGION_KEYS.forEach { regionKey ->
            val config = CatalogHadoopConfigBuilder.build("example_catalog", mapOf(regionKey to "eu-west-1"))

            assertEquals(
                "eu-west-1",
                config.overrides[CatalogStorageProperties.FS_REGION],
                "region key $regionKey was not read",
            )
        }
    }

    @Test
    fun `omits credentials entirely when the catalog has none and no endpoint, so S3A uses its own chain`() {
        val config = CatalogHadoopConfigBuilder.build("spark_catalog", mapOf("warehouse" to "s3a://platform/lakehouse"))

        assertFalse(config.overrides.containsKey(CatalogStorageProperties.FS_ACCESS_KEY))
        assertFalse(config.overrides.containsKey(CatalogStorageProperties.FS_SECRET_KEY))
        assertFalse(config.overrides.containsKey(CatalogStorageProperties.FS_CREDENTIALS_PROVIDER))
        assertTrue(config.removedKeys.isEmpty())
    }

    @Test
    fun `falls back to the inherited hadoop credentials only when the catalog declares no endpoint`() {
        val fallback =
            mapOf(
                CatalogStorageProperties.FS_ACCESS_KEY to "PLATFORM_KEY",
                CatalogStorageProperties.FS_SECRET_KEY to "PLATFORM_SECRET",
            )

        val platformCatalog =
            CatalogHadoopConfigBuilder.build("spark_catalog", mapOf("warehouse" to "s3a://platform/lakehouse")) { fallback[it] }

        assertEquals("PLATFORM_KEY", platformCatalog.overrides[CatalogStorageProperties.FS_ACCESS_KEY])
    }

    @Test
    fun `never sends inherited platform credentials to a catalog endpoint`() {
        val fallback =
            mapOf(
                CatalogStorageProperties.FS_ACCESS_KEY to "PLATFORM_KEY",
                CatalogStorageProperties.FS_SECRET_KEY to "PLATFORM_SECRET",
            )

        val config =
            CatalogHadoopConfigBuilder.build(
                "example_catalog",
                mapOf("s3.endpoint" to "https://objectstore.example.com"),
            ) { fallback[it] }

        assertFalse(config.overrides.containsValue("PLATFORM_KEY"))
        assertFalse(config.overrides.containsValue("PLATFORM_SECRET"))
        assertTrue(config.removedKeys.contains(CatalogStorageProperties.FS_ACCESS_KEY))
        assertTrue(config.removedKeys.contains(CatalogStorageProperties.FS_SECRET_KEY))
        assertTrue(config.removedKeys.contains(CatalogStorageProperties.FS_CREDENTIALS_PROVIDER))
    }

    @Test
    fun `rejects an access key without a secret key`() {
        val error =
            assertThrows(CatalogStorageConfigurationException::class.java) {
                CatalogHadoopConfigBuilder.build(
                    "example_catalog",
                    mapOf(
                        "s3.endpoint" to "https://objectstore.example.com",
                        "s3.access-key-id" to "ECS_ACCESS_KEY",
                    ),
                )
            }

        assertEquals("example_catalog", error.catalog)
        assertTrue(error.message!!.contains("s3.access-key-id"))
        assertTrue(error.message!!.contains("s3.secret-access-key"))
        assertFalse(error.message!!.contains("ECS_ACCESS_KEY"))
    }

    @Test
    fun `rejects a secret key without an access key`() {
        val error =
            assertThrows(CatalogStorageConfigurationException::class.java) {
                CatalogHadoopConfigBuilder.build(
                    "example_catalog",
                    mapOf(
                        "s3.endpoint" to "https://objectstore.example.com",
                        "s3.secret-access-key" to "ECS_SECRET_KEY",
                    ),
                )
            }

        assertEquals("example_catalog", error.catalog)
        assertTrue(error.message!!.contains("s3.secret-access-key"))
        assertTrue(error.message!!.contains("s3.access-key-id"))
        assertFalse(error.message!!.contains("ECS_SECRET_KEY"))
    }

    @Test
    fun `rejects an incomplete credential pair even when a blank value is supplied`() {
        assertThrows(CatalogStorageConfigurationException::class.java) {
            CatalogHadoopConfigBuilder.build(
                "example_catalog",
                mapOf(
                    "s3.access-key-id" to "ECS_ACCESS_KEY",
                    "s3.secret-access-key" to "   ",
                ),
            )
        }
    }

    @Test
    fun `removes an inherited session token when catalog static credentials are used`() {
        val fallback = mapOf(CatalogStorageProperties.FS_SESSION_TOKEN to "PLATFORM_SESSION_TOKEN")

        val config = CatalogHadoopConfigBuilder.build("example_catalog", s3CompatibleCatalogProperties) { fallback[it] }

        assertTrue(config.removedKeys.contains(CatalogStorageProperties.FS_SESSION_TOKEN))
        assertFalse(config.overrides.containsKey(CatalogStorageProperties.FS_SESSION_TOKEN))
    }

    @Test
    fun `keeps inherited platform authentication state when the catalog supplies no credentials`() {
        val fallback =
            mapOf(
                CatalogStorageProperties.FS_ACCESS_KEY to "PLATFORM_KEY",
                CatalogStorageProperties.FS_SECRET_KEY to "PLATFORM_SECRET",
                CatalogStorageProperties.FS_SESSION_TOKEN to "PLATFORM_SESSION_TOKEN",
            )

        val config = CatalogHadoopConfigBuilder.build("spark_catalog", mapOf("warehouse" to "s3a://platform/lakehouse")) { fallback[it] }

        assertFalse(config.removedKeys.contains(CatalogStorageProperties.FS_SESSION_TOKEN))
    }

    @Test
    fun `describe never prints a credential value`() {
        val described = CatalogHadoopConfigBuilder.describe(CatalogHadoopConfigBuilder.build("example_catalog", s3CompatibleCatalogProperties))

        assertFalse(described.contains("ECS_ACCESS_KEY"))
        assertFalse(described.contains("ECS_SECRET_KEY"))
        assertTrue(described.contains("${CatalogStorageProperties.FS_ACCESS_KEY}=set(len=14)"))
        assertTrue(described.contains("https://objectstore.example.com"))
    }

    @Test
    fun `toString never prints a credential value`() {
        val config = CatalogHadoopConfigBuilder.build("example_catalog", s3CompatibleCatalogProperties)

        // The generated data-class toString would print both values verbatim.
        val rendered = config.toString()

        assertFalse(rendered.contains("ECS_ACCESS_KEY"))
        assertFalse(rendered.contains("ECS_SECRET_KEY"))
        assertTrue(rendered.contains("${CatalogStorageProperties.FS_ACCESS_KEY}=set(len=14)"))
        assertTrue(rendered.contains("${CatalogStorageProperties.FS_SECRET_KEY}=set(len=14)"))
    }

    @Test
    fun `string interpolation of the config is redacted`() {
        val config = CatalogHadoopConfigBuilder.build("example_catalog", s3CompatibleCatalogProperties)

        // This is the shape a future log line would take, and the reason toString is overridden.
        val logLine = "Resolved catalog storage configuration: $config"

        assertFalse(logLine.contains("ECS_ACCESS_KEY"))
        assertFalse(logLine.contains("ECS_SECRET_KEY"))
    }

    @Test
    fun `a session token would also be redacted`() {
        val config =
            CatalogStorageConfig(
                catalog = "example_catalog",
                overrides = mapOf(CatalogStorageProperties.FS_SESSION_TOKEN to "SESSION_TOKEN_VALUE"),
                removedKeys = emptySet(),
            )

        assertFalse(config.toString().contains("SESSION_TOKEN_VALUE"))
        assertTrue(config.toString().contains("${CatalogStorageProperties.FS_SESSION_TOKEN}=set(len=19)"))
    }

    @Test
    fun `describe and toString are the same representation`() {
        val config = CatalogHadoopConfigBuilder.build("example_catalog", s3CompatibleCatalogProperties)

        assertEquals(config.toString(), CatalogHadoopConfigBuilder.describe(config))
    }

    @Test
    fun `every credential key the builder can emit is treated as sensitive`() {
        // Guards against a new credential key being added to the builder without being redacted.
        assertTrue(CatalogStorageProperties.FS_ACCESS_KEY in CatalogStorageProperties.SENSITIVE_HADOOP_KEYS)
        assertTrue(CatalogStorageProperties.FS_SECRET_KEY in CatalogStorageProperties.SENSITIVE_HADOOP_KEYS)
        assertTrue(CatalogStorageProperties.FS_SESSION_TOKEN in CatalogStorageProperties.SENSITIVE_HADOOP_KEYS)
    }

    @Test
    fun `recognises only s3 compatible schemes`() {
        assertTrue(CatalogStorageProperties.isS3Scheme("s3"))
        assertTrue(CatalogStorageProperties.isS3Scheme("s3a"))
        assertTrue(CatalogStorageProperties.isS3Scheme("S3A"))
        assertFalse(CatalogStorageProperties.isS3Scheme("gs"))
        assertFalse(CatalogStorageProperties.isS3Scheme(null))
    }
}
