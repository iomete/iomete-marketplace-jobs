package com.iomete.cleanup.untrackedtablefolders.storage

/** Raised when catalog-specific storage configuration cannot be resolved safely. */
class CatalogStorageConfigurationException(
    val catalog: String,
    message: String,
) : IllegalStateException(message)

/** Catalog-specific Hadoop overrides applied to an isolated configuration. */
data class CatalogStorageConfig(
    val catalog: String,
    val overrides: Map<String, String>,
    val removedKeys: Set<String>,
) {
    /** Redacts credential values from string representations. */
    override fun toString(): String {
        val rendered =
            overrides.entries.sortedBy { it.key }.joinToString(", ") { (key, value) ->
                if (key in CatalogStorageProperties.SENSITIVE_HADOOP_KEYS) {
                    "$key=set(len=${value.length})"
                } else {
                    "$key=$value"
                }
            }

        return "catalog=$catalog, overrides={$rendered}, removedKeys=${removedKeys.sorted()}"
    }
}

object CatalogStorageProperties {
    const val CATALOG_CONF_PREFIX = "spark.sql.catalog"

    const val ENDPOINT = "s3.endpoint"
    const val ACCESS_KEY_ID = "s3.access-key-id"
    const val SECRET_ACCESS_KEY = "s3.secret-access-key"
    const val PATH_STYLE_ACCESS = "s3.path-style-access"
    const val CONNECTION_SSL_ENABLED = "s3.connection-ssl-enabled"

    // Region may be emitted under different keys by IOMETE components and Iceberg.
    val REGION_KEYS = listOf("s3.region", "s3.client.region", "client.region")

    const val FS_ENDPOINT = "fs.s3a.endpoint"
    const val FS_ACCESS_KEY = "fs.s3a.access.key"
    const val FS_SECRET_KEY = "fs.s3a.secret.key"
    const val FS_SESSION_TOKEN = "fs.s3a.session.token"
    const val FS_PATH_STYLE_ACCESS = "fs.s3a.path.style.access"
    const val FS_REGION = "fs.s3a.endpoint.region"
    const val FS_SSL_ENABLED = "fs.s3a.connection.ssl.enabled"
    const val FS_CREDENTIALS_PROVIDER = "fs.s3a.aws.credentials.provider"

    /** Hadoop keys whose values are credentials and must never be rendered. */
    val SENSITIVE_HADOOP_KEYS = setOf(FS_ACCESS_KEY, FS_SECRET_KEY, FS_SESSION_TOKEN)

    const val S3A_FILE_SYSTEM_CLASS = "org.apache.hadoop.fs.s3a.S3AFileSystem"
    const val SIMPLE_CREDENTIALS_PROVIDER = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

    val S3_SCHEMES = setOf("s3", "s3a", "s3n")

    fun isS3Scheme(scheme: String?): Boolean = scheme?.lowercase() in S3_SCHEMES

    fun catalogRegistrationKey(catalog: String): String = "$CATALOG_CONF_PREFIX.$catalog"

    fun catalogPropertyPrefix(catalog: String): String = "$CATALOG_CONF_PREFIX.$catalog."
}

/** Maps catalog S3 properties to an isolated Hadoop S3A configuration. */
object CatalogHadoopConfigBuilder {

    fun build(
        catalog: String,
        catalogProperties: Map<String, String>,
        hadoopFallback: (String) -> String? = { null },
    ): CatalogStorageConfig {
        val endpoint = catalogProperties[CatalogStorageProperties.ENDPOINT]?.takeIf { it.isNotBlank() }
        val catalogOwnsEndpoint = endpoint != null

        val catalogAccessKey = catalogProperties[CatalogStorageProperties.ACCESS_KEY_ID]?.takeIf { it.isNotBlank() }
        val catalogSecretKey = catalogProperties[CatalogStorageProperties.SECRET_ACCESS_KEY]?.takeIf { it.isNotBlank() }
        requireCompleteCredentialPair(catalog, catalogAccessKey, catalogSecretKey)

        val accessKey = credential(catalogAccessKey, hadoopFallback, catalogOwnsEndpoint, CatalogStorageProperties.FS_ACCESS_KEY)
        val secretKey = credential(catalogSecretKey, hadoopFallback, catalogOwnsEndpoint, CatalogStorageProperties.FS_SECRET_KEY)

        val region =
            CatalogStorageProperties.REGION_KEYS
                .firstNotNullOfOrNull { catalogProperties[it]?.takeIf { value -> value.isNotBlank() } }
                ?: hadoopFallback(CatalogStorageProperties.FS_REGION)?.takeIf { it.isNotBlank() }

        val pathStyleAccess =
            catalogProperties[CatalogStorageProperties.PATH_STYLE_ACCESS]?.takeIf { it.isNotBlank() }
                ?: hadoopFallback(CatalogStorageProperties.FS_PATH_STYLE_ACCESS)?.takeIf { it.isNotBlank() }
                ?: catalogOwnsEndpoint.toString()

        val sslEnabled =
            catalogProperties[CatalogStorageProperties.CONNECTION_SSL_ENABLED]?.takeIf { it.isNotBlank() }
                ?: endpoint?.let { (!it.startsWith("http://")).toString() }
                ?: hadoopFallback(CatalogStorageProperties.FS_SSL_ENABLED)?.takeIf { it.isNotBlank() }
                ?: "true"

        val overrides = buildMap {
            put("fs.s3.impl", CatalogStorageProperties.S3A_FILE_SYSTEM_CLASS)
            put("fs.s3n.impl", CatalogStorageProperties.S3A_FILE_SYSTEM_CLASS)

            // Avoid reusing a filesystem instance created with another storage configuration.
            CatalogStorageProperties.S3_SCHEMES.forEach { put("fs.$it.impl.disable.cache", "true") }

            put(CatalogStorageProperties.FS_PATH_STYLE_ACCESS, pathStyleAccess)
            put(CatalogStorageProperties.FS_SSL_ENABLED, sslEnabled)
            endpoint?.let { put(CatalogStorageProperties.FS_ENDPOINT, it) }
            region?.let { put(CatalogStorageProperties.FS_REGION, it) }

            // Omitting the credentials lets S3A use its own chain (IRSA, instance profile,
            // environment), which is how IOMETE-managed catalogs work on AWS.
            if (accessKey != null && secretKey != null) {
                put(CatalogStorageProperties.FS_ACCESS_KEY, accessKey)
                put(CatalogStorageProperties.FS_SECRET_KEY, secretKey)
                put(CatalogStorageProperties.FS_CREDENTIALS_PROVIDER, CatalogStorageProperties.SIMPLE_CREDENTIALS_PROVIDER)
            }
        }

        val removedKeys = buildSet {
            if (catalogOwnsEndpoint && (accessKey == null || secretKey == null)) {
                add(CatalogStorageProperties.FS_ACCESS_KEY)
                add(CatalogStorageProperties.FS_SECRET_KEY)
                add(CatalogStorageProperties.FS_SESSION_TOKEN)
                add(CatalogStorageProperties.FS_CREDENTIALS_PROVIDER)
            }

            // An inherited platform session token must not be paired with catalog static credentials.
            if (catalogAccessKey != null) {
                add(CatalogStorageProperties.FS_SESSION_TOKEN)
            }
        }

        return CatalogStorageConfig(
            catalog = catalog,
            overrides = overrides,
            removedKeys = removedKeys,
        )
    }

    private fun requireCompleteCredentialPair(
        catalog: String,
        accessKey: String?,
        secretKey: String?,
    ) {
        if ((accessKey == null) == (secretKey == null)) {
            return
        }

        val present =
            if (accessKey != null) CatalogStorageProperties.ACCESS_KEY_ID else CatalogStorageProperties.SECRET_ACCESS_KEY
        val missing =
            if (accessKey != null) CatalogStorageProperties.SECRET_ACCESS_KEY else CatalogStorageProperties.ACCESS_KEY_ID

        throw CatalogStorageConfigurationException(
            catalog = catalog,
            message =
                "Catalog '$catalog' sets '$present' but not '$missing'. Static credentials must be configured as a " +
                    "pair, or omitted entirely so another credential mechanism is used.",
        )
    }

    private fun credential(
        fromCatalog: String?,
        hadoopFallback: (String) -> String?,
        catalogOwnsEndpoint: Boolean,
        fallbackKey: String,
    ): String? {
        if (fromCatalog != null || catalogOwnsEndpoint) {
            return fromCatalog
        }
        return hadoopFallback(fallbackKey)?.takeIf { it.isNotBlank() }
    }
    /** Returns the redacted configuration representation used for logging. */
    fun describe(config: CatalogStorageConfig): String = config.toString()
}
