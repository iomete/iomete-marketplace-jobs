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

    const val SESSION_TOKEN = "s3.session-token"

    /** Hadoop keys whose values are credentials and must never be rendered. */
    val SENSITIVE_HADOOP_KEYS = setOf(FS_ACCESS_KEY, FS_SECRET_KEY, FS_SESSION_TOKEN)

    // Hadoop ships defaults in core-default.xml for keys such as fs.s3a.path.style.access, so
    // reading the base configuration returns a built-in default rather than a platform choice.
    // Only these keys are read back.
    val INHERITABLE_HADOOP_KEYS = setOf(FS_ACCESS_KEY, FS_SECRET_KEY, FS_REGION)

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

        requireNoSessionToken(catalog, catalogProperties)

        val catalogAccessKey = catalogProperties[CatalogStorageProperties.ACCESS_KEY_ID]?.takeIf { it.isNotBlank() }
        val catalogSecretKey = catalogProperties[CatalogStorageProperties.SECRET_ACCESS_KEY]?.takeIf { it.isNotBlank() }
        requireCompleteCredentialPair(catalog, catalogAccessKey, catalogSecretKey)
        requireCredentialsForOwnEndpoint(catalog, catalogOwnsEndpoint, catalogAccessKey)

        val accessKey = catalogAccessKey ?: inherited(CatalogStorageProperties.FS_ACCESS_KEY, hadoopFallback)
        val secretKey = catalogSecretKey ?: inherited(CatalogStorageProperties.FS_SECRET_KEY, hadoopFallback)

        val region =
            CatalogStorageProperties.REGION_KEYS
                .firstNotNullOfOrNull { catalogProperties[it]?.takeIf { value -> value.isNotBlank() } }
                ?: inherited(CatalogStorageProperties.FS_REGION, hadoopFallback)

        val pathStyleAccess =
            catalogProperties[CatalogStorageProperties.PATH_STYLE_ACCESS]?.takeIf { it.isNotBlank() }
                ?: "true".takeIf { catalogOwnsEndpoint }

        val sslEnabled =
            catalogProperties[CatalogStorageProperties.CONNECTION_SSL_ENABLED]?.takeIf { it.isNotBlank() }
                ?: endpoint?.let { (!it.startsWith("http://")).toString() }

        val overrides = buildMap {
            put("fs.s3.impl", CatalogStorageProperties.S3A_FILE_SYSTEM_CLASS)
            put("fs.s3n.impl", CatalogStorageProperties.S3A_FILE_SYSTEM_CLASS)

            // Avoid reusing a filesystem instance created with another storage configuration.
            CatalogStorageProperties.S3_SCHEMES.forEach { put("fs.$it.impl.disable.cache", "true") }

            pathStyleAccess?.let { put(CatalogStorageProperties.FS_PATH_STYLE_ACCESS, it) }
            sslEnabled?.let { put(CatalogStorageProperties.FS_SSL_ENABLED, it) }
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

        // An inherited platform session token must not be paired with catalog static credentials.
        val removedKeys =
            if (catalogAccessKey != null) setOf(CatalogStorageProperties.FS_SESSION_TOKEN) else emptySet()

        return CatalogStorageConfig(
            catalog = catalog,
            overrides = overrides,
            removedKeys = removedKeys,
        )
    }

    private fun inherited(
        key: String,
        hadoopFallback: (String) -> String?,
    ): String? {
        if (key !in CatalogStorageProperties.INHERITABLE_HADOOP_KEYS) {
            return null
        }
        return hadoopFallback(key)?.takeIf { it.isNotBlank() }
    }

    private fun requireNoSessionToken(
        catalog: String,
        catalogProperties: Map<String, String>,
    ) {
        if (catalogProperties[CatalogStorageProperties.SESSION_TOKEN]?.isNotBlank() != true) {
            return
        }

        throw CatalogStorageConfigurationException(
            catalog = catalog,
            message =
                "Catalog '$catalog' sets '${CatalogStorageProperties.SESSION_TOKEN}'. Temporary session credentials are " +
                    "not supported by this job. Configure static '${CatalogStorageProperties.ACCESS_KEY_ID}' and " +
                    "'${CatalogStorageProperties.SECRET_ACCESS_KEY}' credentials instead.",
        )
    }

    private fun requireCredentialsForOwnEndpoint(
        catalog: String,
        catalogOwnsEndpoint: Boolean,
        catalogAccessKey: String?,
    ) {
        if (!catalogOwnsEndpoint || catalogAccessKey != null) {
            return
        }

        throw CatalogStorageConfigurationException(
            catalog = catalog,
            message =
                "Catalog '$catalog' declares '${CatalogStorageProperties.ENDPOINT}' but no catalog credentials. " +
                    "Platform credentials are never used against a catalog-owned endpoint. Configure " +
                    "'${CatalogStorageProperties.ACCESS_KEY_ID}' and '${CatalogStorageProperties.SECRET_ACCESS_KEY}' " +
                    "on the catalog.",
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

    /** Returns the redacted configuration representation used for logging. */
    fun describe(config: CatalogStorageConfig): String = config.toString()
}
