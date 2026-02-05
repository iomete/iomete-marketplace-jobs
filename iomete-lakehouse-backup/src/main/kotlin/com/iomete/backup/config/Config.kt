package com.iomete.backup.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

/**
 * Root application configuration for the backup job.
 */
data class ApplicationConfig(
    val source: StorageConfig,
    val target: StorageConfig,
    val copy: CopyConfig = CopyConfig()
)

/**
 * Sealed class representing storage configuration.
 * Can be either S3 or HDFS.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = S3Config::class, name = "s3"),
    JsonSubTypes.Type(value = HdfsConfig::class, name = "hdfs")
)
sealed class StorageConfig

/**
 * S3/ECS storage configuration.
 */
data class S3Config(
    val bucket: String,
    val prefix: String = "",
    val endpoint: String? = null,
    val pathStyleAccess: Boolean = false,
    val accessKey: String,
    val secretKey: String
) : StorageConfig()

/**
 * HDFS storage configuration.
 */
data class HdfsConfig(
    val path: String,
    val namenode: String? = null,
    val ha: HaConfig? = null,
    val auth: AuthConfig = AuthConfig.Simple()
) : StorageConfig()

/**
 * HDFS High Availability configuration.
 */
data class HaConfig(
    val nameservice: String,
    val namenodes: List<String>,
    val rpcAddresses: Map<String, String>
)

/**
 * Authentication configuration for HDFS.
 */
@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type"
)
@JsonSubTypes(
    JsonSubTypes.Type(value = AuthConfig.Simple::class, name = "simple"),
    JsonSubTypes.Type(value = AuthConfig.Kerberos::class, name = "kerberos")
)
sealed class AuthConfig {
    /**
     * Simple authentication with username.
     */
    data class Simple(
        val user: String = "hdfs"
    ) : AuthConfig()

    /**
     * Kerberos authentication with principal and keytab.
     */
    data class Kerberos(
        val principal: String,
        val keytabPath: String
    ) : AuthConfig()
}

/**
 * Copy operation configuration.
 */
data class CopyConfig(
    val mode: CopyMode = CopyMode.FULL,
    val incrementalStrategy: IncrementalStrategy = IncrementalStrategy.MTIME,
    val options: CopyOptions = CopyOptions()
)

/**
 * Copy mode: full or incremental.
 */
enum class CopyMode {
    @JsonProperty("full")
    FULL,

    @JsonProperty("incremental")
    INCREMENTAL
}

/**
 * Strategy for incremental copy comparison.
 */
enum class IncrementalStrategy {
    @JsonProperty("mtime")
    MTIME,

    @JsonProperty("checksum")
    CHECKSUM
}

/**
 * Detailed copy options.
 */
data class CopyOptions(
    val skipCrcCheck: Boolean = false,
    val ignoreFailures: Boolean = false,
    val maxMaps: Int = 20,
    val bandwidthMb: Int? = null,
    val numListStatusThreads: Int = 1
)
