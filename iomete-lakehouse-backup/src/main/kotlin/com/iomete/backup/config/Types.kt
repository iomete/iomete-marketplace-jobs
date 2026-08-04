package com.iomete.backup.config

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

data class ApplicationConfig(
    val source: StorageConfig,
    val target: StorageConfig,
    val copy: CopyConfig = CopyConfig(),
)

data class CopyConfig(
    val skipIdentical: Boolean = true,
    // Only treat a target copy as identical when it is newer than the source by this margin: the
    // two clocks are independent and S3 truncates to whole seconds.
    val clockSkewToleranceMs: Long = 30L * 1000,
    val bytesPerTask: Long = 1024L * 1024 * 1024,
    // A byte target alone does not bound task cost: every file pays a fresh filesystem build, so a
    // batch of tiny files runs long while staying far below the byte target.
    val filesPerTask: Int = 1000,
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
)
@JsonSubTypes(
    JsonSubTypes.Type(value = S3Config::class, name = "s3"),
    JsonSubTypes.Type(value = HdfsConfig::class, name = "hdfs"),
)
sealed class StorageConfig : java.io.Serializable {
    abstract val rootUri: String

    // Test seam only: Validator rejects it on the load path, so it is reachable
    // solely by callers driving BackupJob directly.
    abstract val hadoopOptions: Map<String, String>
}

data class S3Config(
    val bucket: String,
    val prefix: String = "",
    val endpoint: String? = null,
    val pathStyleAccess: Boolean = false,
    val accessKey: String,
    val secretKey: String,
    val region: String = "us-east-1",
    override val hadoopOptions: Map<String, String> = emptyMap(),
) : StorageConfig() {
    override val rootUri: String
        get() {
            val trimmedPrefix = prefix.trim('/')
            return if (trimmedPrefix.isEmpty()) "s3a://$bucket" else "s3a://$bucket/$trimmedPrefix"
        }
}

data class HdfsConfig(
    val namenode: String,
    val path: String = "",
    val authentication: String = "simple",
    val user: String,
    override val hadoopOptions: Map<String, String> = emptyMap(),
) : StorageConfig() {
    override val rootUri: String
        get() {
            val trimmedPath = path.trim('/')
            return if (trimmedPath.isEmpty()) "hdfs://$namenode" else "hdfs://$namenode/$trimmedPath"
        }
}
