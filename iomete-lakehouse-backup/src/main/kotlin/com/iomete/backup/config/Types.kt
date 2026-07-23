package com.iomete.backup.config

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

data class ApplicationConfig(
    val source: StorageConfig,
    val target: StorageConfig,
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
