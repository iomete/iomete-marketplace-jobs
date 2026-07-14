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
)
sealed class StorageConfig : java.io.Serializable {
    abstract val rootUri: String
}

data class S3Config(
    val bucket: String,
    val prefix: String = "",
    val endpoint: String? = null,
    val pathStyleAccess: Boolean = false,
    val accessKey: String,
    val secretKey: String,
    val region: String = "us-east-1",
) : StorageConfig() {
    override val rootUri: String
        get() {
            val trimmedPrefix = prefix.trim('/')
            return if (trimmedPrefix.isEmpty()) "s3a://$bucket" else "s3a://$bucket/$trimmedPrefix"
        }
}
