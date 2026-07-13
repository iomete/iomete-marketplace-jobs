package com.iomete.backup.config

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

data class ApplicationConfig(
    val source: StorageConfig,
    val target: StorageConfig,
    val copy: CopyConfig = CopyConfig(),
)

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
)
@JsonSubTypes(
    JsonSubTypes.Type(value = S3Config::class, name = "s3"),
//    JsonSubTypes.Type(value = HdfsConfig::class, name = "hdfs") #TODO
)
sealed class StorageConfig

data class S3Config(
    val bucket: String,
    val prefix: String = "",
    val endpoint: String? = null,
    val pathStyleAccess: Boolean = false,
    val accessKey: String,
    val secretKey: String,
    val region: String = "us-east-1",
) : StorageConfig()

data class CopyConfig(
//    val mode: CopyMode = CopyMode.FULL, #TODO
//    val incrementalStrategy: IncrementalStrategy = IncrementalStrategy.MTIME,
    val options: CopyOptions = CopyOptions(), // TODO: can we get rid of this extra class? why not have it flat ?
)

data class CopyOptions(
//    val skipCrcCheck: Boolean = false,
//    val ignoreFailures: Boolean = false,
    val maxMaps: Int = 20,
    val maxAttempts: Int = 3,
    val retryDelayMs: Long = 1000L,
//    val bandwidthMb: Int? = null,
//    val numListStatusThreads: Int = 1
)
