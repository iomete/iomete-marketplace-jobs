package com.iomete.backup.config

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo

data class ApplicationConfig(
    val source: StorageConfig,
    val target: StorageConfig,
    val copy: CopyConfig = CopyConfig(),
    val stats: StatsConfig = StatsConfig(),
)

data class StatsConfig(
    val enabled: Boolean = true,
    val database: String = "spark_catalog.iomete_system_db",
    val maxFailureRows: Int = 1000,
)

data class CopyConfig(
    val skipIdentical: Boolean = true,
    // Only treat a target copy as identical when it is newer than the source by this margin: the
    // two clocks are independent and S3 truncates to whole seconds.
    val clockSkewToleranceMs: Long = 30L * 1000,
    // Copies are network-bound, so oversubscribing vCPUs is deliberate.
    val slotsPerVcpu: Int = 4,
    val tasksPerSlot: Int = 20,
    // Fixed cost of copying one file, in bytes of equivalent transfer: measured at ~26 MiB on S3.
    val perFileOverheadBytes: Long = 25L * 1024 * 1024,
    val maxBytesPerTask: Long = 1024L * 1024 * 1024,
    // Aggregate ceiling across every executor, unlike DistCp's per-map -bandwidth. Null is uncapped.
    val maxBandwidthMbPerSec: Double? = null,
    // Period granularity of the folder appended to the target root; null writes to the root itself.
    val targetTimestampFolder: String? = null,
)

data class InternalConfig(
    val bytesPerSecPerExecutor: Double? = null,
    val executorCount: Int,
    val vcpuPerExecutor: Double,
    val slotsPerExecutor: Int = 1,
) {
    val slots: Int get() = executorCount * slotsPerExecutor
}

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

    abstract fun withSubFolder(name: String): StorageConfig
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

    override fun withSubFolder(name: String): S3Config = copy(prefix = "${prefix.trim('/')}/$name".trimStart('/'))
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

    override fun withSubFolder(name: String): HdfsConfig = copy(path = "${path.trim('/')}/$name".trimStart('/'))
}
