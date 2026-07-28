package com.iomete.backup.fs

import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig
import org.apache.hadoop.conf.Configuration

object HadoopConfigBuilder {
    fun build(config: StorageConfig): Configuration {
        val conf = Configuration()
        configMap(config).forEach { (key, value) -> conf[key] = value }
        config.hadoopOptions.forEach { (key, value) -> conf[key] = value }
        return conf
    }

    private fun configMap(config: StorageConfig): Map<String, String> =
        when (config) {
            is S3Config -> s3ConfigMap(config)
            is HdfsConfig -> hdfsConfigMap(config)
        }

    private fun s3ConfigMap(config: S3Config): Map<String, String> =
        buildMap {
            put("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            put("fs.s3a.impl.disable.cache", "true")
            config.endpoint?.let { put("fs.s3a.endpoint", it) }
            put("fs.s3a.access.key", config.accessKey)
            put("fs.s3a.secret.key", config.secretKey)
            put("fs.s3a.path.style.access", config.pathStyleAccess.toString())
            put("fs.s3a.endpoint.region", config.region)
            put("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            put("fs.s3a.connection.ssl.enabled", (config.endpoint?.startsWith("https") ?: true).toString())
        }

    private fun hdfsConfigMap(config: HdfsConfig): Map<String, String> =
        buildMap {
            put("fs.defaultFS", "hdfs://${config.namenode}")
            put("hadoop.security.authentication", config.authentication)
        }
}
