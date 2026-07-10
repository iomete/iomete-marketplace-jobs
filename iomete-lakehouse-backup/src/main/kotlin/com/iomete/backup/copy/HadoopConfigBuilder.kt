package com.iomete.backup.copy

import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig
import org.apache.hadoop.conf.Configuration

object HadoopConfigBuilder {

    fun buildConfigMap(config: StorageConfig): Map<String, String> {
        return when (config) {
            is S3Config -> buildS3ConfigMap(config)
//            is HdfsConfig -> buildHdfsConfigMap(config) #TODO
        }
    }

    private fun buildS3ConfigMap(config: S3Config): Map<String, String> {
        val props = mutableMapOf<String, String>()

        // Use S3A filesystem implementation
        props["fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"
        props["fs.s3a.impl.disable.cache"] = "true"

        config.endpoint?.let { endpoint ->
            props["fs.s3a.endpoint"] = endpoint
        }

        props["fs.s3a.access.key"] = config.accessKey
        props["fs.s3a.secret.key"] = config.secretKey
        props["fs.s3a.path.style.access"] = config.pathStyleAccess.toString()
        props["fs.s3a.endpoint.region"] = config.region
        props["fs.s3a.aws.credentials.provider"] = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        // Connection settings for robustness
        props["fs.s3a.connection.ssl.enabled"] =
            (config.endpoint?.startsWith("https") ?: true).toString()

        return props
    }

    fun toHadoopConf(props: Map<String, String>): Configuration {
        val conf = Configuration()
        props.forEach { (key, value) -> conf[key] = value }
        return conf
    }


}