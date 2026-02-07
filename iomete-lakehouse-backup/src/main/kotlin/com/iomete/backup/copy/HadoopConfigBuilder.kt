package com.iomete.backup.copy

import com.iomete.backup.config.AuthConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StorageConfig
import org.apache.hadoop.conf.Configuration

/**
 * Builds Hadoop [Configuration] property maps from [StorageConfig].
 *
 * Produces a serializable [Map] so that Spark executors can reconstruct
 * a [Configuration] without shipping the full object graph.
 */
object HadoopConfigBuilder {

    /**
     * Convert a [StorageConfig] into a flat map of Hadoop configuration properties.
     *
     * For S3: sets fs.s3a.* properties (endpoint, credentials, path-style).
     * For HDFS: sets fs.defaultFS, optional HA properties, and authentication.
     */
    fun buildConfigMap(config: StorageConfig): Map<String, String> {
        return when (config) {
            is S3Config -> buildS3ConfigMap(config)
            is HdfsConfig -> buildHdfsConfigMap(config)
        }
    }

    /**
     * Reconstruct a Hadoop [Configuration] from a property map.
     * Intended for use on Spark executors.
     */
    fun toHadoopConf(props: Map<String, String>): Configuration {
        val conf = Configuration()
        props.forEach { (key, value) -> conf.set(key, value) }
        return conf
    }

    private fun buildS3ConfigMap(config: S3Config): Map<String, String> {
        val props = mutableMapOf<String, String>()

        // Use S3A filesystem implementation
        props["fs.s3a.impl"] = "org.apache.hadoop.fs.s3a.S3AFileSystem"

        config.endpoint?.let { endpoint ->
            props["fs.s3a.endpoint"] = endpoint
        }

        props["fs.s3a.access.key"] = config.accessKey
        props["fs.s3a.secret.key"] = config.secretKey
        props["fs.s3a.path.style.access"] = config.pathStyleAccess.toString()

        // Connection settings for robustness
        props["fs.s3a.connection.ssl.enabled"] =
            (config.endpoint?.startsWith("https") ?: true).toString()

        return props
    }

    private fun buildHdfsConfigMap(config: HdfsConfig): Map<String, String> {
        val props = mutableMapOf<String, String>()

        if (config.ha != null) {
            val ha = config.ha
            val ns = ha.nameservice

            props["fs.defaultFS"] = "hdfs://$ns"
            props["dfs.nameservices"] = ns
            props["dfs.ha.namenodes.$ns"] = ha.namenodes.joinToString(",")

            ha.rpcAddresses.forEach { (nn, address) ->
                props["dfs.namenode.rpc-address.$ns.$nn"] = address
            }

            props["dfs.client.failover.proxy.provider.$ns"] =
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        } else if (config.namenode != null) {
            props["fs.defaultFS"] = config.namenode
        }

        // Authentication
        when (val auth = config.auth) {
            is AuthConfig.Simple -> {
                props["hadoop.security.authentication"] = "simple"
                props["HADOOP_USER_NAME"] = auth.user
            }
            is AuthConfig.Kerberos -> {
                props["hadoop.security.authentication"] = "kerberos"
                props["dfs.namenode.kerberos.principal"] = auth.principal
                props["hadoop.security.keytab.file"] = auth.keytabPath
            }
        }

        return props
    }
}
