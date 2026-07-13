package com.iomete.backup.copy

import com.iomete.backup.config.S3Config
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull

class HadoopConfigBuilderTest {
    @Nested
    inner class S3ConfigTests {
        @Test
        fun `S3 config sets access key and secret key`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    accessKey = "myAccessKey",
                    secretKey = "mySecretKey",
                )
            val props = HadoopConfigBuilder.buildConfigMap(config)

            assertEquals("myAccessKey", props["fs.s3a.access.key"])
            assertEquals("mySecretKey", props["fs.s3a.secret.key"])
        }

        @Test
        fun `S3 config sets endpoint when provided`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    endpoint = "https://s3.example.com",
                    accessKey = "key",
                    secretKey = "secret",
                )
            val props = HadoopConfigBuilder.buildConfigMap(config)

            assertEquals("https://s3.example.com", props["fs.s3a.endpoint"])
        }

        @Test
        fun `S3 config omits endpoint when null`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    accessKey = "key",
                    secretKey = "secret",
                )
            val props = HadoopConfigBuilder.buildConfigMap(config)

            assertFalse(props.containsKey("fs.s3a.endpoint"))
        }

        @Test
        fun `S3 config sets path style access`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    pathStyleAccess = true,
                    accessKey = "key",
                    secretKey = "secret",
                )
            val props = HadoopConfigBuilder.buildConfigMap(config)

            assertEquals("true", props["fs.s3a.path.style.access"])
        }

        @Test
        fun `S3 config sets S3A filesystem implementation`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    accessKey = "key",
                    secretKey = "secret",
                )
            val props = HadoopConfigBuilder.buildConfigMap(config)

            assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", props["fs.s3a.impl"])
        }

        @Test
        fun `S3 config disables filesystem cache`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    accessKey = "key",
                    secretKey = "secret",
                )
            val props = HadoopConfigBuilder.buildConfigMap(config)

            assertEquals("true", props["fs.s3a.impl.disable.cache"])
        }

        @Test
        fun `S3 config enables SSL for https endpoint`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    endpoint = "https://s3.example.com",
                    accessKey = "key",
                    secretKey = "secret",
                )
            val props = HadoopConfigBuilder.buildConfigMap(config)

            assertEquals("true", props["fs.s3a.connection.ssl.enabled"])
        }

        @Test
        fun `S3 config disables SSL for http endpoint`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    endpoint = "http://s3.example.com",
                    accessKey = "key",
                    secretKey = "secret",
                )
            val props = HadoopConfigBuilder.buildConfigMap(config)

            assertEquals("false", props["fs.s3a.connection.ssl.enabled"])
        }

        @Test
        fun `same bucket S3 configs keep credentials isolated in separate maps`() {
            val sourceProps =
                HadoopConfigBuilder.buildConfigMap(
                    S3Config(
                        bucket = "shared-bucket",
                        endpoint = "https://source.example.com",
                        accessKey = "source-key",
                        secretKey = "source-secret",
                    ),
                )
            val targetProps =
                HadoopConfigBuilder.buildConfigMap(
                    S3Config(
                        bucket = "shared-bucket",
                        endpoint = "https://target.example.com",
                        accessKey = "target-key",
                        secretKey = "target-secret",
                    ),
                )

            assertEquals("source-key", sourceProps["fs.s3a.access.key"])
            assertEquals("source-secret", sourceProps["fs.s3a.secret.key"])
            assertEquals("https://source.example.com", sourceProps["fs.s3a.endpoint"])
            assertEquals("target-key", targetProps["fs.s3a.access.key"])
            assertEquals("target-secret", targetProps["fs.s3a.secret.key"])
            assertEquals("https://target.example.com", targetProps["fs.s3a.endpoint"])
        }
    }

    // ---- HDFS config ----

//    @Nested
//    inner class HdfsConfigTests {
//
//        @Test
//        fun `HDFS config with namenode sets fs defaultFS`() {
//            val config = HdfsConfig(
//                path = "/data/warehouse",
//                namenode = "hdfs://namenode:8020"
//            )
//            val props = HadoopConfigBuilder.buildConfigMap(config)
//
//            assertEquals("hdfs://namenode:8020", props["fs.defaultFS"])
//        }
//
//        @Test
//        fun `HDFS config with simple auth sets authentication type and user`() {
//            val config = HdfsConfig(
//                path = "/data",
//                namenode = "hdfs://nn:8020",
//                auth = AuthConfig.Simple(user = "hadoop-user")
//            )
//            val props = HadoopConfigBuilder.buildConfigMap(config)
//
//            assertEquals("simple", props["hadoop.security.authentication"])
//            assertEquals("hadoop-user", props["HADOOP_USER_NAME"])
//        }
//
//        @Test
//        fun `HDFS config with Kerberos auth sets principal and keytab`() {
//            val config = HdfsConfig(
//                path = "/data",
//                namenode = "hdfs://nn:8020",
//                auth = AuthConfig.Kerberos(
//                    principal = "hdfs-backup@EXAMPLE.COM",
//                    keytabPath = "/etc/keytabs/hdfs.keytab"
//                )
//            )
//            val props = HadoopConfigBuilder.buildConfigMap(config)
//
//            assertEquals("kerberos", props["hadoop.security.authentication"])
//            assertEquals("hdfs-backup@EXAMPLE.COM", props["dfs.namenode.kerberos.principal"])
//            assertEquals("/etc/keytabs/hdfs.keytab", props["hadoop.security.keytab.file"])
//        }
//
//        @Test
//        fun `HDFS config with HA sets nameservice and namenode properties`() {
//            val config = HdfsConfig(
//                path = "/data",
//                ha = HaConfig(
//                    nameservice = "mycluster",
//                    namenodes = listOf("nn1", "nn2"),
//                    rpcAddresses = mapOf(
//                        "nn1" to "namenode1.example.com:8020",
//                        "nn2" to "namenode2.example.com:8020"
//                    )
//                )
//            )
//            val props = HadoopConfigBuilder.buildConfigMap(config)
//
//            assertEquals("hdfs://mycluster", props["fs.defaultFS"])
//            assertEquals("mycluster", props["dfs.nameservices"])
//            assertEquals("nn1,nn2", props["dfs.ha.namenodes.mycluster"])
//            assertEquals("namenode1.example.com:8020", props["dfs.namenode.rpc-address.mycluster.nn1"])
//            assertEquals("namenode2.example.com:8020", props["dfs.namenode.rpc-address.mycluster.nn2"])
//            assertEquals(
//                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider",
//                props["dfs.client.failover.proxy.provider.mycluster"]
//            )
//        }
//    }

    // ---- toHadoopConf ----

    @Nested
    inner class ToHadoopConfTests {
        @Test
        fun `toHadoopConf reconstructs Configuration from map`() {
            val props =
                mapOf(
                    "fs.defaultFS" to "hdfs://namenode:8020",
                    "custom.property" to "custom-value",
                )
            val conf = HadoopConfigBuilder.toHadoopConf(props)

            assertEquals("hdfs://namenode:8020", conf.get("fs.defaultFS"))
            assertEquals("custom-value", conf.get("custom.property"))
        }

        @Test
        fun `toHadoopConf with empty map returns default Configuration`() {
            val conf = HadoopConfigBuilder.toHadoopConf(emptyMap())

            // Should not throw; returns a valid Configuration with defaults
            assertNull(conf.get("fs.s3a.endpoint"))
        }
    }
}
