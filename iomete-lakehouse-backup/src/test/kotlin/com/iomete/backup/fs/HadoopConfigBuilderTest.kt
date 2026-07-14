package com.iomete.backup.fs

import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class HadoopConfigBuilderTest {
    @Nested
    inner class S3ConfigTests {
        @Test
        fun `S3 config sets credentials and static s3a properties`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    pathStyleAccess = true,
                    accessKey = "myAccessKey",
                    secretKey = "mySecretKey",
                    region = "eu-west-1",
                )
            val conf = HadoopConfigBuilder.build(config)

            assertEquals("myAccessKey", conf.get("fs.s3a.access.key"))
            assertEquals("mySecretKey", conf.get("fs.s3a.secret.key"))
            assertEquals("eu-west-1", conf.get("fs.s3a.endpoint.region"))
            assertEquals("true", conf.get("fs.s3a.path.style.access"))
            assertEquals("org.apache.hadoop.fs.s3a.S3AFileSystem", conf.get("fs.s3a.impl"))
            assertEquals("true", conf.get("fs.s3a.impl.disable.cache"))
            assertEquals(
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
                conf.get("fs.s3a.aws.credentials.provider"),
            )
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
            val conf = HadoopConfigBuilder.build(config)

            assertEquals("https://s3.example.com", conf.get("fs.s3a.endpoint"))
        }

        @Test
        fun `S3 config omits endpoint when null`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    accessKey = "key",
                    secretKey = "secret",
                )
            val conf = HadoopConfigBuilder.build(config)

            assertNull(conf.get("fs.s3a.endpoint"))
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
            val conf = HadoopConfigBuilder.build(config)

            assertEquals("true", conf.get("fs.s3a.connection.ssl.enabled"))
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
            val conf = HadoopConfigBuilder.build(config)

            assertEquals("false", conf.get("fs.s3a.connection.ssl.enabled"))
        }

        @Test
        fun `S3 config enables SSL by default when endpoint is null`() {
            val config =
                S3Config(
                    bucket = "my-bucket",
                    accessKey = "key",
                    secretKey = "secret",
                )
            val conf = HadoopConfigBuilder.build(config)

            assertEquals("true", conf.get("fs.s3a.connection.ssl.enabled"))
        }

        @Test
        fun `same bucket S3 configs keep credentials isolated in separate configurations`() {
            val sourceConf =
                HadoopConfigBuilder.build(
                    S3Config(
                        bucket = "shared-bucket",
                        endpoint = "https://source.example.com",
                        accessKey = "source-key",
                        secretKey = "source-secret",
                    ),
                )
            val targetConf =
                HadoopConfigBuilder.build(
                    S3Config(
                        bucket = "shared-bucket",
                        endpoint = "https://target.example.com",
                        accessKey = "target-key",
                        secretKey = "target-secret",
                    ),
                )

            assertEquals("source-key", sourceConf.get("fs.s3a.access.key"))
            assertEquals("source-secret", sourceConf.get("fs.s3a.secret.key"))
            assertEquals("https://source.example.com", sourceConf.get("fs.s3a.endpoint"))
            assertEquals("target-key", targetConf.get("fs.s3a.access.key"))
            assertEquals("target-secret", targetConf.get("fs.s3a.secret.key"))
            assertEquals("https://target.example.com", targetConf.get("fs.s3a.endpoint"))
        }
    }

    @Nested
    inner class HdfsConfigTests {
        @Test
        fun `HDFS config sets defaultFS and authentication`() {
            val config =
                HdfsConfig(
                    namenode = "isilon.example.com:8020",
                    path = "backups",
                    authentication = "simple",
                    user = "isilon-user",
                )
            val conf = HadoopConfigBuilder.build(config)

            assertEquals("hdfs://isilon.example.com:8020", conf.get("fs.defaultFS"))
            assertEquals("simple", conf.get("hadoop.security.authentication"))
        }

        @Test
        fun `HDFS config does not set datanode hostname flag`() {
            val config =
                HdfsConfig(
                    namenode = "isilon.example.com:8020",
                    user = "isilon-user",
                )
            val conf = HadoopConfigBuilder.build(config)

            assertNull(conf.get("dfs.client.use.datanode.hostname"))
        }
    }
}
