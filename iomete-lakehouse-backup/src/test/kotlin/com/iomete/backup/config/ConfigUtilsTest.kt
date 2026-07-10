package com.iomete.backup.config

import com.github.dockerjava.api.model.AuthConfig
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class ConfigUtilsTest {

    @Test
    fun `redactSecrets masks S3 accessKey and secretKey`() {
        val config = ApplicationConfig(
            source = S3Config(
                bucket = "source-bucket",
                prefix = "data/",
                accessKey = "AKIAIOSFODNN7EXAMPLE",
                secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
            ),
            target = S3Config(
                bucket = "target-bucket",
                prefix = "backup/",
                accessKey = "AKIAI44QH8DHBEXAMPLE",
                secretKey = "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY"
            )
        )

        val redacted = ConfigUtils.redactSecrets(config)

        val source = redacted.source as S3Config
        assertEquals("source-bucket", source.bucket) // Non-sensitive preserved
        assertEquals("***", source.accessKey)
        assertEquals("***", source.secretKey)

        val target = redacted.target as S3Config
        assertEquals("target-bucket", target.bucket)
        assertEquals("***", target.accessKey)
        assertEquals("***", target.secretKey)
    }


//    @Test #TODO
//    fun `redactSecrets masks Kerberos keytabPath`() {
//        val config = ApplicationConfig(
//            source = S3Config(
//                bucket = "source-bucket",
//                accessKey = "key",
//                secretKey = "secret"
//            ),
//            target = HdfsConfig(
//                path = "/backups/warehouse",
//                namenode = "hdfs://namenode:8020",
//                auth = AuthConfig.Kerberos(
//                    principal = "hdfs@EXAMPLE.COM",
//                    keytabPath = "/etc/security/keytabs/hdfs.keytab"
//                )
//            )
//        )
//
//        val redacted = ConfigUtils.redactSecrets(config)
//
//        val target = redacted.target as HdfsConfig
//        assertEquals("/backups/warehouse", target.path) // Non-sensitive preserved
//
//        val auth = target.auth as AuthConfig.Kerberos
//        assertEquals("hdfs@EXAMPLE.COM", auth.principal) // Principal is not secret
//        assertEquals("***", auth.keytabPath)
//    }

//    @Test
//    fun `redactSecrets preserves Simple auth unchanged`() {
//        val config = ApplicationConfig(
//            source = HdfsConfig(
//                path = "/data/warehouse",
//                namenode = "hdfs://namenode:8020",
//                auth = AuthConfig.Simple(user = "hadoop")
//            ),
//            target = S3Config(
//                bucket = "target-bucket",
//                accessKey = "key",
//                secretKey = "secret"
//            )
//        )
//
//        val redacted = ConfigUtils.redactSecrets(config)
//
//        val source = redacted.source as HdfsConfig
//        assertIs<AuthConfig.Simple>(source.auth)
//        assertEquals("hadoop", (source.auth as AuthConfig.Simple).user)
//    }

//    @Test
//    fun `redactSecrets preserves copy config unchanged`() {
//        val config = ApplicationConfig(
//            source = S3Config(bucket = "src", accessKey = "key", secretKey = "secret"),
//            target = S3Config(bucket = "dst", accessKey = "key", secretKey = "secret"),
//            copy = CopyConfig(
//                mode = CopyMode.INCREMENTAL,
//                incrementalStrategy = IncrementalStrategy.CHECKSUM,
//                options = CopyOptions(maxMaps = 100, bandwidthMb = 500)
//            )
//        )
//
//        val redacted = ConfigUtils.redactSecrets(config)
//
//        assertEquals(CopyMode.INCREMENTAL, redacted.copy.mode)
//        assertEquals(IncrementalStrategy.CHECKSUM, redacted.copy.incrementalStrategy)
//        assertEquals(100, redacted.copy.options.maxMaps)
//        assertEquals(500, redacted.copy.options.bandwidthMb)
//    }
}