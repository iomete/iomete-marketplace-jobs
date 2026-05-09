package com.iomete.backup.config

import org.junit.jupiter.api.Test
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ConfigValidatorTest {

    // Helper functions to create test configs
    private fun s3Config(
        bucket: String = "test-bucket",
        prefix: String = "data/",
        accessKey: String = "access-key",
        secretKey: String = "secret-key",
        endpoint: String? = null,
        pathStyleAccess: Boolean = false
    ) = S3Config(
        bucket = bucket,
        prefix = prefix,
        accessKey = accessKey,
        secretKey = secretKey,
        endpoint = endpoint,
        pathStyleAccess = pathStyleAccess
    )

//    private fun hdfsConfig( #TODO
//        path: String = "/data/warehouse",
//        namenode: String? = "hdfs://namenode:8020",
//        ha: HaConfig? = null,
//        auth: AuthConfig = AuthConfig.Simple()
//    ) = HdfsConfig(
//        path = path,
//        namenode = namenode,
//        ha = ha,
//        auth = auth
//    )
//
//    private fun haConfig() = HaConfig(
//        nameservice = "mycluster",
//        namenodes = listOf("nn1", "nn2"),
//        rpcAddresses = mapOf(
//            "nn1" to "namenode1:8020",
//            "nn2" to "namenode2:8020"
//        )
//    )

    @Test
    fun `valid S3-to-S3 config passes validation`() {
        val config = ApplicationConfig(
            source = s3Config(bucket = "source-bucket"),
            target = s3Config(bucket = "target-bucket")
        )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Valid>(result)
    }

//    @Test
//    fun `valid S3-to-HDFS config passes validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig()
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Valid>(result)
//    }

//    @Test
//    fun `valid HDFS-to-S3 config passes validation`() {
//        val config = ApplicationConfig(
//            source = hdfsConfig(),
//            target = s3Config()
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Valid>(result)
//    }
//
//    @Test
//    fun `valid HDFS-to-HDFS config passes validation`() {
//        val config = ApplicationConfig(
//            source = hdfsConfig(namenode = "hdfs://source:8020"),
//            target = hdfsConfig(namenode = "hdfs://target:8020")
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Valid>(result)
//    }
//
//    @Test
//    fun `valid HDFS with HA config passes validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(namenode = null, ha = haConfig())
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Valid>(result)
//    }
//
//    @Test
//    fun `valid HDFS with Kerberos auth passes validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(
//                auth = AuthConfig.Kerberos(
//                    principal = "hdfs@EXAMPLE.COM",
//                    keytabPath = "/etc/keytabs/hdfs.keytab"
//                )
//            )
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Valid>(result)
//    }

    @Test
    fun `S3 with empty bucket fails validation`() {
        val config = ApplicationConfig(
            source = s3Config(bucket = ""),
            target = s3Config()
        )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("bucket") && it.contains("source") })
    }

    @Test
    fun `S3 with empty accessKey fails validation`() {
        val config = ApplicationConfig(
            source = s3Config(accessKey = ""),
            target = s3Config()
        )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("accessKey") && it.contains("source") })
    }

    @Test
    fun `S3 with empty secretKey fails validation`() {
        val config = ApplicationConfig(
            source = s3Config(),
            target = s3Config(secretKey = "")
        )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("secretKey") && it.contains("target") })
    }

    @Test
    fun `copy options with non-positive maxAttempts fails validation`() {
        val config = ApplicationConfig(
            source = s3Config(),
            target = s3Config(),
            copy = CopyConfig(options = CopyOptions(maxAttempts = 0))
        )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("maxAttempts") })
    }

    @Test
    fun `copy options with negative retryDelayMs fails validation`() {
        val config = ApplicationConfig(
            source = s3Config(),
            target = s3Config(),
            copy = CopyConfig(options = CopyOptions(retryDelayMs = -1))
        )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("retryDelayMs") })
    }

//    @Test
//    fun `HDFS with neither namenode nor HA fails validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(namenode = null, ha = null)
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.any {
//            it.contains("namenode") && it.contains("ha")
//        })
//    }
//
//    @Test
//    fun `HDFS with both namenode and HA fails validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(namenode = "hdfs://namenode:8020", ha = haConfig())
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.any {
//            it.contains("namenode") && it.contains("ha") && it.contains("both")
//        })
//    }
//
//    @Test
//    fun `HDFS with empty path fails validation`() {
//        val config = ApplicationConfig(
//            source = hdfsConfig(path = ""),
//            target = s3Config()
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.any { it.contains("path") && it.contains("source") })
//    }
//
//    @Test
//    fun `Kerberos auth with empty principal fails validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(
//                auth = AuthConfig.Kerberos(
//                    principal = "",
//                    keytabPath = "/etc/keytabs/hdfs.keytab"
//                )
//            )
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.any { it.contains("principal") })
//    }
//
//    @Test
//    fun `Kerberos auth with empty keytabPath fails validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(
//                auth = AuthConfig.Kerberos(
//                    principal = "hdfs@EXAMPLE.COM",
//                    keytabPath = ""
//                )
//            )
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.any { it.contains("keytabPath") })
//    }
//
//    @Test
//    fun `HA config with empty nameservice fails validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(
//                namenode = null,
//                ha = HaConfig(
//                    nameservice = "",
//                    namenodes = listOf("nn1", "nn2"),
//                    rpcAddresses = mapOf("nn1" to "host1:8020", "nn2" to "host2:8020")
//                )
//            )
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.any { it.contains("nameservice") })
//    }
//
//    @Test
//    fun `HA config with empty namenodes fails validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(
//                namenode = null,
//                ha = HaConfig(
//                    nameservice = "mycluster",
//                    namenodes = emptyList(),
//                    rpcAddresses = emptyMap()
//                )
//            )
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.any { it.contains("namenodes") })
//    }
//
//    @Test
//    fun `HA config with mismatched rpcAddresses fails validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = hdfsConfig(
//                namenode = null,
//                ha = HaConfig(
//                    nameservice = "mycluster",
//                    namenodes = listOf("nn1", "nn2"),
//                    rpcAddresses = mapOf("nn1" to "host1:8020") // missing nn2
//                )
//            )
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.any { it.contains("rpcAddresses") || it.contains("nn2") })
//    }
//
//    @Test
//    fun `multiple validation errors are collected`() {
//        val config = ApplicationConfig(
//            source = s3Config(bucket = "", accessKey = ""),
//            target = hdfsConfig(path = "", namenode = null, ha = null)
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Invalid>(result)
//        assertTrue(result.errors.size >= 3) // At least bucket, accessKey, and HDFS namenode/ha
//    }
//
//    @Test
//    fun `valid incremental copy config passes validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = s3Config(),
//            copy = CopyConfig(
//                mode = CopyMode.INCREMENTAL,
//                incrementalStrategy = IncrementalStrategy.CHECKSUM
//            )
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Valid>(result)
//    }
//
//    @Test
//    fun `copy options with valid values pass validation`() {
//        val config = ApplicationConfig(
//            source = s3Config(),
//            target = s3Config(),
//            copy = CopyConfig(
//                options = CopyOptions(
//                    maxMaps = 100,
//                    bandwidthMb = 500,
//                    numListStatusThreads = 10
//                )
//            )
//        )
//
//        val result = ConfigValidator.validate(config)
//
//        assertIs<ValidationResult.Valid>(result)
//    }
//
    @Test
    fun `copy options with zero maxMaps fails validation`() {
        val config = ApplicationConfig(
            source = s3Config(),
            target = s3Config(),
            copy = CopyConfig(
                options = CopyOptions(maxMaps = 0)
            )
        )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("maxMaps") })
    }

    @Test
    fun `copy options with negative maxMaps fails validation`() {
        val config = ApplicationConfig(
            source = s3Config(),
            target = s3Config(),
            copy = CopyConfig(
                options = CopyOptions(maxMaps = -1)
            )
        )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("maxMaps") })
    }
}
