package com.iomete.backup.config.internal

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.CopyConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import org.junit.jupiter.api.Test
import kotlin.test.assertIs
import kotlin.test.assertTrue

class ValidatorTest {
    // Helper functions to create test configs
    private fun s3Config(
        bucket: String = "test-bucket",
        prefix: String = "data/",
        accessKey: String = "access-key",
        secretKey: String = "secret-key",
        endpoint: String? = null,
        pathStyleAccess: Boolean = false,
    ) = S3Config(
        bucket = bucket,
        prefix = prefix,
        accessKey = accessKey,
        secretKey = secretKey,
        endpoint = endpoint,
        pathStyleAccess = pathStyleAccess,
    )

    private fun hdfsConfig(
        namenode: String = "isilon.example.com:8020",
        path: String = "backups",
        authentication: String = "simple",
        user: String = "isilon-user",
    ) = HdfsConfig(
        namenode = namenode,
        path = path,
        authentication = authentication,
        user = user,
    )

    @Test
    fun `valid S3-to-S3 config passes validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(bucket = "source-bucket"),
                target = s3Config(bucket = "target-bucket"),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Valid>(result)
    }

    @Test
    fun `S3 with empty bucket fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(bucket = ""),
                target = s3Config(),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("bucket") && it.contains("source") })
    }

    @Test
    fun `S3 with empty accessKey fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(accessKey = ""),
                target = s3Config(),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("accessKey") && it.contains("source") })
    }

    @Test
    fun `S3 with empty secretKey fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(),
                target = s3Config(secretKey = ""),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("secretKey") && it.contains("target") })
    }

    @Test
    fun `valid S3-to-HDFS config passes validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(bucket = "source-bucket"),
                target = hdfsConfig(),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Valid>(result)
    }

    @Test
    fun `HDFS with blank namenode fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(),
                target = hdfsConfig(namenode = ""),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("namenode") && it.contains("target") })
    }

    @Test
    fun `HDFS with blank user fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(),
                target = hdfsConfig(user = ""),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("user") && it.contains("target") })
    }

    @Test
    fun `S3 with hadoopOptions fails validation`() {
        val config =
            ApplicationConfig(
                source = S3Config(bucket = "b", accessKey = "k", secretKey = "s", hadoopOptions = mapOf("fs.s3a.impl" to "x")),
                target = s3Config(),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("hadoopOptions") && it.contains("source") })
    }

    @Test
    fun `HDFS with hadoopOptions fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(),
                target = HdfsConfig(namenode = "nn:8020", user = "u", hadoopOptions = mapOf("fs.hdfs.impl" to "x")),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("hadoopOptions") && it.contains("target") })
    }

    @Test
    fun `HDFS with unsupported authentication fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(),
                target = hdfsConfig(authentication = "kerberos"),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("authentication") && it.contains("target") })
    }

    @Test
    fun `a non-positive bandwidth cap fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(),
                target = s3Config(),
                copy = CopyConfig(maxBandwidthMbPerSec = 0.0),
            )

        val result = Validator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("maxBandwidthMbPerSec") })
    }

    @Test
    fun `a positive bandwidth cap passes validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(),
                target = s3Config(),
                copy = CopyConfig(maxBandwidthMbPerSec = 600.0),
            )

        assertIs<ValidationResult.Valid>(Validator.validate(config))
    }
}
