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
        pathStyleAccess: Boolean = false,
    ) = S3Config(
        bucket = bucket,
        prefix = prefix,
        accessKey = accessKey,
        secretKey = secretKey,
        endpoint = endpoint,
        pathStyleAccess = pathStyleAccess,
    )

    @Test
    fun `valid S3-to-S3 config passes validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(bucket = "source-bucket"),
                target = s3Config(bucket = "target-bucket"),
            )

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Valid>(result)
    }

    @Test
    fun `S3 with empty bucket fails validation`() {
        val config =
            ApplicationConfig(
                source = s3Config(bucket = ""),
                target = s3Config(),
            )

        val result = ConfigValidator.validate(config)

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

        val result = ConfigValidator.validate(config)

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

        val result = ConfigValidator.validate(config)

        assertIs<ValidationResult.Invalid>(result)
        assertTrue(result.errors.any { it.contains("secretKey") && it.contains("target") })
    }
}
