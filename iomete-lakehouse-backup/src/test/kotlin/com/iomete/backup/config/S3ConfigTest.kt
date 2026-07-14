package com.iomete.backup.config

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class S3ConfigTest {
    @Test
    fun `S3 config with prefix produces s3a URI`() {
        val config =
            S3Config(
                bucket = "my-bucket",
                prefix = "data/warehouse/",
                accessKey = "key",
                secretKey = "secret",
            )
        assertEquals("s3a://my-bucket/data/warehouse", config.rootUri)
    }

    @Test
    fun `S3 config without prefix produces bucket-only URI`() {
        val config =
            S3Config(
                bucket = "my-bucket",
                prefix = "",
                accessKey = "key",
                secretKey = "secret",
            )
        assertEquals("s3a://my-bucket", config.rootUri)
    }

    @Test
    fun `S3 config trims leading and trailing slashes from prefix`() {
        val config =
            S3Config(
                bucket = "my-bucket",
                prefix = "/data/warehouse/",
                accessKey = "key",
                secretKey = "secret",
            )
        assertEquals("s3a://my-bucket/data/warehouse", config.rootUri)
    }
}
