package com.iomete.backup.config.internal

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.S3Config
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals

class UtilsTest {
    @Test
    fun `redactSecrets masks S3 accessKey and secretKey`() {
        val config =
            ApplicationConfig(
                source =
                    S3Config(
                        bucket = "source-bucket",
                        prefix = "data/",
                        accessKey = "AKIAIOSFODNN7EXAMPLE",
                        secretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
                    ),
                target =
                    S3Config(
                        bucket = "target-bucket",
                        prefix = "backup/",
                        accessKey = "AKIAI44QH8DHBEXAMPLE",
                        secretKey = "je7MtGbClwBF/2Zp9Utk/h3yCo8nvbEXAMPLEKEY",
                    ),
            )

        val redacted = Utils.redactSecrets(config)

        val source = redacted.source as S3Config
        assertEquals("source-bucket", source.bucket) // Non-sensitive preserved
        assertEquals("********", source.accessKey)
        assertEquals("********", source.secretKey)

        val target = redacted.target as S3Config
        assertEquals("target-bucket", target.bucket)
        assertEquals("********", target.accessKey)
        assertEquals("********", target.secretKey)
    }
}
