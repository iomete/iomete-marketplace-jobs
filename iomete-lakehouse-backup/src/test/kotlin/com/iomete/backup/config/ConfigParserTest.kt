package com.iomete.backup.config

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull

class ConfigParserTest {
    @Test
    fun `parse minimal S3-to-S3 config`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "prefix": "data/",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "prefix": "backup/",
                "accessKey": "access789",
                "secretKey": "secret012"
              }
            }
            """.trimIndent()

        val config = ConfigParser.parse(json)

        assertIs<S3Config>(config.source)
        assertIs<S3Config>(config.target)

        val source = config.source as S3Config
        assertEquals("source-bucket", source.bucket)
        assertEquals("data/", source.prefix)
        assertEquals("access123", source.accessKey)
        assertEquals("secret456", source.secretKey)
        assertEquals(false, source.pathStyleAccess) // default
        assertNull(source.endpoint) // optional

        val target = config.target as S3Config
        assertEquals("target-bucket", target.bucket)
        assertEquals("backup/", target.prefix)
    }

    @Test
    fun `reject malformed JSON`() {
        val malformedJson =
            """
            {
              "source": {
                "type": "s3"
                "bucket": "missing-comma"
              }
            }
            """.trimIndent()

        assertThrows<ConfigParseException> {
            ConfigParser.parse(malformedJson)
        }
    }

    @Test
    fun `reject config with missing required source`() {
        val json =
            """
            {
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "key",
                "secretKey": "secret"
              }
            }
            """.trimIndent()

        assertThrows<ConfigParseException> {
            ConfigParser.parse(json)
        }
    }

    @Test
    fun `reject config with unknown storage type`() {
        val json =
            """
            {
              "source": {
                "type": "gcs",
                "bucket": "source-bucket"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "key",
                "secretKey": "secret"
              }
            }
            """.trimIndent()

        assertThrows<ConfigParseException> {
            ConfigParser.parse(json)
        }
    }
}
