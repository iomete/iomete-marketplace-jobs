package com.iomete.backup.config.internal

import com.iomete.backup.config.ConfigParseException
import com.iomete.backup.config.CopyConfig
import com.iomete.backup.config.HdfsConfig
import com.iomete.backup.config.S3Config
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull

class ParserTest {
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

        val config = Parser.parse(json)

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
    fun `parse copy block binds clock skew tolerance`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "access789",
                "secretKey": "secret012"
              },
              "copy": {
                "clockSkewToleranceMs": 0
              }
            }
            """.trimIndent()

        assertEquals(0, Parser.parse(json).copy.clockSkewToleranceMs)
    }

    @Test
    fun `parse copy block binds the skip-identical flag`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "access789",
                "secretKey": "secret012"
              },
              "copy": {
                "skipIdentical": false
              }
            }
            """.trimIndent()

        assertEquals(false, Parser.parse(json).copy.skipIdentical)
    }

    @Test
    fun `parse copy block binds the concurrency settings`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "access789",
                "secretKey": "secret012"
              },
              "copy": {
                "slotsPerVcpu": 4,
                "tasksPerSlot": 8,
                "perFileOverheadBytes": 1024,
                "maxBytesPerTask": 2048
              }
            }
            """.trimIndent()

        val copy = Parser.parse(json).copy

        assertEquals(4, copy.slotsPerVcpu)
        assertEquals(8, copy.tasksPerSlot)
        assertEquals(1024, copy.perFileOverheadBytes)
        assertEquals(2048, copy.maxBytesPerTask)
    }

    @Test
    fun `a copy block that still sets filesPerTask loads, and the setting does nothing`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "access789",
                "secretKey": "secret012"
              },
              "copy": {
                "filesPerTask": 250,
                "bytesPerTask": 1000
              }
            }
            """.trimIndent()

        assertEquals(CopyConfig(), Parser.parse(json).copy)
    }

    @Test
    fun `parse config without copy block defaults to skipping identical files`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "access789",
                "secretKey": "secret012"
              }
            }
            """.trimIndent()

        assertEquals(true, Parser.parse(json).copy.skipIdentical)
    }

    @Test
    fun `parse config without copy block defaults the clock skew tolerance`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "access789",
                "secretKey": "secret012"
              }
            }
            """.trimIndent()

        assertEquals(30_000, Parser.parse(json).copy.clockSkewToleranceMs)
    }

    @Test
    fun `parse stats block binds recording settings`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "access789",
                "secretKey": "secret012"
              },
              "stats": { "enabled": false, "database": "other.db", "maxFailureRows": 0 }
            }
            """.trimIndent()

        val stats = Parser.parse(json).stats

        assertEquals(false, stats.enabled)
        assertEquals("other.db", stats.database)
        assertEquals(0, stats.maxFailureRows)
    }

    @Test
    fun `parse config without stats block records to the system database`() {
        val json =
            """
            {
              "source": {
                "type": "s3",
                "bucket": "source-bucket",
                "accessKey": "access123",
                "secretKey": "secret456"
              },
              "target": {
                "type": "s3",
                "bucket": "target-bucket",
                "accessKey": "access789",
                "secretKey": "secret012"
              }
            }
            """.trimIndent()

        val stats = Parser.parse(json).stats

        assertEquals(true, stats.enabled)
        assertEquals("spark_catalog.iomete_system_db", stats.database)
        assertEquals(1000, stats.maxFailureRows)
    }

    @Test
    fun `parse S3-to-HDFS config binds HdfsConfig target`() {
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
                "type": "hdfs",
                "namenode": "isilon.example.com:8020",
                "path": "backups",
                "user": "isilon-user"
              }
            }
            """.trimIndent()

        val config = Parser.parse(json)

        assertIs<S3Config>(config.source)
        assertIs<HdfsConfig>(config.target)

        val target = config.target as HdfsConfig
        assertEquals("isilon.example.com:8020", target.namenode)
        assertEquals("backups", target.path)
        assertEquals("isilon-user", target.user)
        assertEquals("simple", target.authentication) // default
    }

    @Test
    fun `HDFS config missing user names the field`() {
        val json =
            """
            {
              "source": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" },
              "target": { "type": "hdfs", "namenode": "isilon.example.com:8020" }
            }
            """.trimIndent()

        val e = assertThrows<ConfigParseException> { Parser.parse(json) }
        assertEquals("Missing required field 'target.user'", e.message)
    }

    @Test
    fun `malformed JSON reports line and column`() {
        val json = """{ "source": { "type": "s3" "bucket": "x" } }"""

        val e = assertThrows<ConfigParseException> { Parser.parse(json) }
        assertEquals("Invalid JSON syntax at line 1, column 28", e.message)
    }

    @Test
    fun `missing top-level field names the field`() {
        val json =
            """
            {
              "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" }
            }
            """.trimIndent()

        val e = assertThrows<ConfigParseException> { Parser.parse(json) }
        assertEquals("Missing required field 'source'", e.message)
    }

    @Test
    fun `missing nested field reports dotted path`() {
        val json =
            """
            {
              "source": { "type": "s3", "accessKey": "k", "secretKey": "s" },
              "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" }
            }
            """.trimIndent()

        val e = assertThrows<ConfigParseException> { Parser.parse(json) }
        assertEquals("Missing required field 'source.bucket'", e.message)
    }

    @Test
    fun `explicit null for non-nullable field is a missing field`() {
        val json =
            """
            {
              "source": { "type": "s3", "bucket": null, "accessKey": "k", "secretKey": "s" },
              "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" }
            }
            """.trimIndent()

        val e = assertThrows<ConfigParseException> { Parser.parse(json) }
        assertEquals("Missing required field 'source.bucket'", e.message)
    }

    @Test
    fun `wrong value type reports value path and expected type`() {
        val json =
            """
            {
              "source": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s", "pathStyleAccess": "yes" },
              "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" }
            }
            """.trimIndent()

        val e = assertThrows<ConfigParseException> { Parser.parse(json) }
        assertEquals("Invalid value 'yes' at 'source.pathStyleAccess' (expected boolean)", e.message)
    }

    @Test
    fun `unknown storage type names the bad type`() {
        val json =
            """
            {
              "source": { "type": "gcs", "bucket": "b" },
              "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" }
            }
            """.trimIndent()

        val e = assertThrows<ConfigParseException> { Parser.parse(json) }
        assertEquals("Unknown type 'gcs' at 'source'", e.message)
    }

    @Test
    fun `missing type discriminator is reported`() {
        val json =
            """
            {
              "source": { "bucket": "b" },
              "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" }
            }
            """.trimIndent()

        val e = assertThrows<ConfigParseException> { Parser.parse(json) }
        assertEquals("Missing required 'type' field at 'source'", e.message)
    }

    @Test
    fun `error messages never leak internal class names`() {
        val badInputs =
            listOf(
                """{ "source": { "type": "s3" "bucket": "x" } }""",
                """{ "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" } }""",
                """{ "source": { "type": "gcs", "bucket": "b" }, "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" } }""",
                """{ "source": [], "target": { "type": "s3", "bucket": "b", "accessKey": "k", "secretKey": "s" } }""",
            )

        badInputs.forEach { json ->
            val e = assertThrows<ConfigParseException> { Parser.parse(json) }
            assertFalse(
                e.message.orEmpty().contains("com.iomete"),
                "message leaked FQCN: ${e.message}",
            )
        }
    }
}
