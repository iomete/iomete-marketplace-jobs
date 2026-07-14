package com.iomete.backup.config

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertIs

class ConfigLoaderTest {
    private fun write(
        dir: Path,
        json: String,
    ): String = File(dir.toFile(), "application.json").apply { writeText(json) }.absolutePath

    @Test
    fun `load parses and returns valid config`(
        @TempDir dir: Path,
    ) {
        val path =
            write(
                dir,
                """
                {
                  "source": { "type": "s3", "bucket": "src", "accessKey": "k", "secretKey": "s" },
                  "target": { "type": "s3", "bucket": "dst", "accessKey": "k", "secretKey": "s" }
                }
                """.trimIndent(),
            )

        val config = ConfigLoader.load(path)

        assertEquals("src", (config.source as S3Config).bucket)
        assertEquals("dst", (config.target as S3Config).bucket)
    }

    @Test
    fun `load throws ConfigValidationException with accumulated errors`(
        @TempDir dir: Path,
    ) {
        val path =
            write(
                dir,
                """
                {
                  "source": { "type": "s3", "bucket": "", "accessKey": "", "secretKey": "s" },
                  "target": { "type": "s3", "bucket": "dst", "accessKey": "k", "secretKey": "s" }
                }
                """.trimIndent(),
            )

        val e = assertThrows<ConfigValidationException> { ConfigLoader.load(path) }

        assertEquals(2, e.errors.size)
    }

    @Test
    fun `parse and validation errors share a common ConfigException type`(
        @TempDir dir: Path,
    ) {
        val path = write(dir, """{ "source": { "type": "s3" """)

        assertIs<ConfigException>(assertThrows<ConfigParseException> { ConfigLoader.load(path) })
    }
}
