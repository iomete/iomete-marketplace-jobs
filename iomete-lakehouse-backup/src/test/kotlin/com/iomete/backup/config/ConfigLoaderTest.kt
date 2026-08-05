package com.iomete.backup.config

import org.apache.spark.SparkConf
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

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

    private fun bandwidthConfig(maxBandwidthMbPerSec: Double?): ApplicationConfig {
        val storage = S3Config(bucket = "bucket", accessKey = "k", secretKey = "s")
        return ApplicationConfig(
            source = storage,
            target = storage,
            copy = CopyConfig(maxBandwidthMbPerSec = maxBandwidthMbPerSec),
        )
    }

    private fun clusterConf(vararg settings: Pair<String, String>): SparkConf =
        SparkConf(false)
            .set("spark.master", "k8s://https://kubernetes.default.svc")
            .also { conf -> settings.forEach { (key, value) -> conf.set(key, value) } }

    @Test
    fun `loadInternalConfig leaves an uncapped run unpaced`() {
        val internalConfig = ConfigLoader.loadInternalConfig(bandwidthConfig(null), clusterConf())

        assertNull(internalConfig.bytesPerSecPerExecutor)
    }

    @Test
    fun `loadInternalConfig resolves a capped run against the executor count`() {
        val conf = clusterConf("spark.executor.instances" to "4")

        val internalConfig = ConfigLoader.loadInternalConfig(bandwidthConfig(600.0), conf)

        assertEquals(150 * 1024.0 * 1024.0, internalConfig.bytesPerSecPerExecutor)
    }

    @Test
    fun `loadInternalConfig rejects a cap it cannot divide, naming the setting to fix`() {
        val e =
            assertThrows<ConfigValidationException> {
                ConfigLoader.loadInternalConfig(bandwidthConfig(600.0), clusterConf())
            }

        assertTrue(e.errors.single().contains("spark.executor.instances"), e.message!!)
    }
}
