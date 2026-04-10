package com.iomete.catalogsync

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.nio.file.Path

class ApplicationConfigExtractorTest {

    private val extractor = ApplicationConfigExtractor()

    @Test
    fun `load should parse valid JSON config file`(@TempDir tempDir: Path) {
        val configFile = File(tempDir.toFile(), "config.json")
        configFile.writeText("""
            {
                "catalog": {
                    "include": ["catalog1", "catalog2"],
                    "exclude": ["catalog3"]
                }
            }
        """.trimIndent())

        val result = extractor.load(configFile.absolutePath)

        assertEquals(listOf("catalog1", "catalog2"), result.catalog.include)
        assertEquals(listOf("catalog3"), result.catalog.exclude)
    }

    @Test
    fun `load should return default config when file does not exist`() {
        val result = extractor.load("/nonexistent/path/config.json")

        assertEquals(emptyList<String>(), result.catalog.include)
        assertEquals(emptyList<String>(), result.catalog.exclude)
    }

    @Test
    fun `load should return default config when JSON is malformed`(@TempDir tempDir: Path) {
        val configFile = File(tempDir.toFile(), "config.json")
        configFile.writeText("{ invalid json !!!")

        val result = extractor.load(configFile.absolutePath)

        assertEquals(emptyList<String>(), result.catalog.include)
        assertEquals(emptyList<String>(), result.catalog.exclude)
    }

    @Test
    fun `load should handle empty catalog config`(@TempDir tempDir: Path) {
        val configFile = File(tempDir.toFile(), "config.json")
        configFile.writeText("""{"catalog": {}}""")

        val result = extractor.load(configFile.absolutePath)

        assertEquals(emptyList<String>(), result.catalog.include)
        assertEquals(emptyList<String>(), result.catalog.exclude)
    }

    @Test
    fun `load should parse config with only include list`(@TempDir tempDir: Path) {
        val configFile = File(tempDir.toFile(), "config.json")
        configFile.writeText("""{"catalog": {"include": ["cat1", "cat2"]}}""")

        val result = extractor.load(configFile.absolutePath)

        assertEquals(listOf("cat1", "cat2"), result.catalog.include)
        assertEquals(emptyList<String>(), result.catalog.exclude)
    }

    @Test
    fun `load should parse config with only exclude list`(@TempDir tempDir: Path) {
        val configFile = File(tempDir.toFile(), "config.json")
        configFile.writeText("""{"catalog": {"exclude": ["cat3"]}}""")

        val result = extractor.load(configFile.absolutePath)

        assertEquals(emptyList<String>(), result.catalog.include)
        assertEquals(listOf("cat3"), result.catalog.exclude)
    }
}
