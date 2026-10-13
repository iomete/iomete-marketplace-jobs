package com.iomete.cleanup.untrackedtablefolders.config

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ConfigTest {

    @Test
    fun `valid exclude database folders pass validation`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
                excludeDatabaseFolders = listOf("analytics.customer_events"),
            )

        assertDoesNotThrow { config.validate() }
    }

    @Test
    fun `exclude database folders must use database folder format`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
                excludeDatabaseFolders = listOf("customer_events"),
            )

        assertThrows(IllegalArgumentException::class.java) {
            config.validate()
        }
    }

    @Test
    fun `exclude database folders database must be configured`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
                excludeDatabaseFolders = listOf("sales.customer_events"),
            )

        assertThrows(IllegalArgumentException::class.java) {
            config.validate()
        }
    }

    @Test
    fun `exclude database folders must reference immediate child folder`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
                excludeDatabaseFolders = listOf("analytics.customer_events/nested"),
            )

        assertThrows(IllegalArgumentException::class.java) {
            config.validate()
        }
    }

    @Test
    fun `collect_size_statistics is still accepted by the strict config parser`() {
        val config =
            applicationConfigObjectMapper().readValue(
                """{"catalog": "spark_catalog", "databases": ["analytics"], "collect_size_statistics": false}""",
                ApplicationConfig::class.java,
            )

        assertEquals(false, config.collectSizeStatistics)
    }

    @Test
    fun `explicit false produces one deprecation warning`() {
        val config = ApplicationConfig(catalog = "spark_catalog", databases = listOf("analytics"), collectSizeStatistics = false)

        assertEquals(1, config.deprecationWarnings().size)
        assertTrue(config.deprecationWarnings().single().contains("collect_size_statistics=false is deprecated and ignored"))
    }

    @Test
    fun `absent or true produces no deprecation warning`() {
        assertEquals(emptyList<String>(), ApplicationConfig(catalog = "spark_catalog", databases = listOf("analytics")).deprecationWarnings())
        assertEquals(
            emptyList<String>(),
            ApplicationConfig(catalog = "spark_catalog", databases = listOf("analytics"), collectSizeStatistics = true).deprecationWarnings(),
        )
    }
}
