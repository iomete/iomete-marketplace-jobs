package com.iomete.cleanup.untrackedtablefolders.config

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertFalse
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
    fun `collect size statistics defaults to true`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
            )

        assertTrue(config.collectSizeStatistics)
    }

    @Test
    fun `collect size statistics can be disabled`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
                collectSizeStatistics = false,
            )

        assertFalse(config.collectSizeStatistics)
    }
}
