package com.iomete.cleanup.untrackedtablefolders.config

import org.junit.jupiter.api.Assertions.assertDoesNotThrow
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
}