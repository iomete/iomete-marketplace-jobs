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

    @Test
    fun `report-only storage roots are optional and parsed per database`() {
        val mapper = applicationConfigObjectMapper()

        val withoutRoots = mapper.readValue("""{"catalog":"spark_catalog","databases":["sales"]}""", ApplicationConfig::class.java)
        val withRoots =
            mapper.readValue(
                """{"catalog":"spark_catalog","databases":["sales"],"report_only_storage_roots":{"sales":["s3a://bucket2/ext/a"]}}""",
                ApplicationConfig::class.java,
            )

        assertEquals(emptyMap<String, List<String>>(), withoutRoots.reportOnlyStorageRoots)
        assertEquals(mapOf("sales" to listOf("s3a://bucket2/ext/a")), withRoots.reportOnlyStorageRoots)
        assertDoesNotThrow { withRoots.validate() }
    }

    @Test
    fun `report-only storage roots must name a configured database`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("sales"),
                reportOnlyStorageRoots = mapOf("analytics" to listOf("s3a://bucket2/ext/a")),
            )

        assertThrows(IllegalArgumentException::class.java) { config.validate() }
    }

    @Test
    fun `report-only storage roots must not be blank`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("sales"),
                reportOnlyStorageRoots = mapOf("sales" to listOf(" ")),
            )

        assertThrows(IllegalArgumentException::class.java) { config.validate() }
    }
}
