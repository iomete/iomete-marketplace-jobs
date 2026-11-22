package com.iomete.cleanup.untrackedtablefolders.config

import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException
import org.junit.jupiter.api.Assertions.assertDoesNotThrow
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.ValueSource

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

    @Test
    fun `exclude paths must include a scheme and bucket`() {
        val config =
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("sales"),
                excludePaths = listOf("/lake/sales.db/keep"),
            )

        assertThrows(IllegalArgumentException::class.java) { config.validate() }
    }

    @ParameterizedTest(name = "older_than_hours={0} in deletion mode uses {1}")
    @CsvSource("0, 72", "24, 72", "71, 72", "72, 72", "73, 73", "100, 100")
    fun `deletion mode uses at least 72 hours`(
        configured: Long,
        effective: Long,
    ) {
        val config = deletionConfig(configured)

        assertEquals(effective, config.effectiveOlderThanHours())
        assertEquals(configured, config.olderThanHours)
        assertDoesNotThrow { config.validate() }
    }

    @ParameterizedTest(name = "older_than_hours={0} in a dry run stays {0}")
    @ValueSource(longs = [0, 1, 24, 71, 72, 100])
    fun `a dry run keeps the configured age`(configured: Long) {
        assertEquals(configured, dryRunConfig(configured).effectiveOlderThanHours())
        assertNull(dryRunConfig(configured).ageFloorWarning())
    }

    @Test
    fun `an unset older_than_hours defaults to 24 and runs at 72 hours in deletion mode`() {
        val deletion = parse("""{"catalog":"spark_catalog","databases":["sales"],"dry_run":false,"delete_enabled":true}""")
        val dryRun = parse("""{"catalog":"spark_catalog","databases":["sales"]}""")

        assertEquals(24, deletion.olderThanHours)
        assertEquals(72, deletion.effectiveOlderThanHours())
        assertEquals(24, dryRun.olderThanHours)
        assertEquals(24, dryRun.effectiveOlderThanHours())
    }

    @Test
    fun `one warning names the configured and the effective age when the minimum applies`() {
        assertEquals(
            "older_than_hours=24 is below the 72-hour minimum for deletion mode, so this run uses 72 hours. " +
                "configured_older_than_hours=24, effective_older_than_hours=72.",
            deletionConfig(24).ageFloorWarning(),
        )
        assertTrue(deletionConfig(0).ageFloorWarning()!!.contains("configured_older_than_hours=0, effective_older_than_hours=72"))
        assertTrue(deletionConfig(71).ageFloorWarning()!!.contains("configured_older_than_hours=71, effective_older_than_hours=72"))
        assertNull(deletionConfig(72).ageFloorWarning())
        assertNull(deletionConfig(100).ageFloorWarning())
    }

    // The job reads only this JSON file, and the parser rejects every key it does not map. So no key can lower the
    // minimum or set the effective age.
    @Test
    fun `the config file accepts exactly the documented keys`() {
        val mapper = applicationConfigObjectMapper()
        val keys =
            mapper.deserializationConfig
                .introspect(mapper.constructType(ApplicationConfig::class.java))
                .findProperties()
                .map { it.name }
                .toSet()

        assertEquals(
            setOf(
                "catalog", "databases", "exclude_paths", "exclude_database_folders", "older_than_hours", "dry_run", "delete_enabled",
                "max_candidate_folders_per_database", "collect_size_statistics", "report_only_storage_roots",
            ),
            keys,
        )
    }

    @ParameterizedTest(name = "{0} is rejected")
    @ValueSource(
        strings = [
            "effective_older_than_hours", "effectiveOlderThanHours", "deletion_min_age_hours", "DELETION_MIN_AGE_HOURS",
            "min_older_than_hours", "minimum_age_hours", "age_floor", "skip_age_floor", "olderThanHours", "Older_Than_Hours",
            "older-than-hours", "dryRun", "ageFloorWarning",
        ],
    )
    fun `no other key can change the age`(key: String) {
        val error =
            assertThrows(UnrecognizedPropertyException::class.java) {
                parse("""{"catalog":"spark_catalog","databases":["sales"],"dry_run":false,"delete_enabled":true,"$key":0}""")
            }

        assertEquals(key, error.propertyName)
    }

    @Test
    fun `coerced and repeated older_than_hours values still get the minimum in deletion mode`() {
        val deletion = """{"catalog":"spark_catalog","databases":["sales"],"dry_run":false,"delete_enabled":true"""

        assertEquals(72, parse("""$deletion,"older_than_hours":"0"}""").effectiveOlderThanHours())
        assertEquals(72, parse("""$deletion,"older_than_hours":1.9}""").effectiveOlderThanHours())
        // The last repeated key wins; the minimum still applies to it.
        assertEquals(72, parse("""$deletion,"older_than_hours":100,"older_than_hours":0}""").effectiveOlderThanHours())
        assertEquals(100, parse("""$deletion,"older_than_hours":0,"older_than_hours":100}""").effectiveOlderThanHours())
        assertThrows(IllegalArgumentException::class.java) { parse("""$deletion,"older_than_hours":-1}""").validate() }
    }

    private fun parse(json: String): ApplicationConfig = applicationConfigObjectMapper().readValue(json, ApplicationConfig::class.java)

    private fun deletionConfig(olderThanHours: Long) =
        ApplicationConfig(
            catalog = "spark_catalog",
            databases = listOf("sales"),
            olderThanHours = olderThanHours,
            dryRun = false,
            deleteEnabled = true,
        )

    private fun dryRunConfig(olderThanHours: Long) =
        ApplicationConfig(catalog = "spark_catalog", databases = listOf("sales"), olderThanHours = olderThanHours, dryRun = true)
}
