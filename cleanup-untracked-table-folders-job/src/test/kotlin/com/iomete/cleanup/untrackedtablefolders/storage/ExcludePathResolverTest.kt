package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class ExcludePathResolverTest {

    @Test
    fun `returns empty when no configured exclude paths`() {
        val resolver = resolverFor(
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
            )
        )

        assertEquals(emptyList<String>(), resolver.normalizedConfiguredExcludePaths())
    }

    @Test
    fun `normalizes scheme casing and trailing slash, then dedupes and sorts configured exclude paths`() {
        val resolver = resolverFor(
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
                excludePaths = listOf(
                    "s3a://bucket/db/zebra/",
                    "S3A://bucket/db/zebra",
                    "s3a://bucket/db/apple",
                ),
            )
        )

        assertEquals(
            listOf(
                "s3a://bucket/db/apple",
                "s3a://bucket/db/zebra",
            ),
            resolver.normalizedConfiguredExcludePaths(),
        )
    }

    @Test
    fun `effective paths combines configured and database-folder paths and sorts them`() {
        val resolver = resolverFor(
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
                excludePaths = listOf("s3a://bucket/db/external"),
                excludeDatabaseFolders = listOf("analytics.customer_events"),
            )
        )

        val result = resolver.effectiveExcludedPaths(
            database = "analytics",
            storageScanLocation = "s3a://bucket/db",
        )

        assertEquals(
            listOf(
                "s3a://bucket/db/customer_events",
                "s3a://bucket/db/external",
            ),
            result,
        )
    }

    @Test
    fun `effective paths ignores database-folder entries for unrelated databases`() {
        val resolver = resolverFor(
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics", "sales"),
                excludeDatabaseFolders = listOf("sales.invoices"),
            )
        )

        val result = resolver.effectiveExcludedPaths(
            database = "analytics",
            storageScanLocation = "s3a://bucket/db",
        )

        assertEquals(emptyList<String>(), result)
    }

    @Test
    fun `effective paths dedupe when configured path matches resolved database-folder path`() {
        val resolver = resolverFor(
            ApplicationConfig(
                catalog = "spark_catalog",
                databases = listOf("analytics"),
                excludePaths = listOf("s3a://bucket/db/customer_events"),
                excludeDatabaseFolders = listOf("analytics.customer_events"),
            )
        )

        val result = resolver.effectiveExcludedPaths(
            database = "analytics",
            storageScanLocation = "s3a://bucket/db",
        )

        assertEquals(
            listOf("s3a://bucket/db/customer_events"),
            result,
        )
    }

    private fun resolverFor(applicationConfig: ApplicationConfig): ExcludePathResolver =
        ExcludePathResolver().apply { config = applicationConfig }
}
