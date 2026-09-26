package com.iomete.cleanup.untrackedtablefolders.catalog

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class DiscoveredDatabaseTest {

    @Test
    fun `tables holds resolved and unresolved catalog tables together`() {
        val database =
            DiscoveredDatabase(
                catalog = "spark_catalog",
                database = "analytics",
                location = "s3a://bucket/db",
                tables = listOf(
                    table("a", location = "s3a://bucket/db/a"),
                    table("b", location = "s3a://bucket/db/b"),
                    table("c", unresolvedReason = "Location does not exist: s3://bucket/db/c/metadata/1.metadata.json"),
                    table("d", unresolvedReason = "Location does not exist: s3://bucket/db/d/metadata/1.metadata.json"),
                ),
            )

        assertEquals(4, database.tables.size)
        assertEquals(2, database.unresolvedTables.size)
        assertEquals(2, database.tables.mapNotNull { it.location }.size)
        assertEquals(
            listOf("spark_catalog.analytics.c", "spark_catalog.analytics.d"),
            database.unresolvedTables.map { it.qualifiedName },
        )
    }

    private fun table(
        name: String,
        location: String? = null,
        unresolvedReason: String? = null,
    ): DiscoveredTable =
        DiscoveredTable(
            catalog = "spark_catalog",
            database = "analytics",
            table = name,
            isTemporary = false,
            location = location,
            unresolvedReason = unresolvedReason,
        )
}
