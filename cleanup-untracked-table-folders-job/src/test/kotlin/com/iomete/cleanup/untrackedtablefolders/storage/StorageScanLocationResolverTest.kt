package com.iomete.cleanup.untrackedtablefolders.storage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class StorageScanLocationResolverTest {

    private val resolver = StorageScanLocationResolver()

    @Test
    fun `returns database location when active table locations are empty`() {
        val result =
            resolver.resolve(
                databaseLocation = "s3a://bucket/data/delete_1.db",
                activeTableLocations = emptyList(),
            )

        assertEquals("s3a://bucket/data/delete_1.db", result)
    }

    @Test
    fun `returns parent of single active table location`() {
        val result =
            resolver.resolve(
                databaseLocation = "s3a://bucket/data/delete_1.db",
                activeTableLocations =
                    listOf("s3a://bucket/data/delete_1/delete_table_a"),
            )

        assertEquals("s3a://bucket/data/delete_1", result)
    }

    @Test
    fun `returns shared parent when all active table locations have the same parent`() {
        val result =
            resolver.resolve(
                databaseLocation = "s3a://bucket/data/delete_1.db",
                activeTableLocations =
                    listOf(
                        "s3a://bucket/data/delete_1/delete_table_a",
                        "s3a://bucket/data/delete_1/delete_table_b",
                        "s3a://bucket/data/delete_1/delete_table_c",
                    ),
            )

        assertEquals("s3a://bucket/data/delete_1", result)
    }

    @Test
    fun `falls back to database location when active table locations have different parents`() {
        val result =
            resolver.resolve(
                databaseLocation = "s3a://bucket/data/default.db",
                activeTableLocations =
                    listOf(
                        "s3a://bucket/data/default.db/table_a",
                        "s3a://bucket/data/default/table_b",
                        "s3a://bucket/data/DEFAULT/table_c",
                    ),
            )

        assertEquals("s3a://bucket/data/default.db", result)
    }

    @Test
    fun `allows non db sibling root for db database location`() {
        val result =
            resolver.resolve(
                databaseLocation = "s3a://bucket/data/delete_1.db",
                activeTableLocations =
                    listOf("s3a://bucket/data/delete_1/delete_table_a"),
            )

        assertEquals("s3a://bucket/data/delete_1", result)
    }

    @Test
    fun `throws when inferred scan location escapes database boundary`() {
        val error =
            assertThrows(IllegalStateException::class.java) {
                resolver.resolve(
                    databaseLocation = "s3a://bucket/data/delete_1.db",
                    activeTableLocations =
                        listOf("s3a://bucket/other/delete_table_a"),
                )
            }

        assertTrue(error.message?.contains("escapes database boundary") == true)
    }

    @Test
    fun `trims trailing slash from active table location before resolving parent`() {
        val result =
            resolver.resolve(
                databaseLocation = "s3a://bucket/data/delete_1.db",
                activeTableLocations =
                    listOf("s3a://bucket/data/delete_1/delete_table_a/"),
            )

        assertEquals("s3a://bucket/data/delete_1", result)
    }
}