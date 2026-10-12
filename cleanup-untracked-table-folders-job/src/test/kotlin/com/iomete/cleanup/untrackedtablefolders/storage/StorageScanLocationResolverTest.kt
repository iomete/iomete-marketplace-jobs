package com.iomete.cleanup.untrackedtablefolders.storage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class StorageScanLocationResolverTest {

    private val resolver = StorageScanLocationResolver()

    private val warehouseRoots = listOf("s3a://warehouse-bucket/warehouse")

    private fun resolve(
        databaseLocation: String,
        activeTableLocations: List<String>,
    ): String = resolver.resolve(databaseLocation, activeTableLocations, warehouseRoots)

    @Test
    fun `returns database location when active table locations are empty`() {
        val result =
            resolve(
                databaseLocation = "s3a://bucket/data/delete_1.db",
                activeTableLocations = emptyList(),
            )

        assertEquals("s3a://bucket/data/delete_1.db", result)
    }

    @Test
    fun `returns parent of single active table location`() {
        val result =
            resolve(
                databaseLocation = "s3a://bucket/data/delete_1.db",
                activeTableLocations =
                    listOf("s3a://bucket/data/delete_1/delete_table_a"),
            )

        assertEquals("s3a://bucket/data/delete_1", result)
    }

    @Test
    fun `returns shared parent when all active table locations have the same parent`() {
        val result =
            resolve(
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
            resolve(
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
            resolve(
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
                resolve(
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
            resolve(
                databaseLocation = "s3a://bucket/data/delete_1.db",
                activeTableLocations =
                    listOf("s3a://bucket/data/delete_1/delete_table_a/"),
            )

        assertEquals("s3a://bucket/data/delete_1", result)
    }

    @Test
    fun `table stored at a bucket root contributes no inferred parent`() {
        val result =
            resolve(
                databaseLocation = "s3a://bucket/data/sales.db",
                activeTableLocations = listOf("s3a://bucket"),
            )

        assertEquals("s3a://bucket/data/sales.db", result)
    }

    @Test
    fun `rejects a bucket root database location even when the table parent is deeper`() {
        val error =
            assertThrows(IllegalStateException::class.java) {
                resolve(
                    databaseLocation = "s3a://bucket",
                    activeTableLocations = listOf("s3a://bucket/team_x/orders"),
                )
            }

        assertTrue(error.message?.contains("bucket root") == true)
    }

    @Test
    fun `rejects a database location that equals the warehouse root`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolver.resolve(
                    databaseLocation = "s3a://bucket/lake",
                    activeTableLocations = listOf("s3a://bucket/lake/events"),
                    sharedWarehouseRoots = listOf("s3a://bucket/lake/"),
                )
            }

        assertEquals(UnsafeScanRootReason.DATABASE_LOCATION_CONTAINS_WAREHOUSE_ROOT, error.reason)
    }

    @Test
    fun `rejects a database location that contains the warehouse root`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolver.resolve(
                    databaseLocation = "s3a://bucket/data",
                    activeTableLocations = listOf("s3a://bucket/data/team/orders"),
                    sharedWarehouseRoots = listOf("s3://bucket/data/lake"),
                )
            }

        assertEquals(UnsafeScanRootReason.DATABASE_LOCATION_CONTAINS_WAREHOUSE_ROOT, error.reason)
    }

    @Test
    fun `checks every warehouse root including the db suffix fallback root`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolver.resolve(
                    databaseLocation = "s3a://bucket/lake.db",
                    activeTableLocations = listOf("s3a://bucket/lake.db/orders"),
                    sharedWarehouseRoots = listOf("s3a://other-bucket/warehouse", "s3a://bucket/lake"),
                )
            }

        assertEquals(UnsafeScanRootReason.DATABASE_LOCATION_CONTAINS_WAREHOUSE_ROOT, error.reason)
    }

    @Test
    fun `accepts a database location inside the warehouse root`() {
        val result =
            resolver.resolve(
                databaseLocation = "s3a://bucket/lake/sales.db",
                activeTableLocations = listOf("s3a://bucket/lake/sales.db/orders"),
                sharedWarehouseRoots = listOf("s3a://bucket/lake"),
            )

        assertEquals("s3a://bucket/lake/sales.db", result)
    }

    @Test
    fun `accepts an isolated database location outside the warehouse root`() {
        val result =
            resolver.resolve(
                databaseLocation = "s3a://finance-bucket/ledger",
                activeTableLocations = listOf("s3a://finance-bucket/ledger/entries"),
                sharedWarehouseRoots = listOf("s3a://bucket/lake"),
            )

        assertEquals("s3a://finance-bucket/ledger", result)
    }

    @Test
    fun `fails closed when the catalog warehouse is unknown`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolver.resolve(
                    databaseLocation = "s3a://bucket/lake/sales.db",
                    activeTableLocations = listOf("s3a://bucket/lake/sales.db/orders"),
                    sharedWarehouseRoots = emptyList(),
                )
            }

        assertEquals(UnsafeScanRootReason.CATALOG_WAREHOUSE_UNKNOWN, error.reason)
    }

    @Test
    fun `fails closed when a warehouse root has no scheme or bucket`() {
        listOf("/lake", "bucket/lake", "s3a:/lake").forEach { warehouseRoot ->
            val error =
                assertThrows(UnsafeScanRootException::class.java, {
                    resolver.resolve(
                        databaseLocation = "s3a://bucket/lake",
                        activeTableLocations = listOf("s3a://bucket/lake/events"),
                        sharedWarehouseRoots = listOf(warehouseRoot),
                    )
                }, "warehouseRoot=$warehouseRoot")

            assertEquals(UnsafeScanRootReason.INVALID_STORAGE_LOCATION, error.reason, "warehouseRoot=$warehouseRoot")
        }
    }

    @Test
    fun `reports a bucket root database location with its reason`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolve(
                    databaseLocation = "s3a://bucket/",
                    activeTableLocations = listOf("s3a://bucket/team_x/orders"),
                )
            }

        assertEquals(UnsafeScanRootReason.DATABASE_LOCATION_IS_BUCKET_ROOT, error.reason)
    }

    @Test
    fun `reports an escaping scan root with its reason`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolve(
                    databaseLocation = "s3a://bucket/data/delete_1.db",
                    activeTableLocations = listOf("s3a://bucket/other/delete_table_a"),
                )
            }

        assertEquals(UnsafeScanRootReason.SCAN_ROOT_OUTSIDE_DATABASE_BOUNDARY, error.reason)
    }

    @Test
    fun `reports an unparseable location instead of a URI failure`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolve(
                    databaseLocation = "s3a:",
                    activeTableLocations = listOf("s3a://bucket/data/orders"),
                )
            }

        assertEquals(UnsafeScanRootReason.INVALID_STORAGE_LOCATION, error.reason)
    }
}
