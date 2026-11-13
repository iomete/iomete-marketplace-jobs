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
    fun `fails closed when the database location has no scheme or bucket`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolver.resolve(
                    databaseLocation = "/lake",
                    activeTableLocations = listOf("s3a://bucket/lake/sales.db/orders", "s3a://bucket/lake/other/events"),
                    sharedWarehouseRoots = listOf("s3a://bucket/lake"),
                )
            }

        assertEquals(UnsafeScanRootReason.INVALID_STORAGE_LOCATION, error.reason)
    }

    @Test
    fun `refuses a database on a storage scheme the job does not support`() {
        val error =
            assertThrows(UnsafeScanRootException::class.java) {
                resolver.resolve(
                    databaseLocation = "hdfs://namenode/lake/sales.db",
                    activeTableLocations = listOf("hdfs://namenode/lake/sales.db/orders"),
                    sharedWarehouseRoots = listOf("hdfs://namenode/lake"),
                )
            }

        assertEquals(UnsafeScanRootReason.INVALID_STORAGE_LOCATION, error.reason)
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

    @Test
    fun `s3 and s3a spellings of one folder give one parent`() {
        val result =
            resolve(
                databaseLocation = "s3a://bucket/data/sales.db",
                activeTableLocations = listOf("s3://bucket/data/sales/orders", "s3a://bucket/data/sales/customers"),
            )

        assertEquals("s3://bucket/data/sales", result)
    }

    @Test
    fun `parents that differ only by a trailing slash give one parent`() {
        val result =
            resolve(
                databaseLocation = "s3a://bucket/data/sales.db",
                activeTableLocations = listOf("s3a://bucket/data/sales/orders/", "s3a://bucket/data/sales/customers"),
            )

        assertEquals("s3a://bucket/data/sales", result)
    }

    private fun cleanupRoots(
        tables: List<String>,
        restRoot: String? = MANAGED_TWIN,
        databaseLocation: String = MANAGED_DB,
    ): CleanupRoots =
        resolver.cleanupRoots(
            resolvedScanLocation = resolve(databaseLocation, tables),
            databaseLocation = databaseLocation,
            activeTableLocations = tables,
            derivedRestNamespaceRoot = { restRoot },
        )

    @Test
    fun `tables in both folders make both folders cleanup roots`() {
        val roots = cleanupRoots(listOf("$MANAGED_DB/t1", "$MANAGED_TWIN/t2"))

        assertEquals(listOf(MANAGED_DB, MANAGED_TWIN), roots.locations)
        assertEquals("used", roots.managedRootStatus)
    }

    @Test
    fun `tables only in the folder without db and outside make that folder the only cleanup root`() {
        val roots = cleanupRoots(listOf("$MANAGED_TWIN/t1", "s3a://other/ext/t2"))

        assertEquals(listOf(MANAGED_TWIN), roots.locations)
        assertEquals("used", roots.managedRootStatus)
    }

    @Test
    fun `a REST root spelled with s3 still matches the folder without db`() {
        val roots = cleanupRoots(listOf("$MANAGED_DB/t1", "$MANAGED_TWIN/t2"), restRoot = "s3://bucket/lake/sales/")

        assertEquals(listOf(MANAGED_DB, MANAGED_TWIN), roots.locations)
    }

    @Test
    fun `the folder without db is refused with a reason when it cannot be proven`() {
        val mixed = listOf("$MANAGED_DB/t1", "$MANAGED_TWIN/t2")

        assertEquals("refused:rest_root_not_derived", cleanupRoots(mixed, restRoot = null).managedRootStatus)
        assertEquals("refused:rest_root_differs", cleanupRoots(mixed, restRoot = "s3a://bucket/lake/other").managedRootStatus)
        assertEquals("refused:no_visible_table", cleanupRoots(listOf("$MANAGED_DB/t1", "s3a://other/ext/t2")).managedRootStatus)
        assertEquals("refused:table_at_or_above_root", cleanupRoots(listOf("$MANAGED_DB/t1", MANAGED_TWIN, "$MANAGED_TWIN/t3")).managedRootStatus)
        listOf(
            cleanupRoots(mixed, restRoot = null),
            cleanupRoots(listOf("$MANAGED_DB/t1", "s3a://other/ext/t2")),
        ).forEach { assertEquals(listOf(MANAGED_DB), it.locations) }
    }

    @Test
    fun `the managed root is not evaluated when every table is under the resolved root`() {
        var asked = false
        val tables = listOf("$MANAGED_DB/t1", "$MANAGED_DB/archive/t2")
        val roots =
            resolver.cleanupRoots(
                resolvedScanLocation = resolve(MANAGED_DB, tables),
                databaseLocation = MANAGED_DB,
                activeTableLocations = tables,
                derivedRestNamespaceRoot = { asked = true; MANAGED_TWIN },
            )

        assertEquals(listOf(MANAGED_DB), roots.locations)
        assertEquals("not_applicable", roots.managedRootStatus)
        assertEquals(false, asked)
    }

    @Test
    fun `a single parent outside the db folder is unchanged`() {
        val roots = cleanupRoots(listOf("$MANAGED_TWIN/t1", "$MANAGED_TWIN/t2"))

        assertEquals(listOf(MANAGED_TWIN), roots.locations)
        assertEquals("not_applicable", roots.managedRootStatus)
    }

    @Test
    fun `a database location without db has no second root`() {
        val roots = cleanupRoots(listOf("s3a://bucket/lake/sales/t1", "s3a://other/ext/t2"), databaseLocation = "s3a://bucket/lake/sales")

        assertEquals(listOf("s3a://bucket/lake/sales"), roots.locations)
        assertEquals("not_applicable", roots.managedRootStatus)
    }
}

private const val MANAGED_DB = "s3a://bucket/lake/sales.db"
private const val MANAGED_TWIN = "s3a://bucket/lake/sales"
