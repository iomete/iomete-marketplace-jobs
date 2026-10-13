package com.iomete.cleanup.untrackedtablefolders.storage

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

private const val WAREHOUSE = "s3a://bucket/lake"
private const val DATABASE_LOCATION = "s3a://bucket/lake/sales.db"
private const val REST_ROOT = "s3a://bucket/lake/sales"

class StorageScanLocationPlannerTest {

    private val planner = StorageScanLocationPlanner()

    @Test
    fun `nine parents across two buckets keep only in-boundary roots and name every unsupported one`() {
        val plan =
            plan(
                cleanupRoot = DATABASE_LOCATION,
                tables =
                    listOf(
                        "$DATABASE_LOCATION/t1",
                        "$REST_ROOT/t2",
                        "$REST_ROOT/t3",
                        "$DATABASE_LOCATION/archive/t4",
                        "$DATABASE_LOCATION/landing/t5",
                        "s3a://bucket/custom/a/t6",
                        "s3a://bucket/custom/b/t7",
                        "s3a://bucket2/ext/a/t8",
                        "s3a://bucket2/ext/b/t9",
                        "s3a://bucket2/ext/c/t10",
                    ),
            )

        assertEquals(DATABASE_LOCATION, plan.cleanupRoot?.location)
        assertEquals(
            listOf(REST_ROOT, "$DATABASE_LOCATION/archive", "$DATABASE_LOCATION/landing"),
            plan.reportOnlyRoots.map { it.location },
        )
        assertEquals(
            mapOf(
                "s3a://bucket/custom/a" to ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY,
                "s3a://bucket/custom/b" to ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY,
                "s3a://bucket2/ext/a" to ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY,
                "s3a://bucket2/ext/b" to ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY,
                "s3a://bucket2/ext/c" to ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY,
            ),
            plan.roots.filter { it.rejection != null }.associate { it.location to it.rejection },
        )
    }

    @Test
    fun `the managed REST namespace root is a root even when none of its tables is visible`() {
        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = listOf("$DATABASE_LOCATION/t1"), restNamespaceRoot = REST_ROOT)

        val restRoot = plan.roots.single { it.location == REST_ROOT }
        assertTrue(restRoot.reportOnly)
        assertEquals(setOf(ScanRootSource.REST_NAMESPACE), restRoot.sources)
    }

    @Test
    fun `a REST namespace root in another bucket is accepted as a cross-bucket root`() {
        val plan =
            plan(
                cleanupRoot = "s3a://bucket2/dbs/sales.db",
                databaseLocation = "s3a://bucket2/dbs/sales.db",
                tables = listOf("s3a://bucket2/dbs/sales.db/t1", "$REST_ROOT/t2"),
                restNamespaceRoot = REST_ROOT,
            )

        val restRoot = plan.roots.single { it.location == REST_ROOT }
        assertTrue(restRoot.reportOnly)
        assertEquals(setOf(ScanRootSource.REST_NAMESPACE, ScanRootSource.TABLE_PARENT), restRoot.sources)
    }

    @Test
    fun `without a REST namespace root a parent in another bucket is not scanned`() {
        val plan =
            plan(
                cleanupRoot = "s3a://bucket2/dbs/sales.db",
                databaseLocation = "s3a://bucket2/dbs/sales.db",
                tables = listOf("s3a://bucket2/dbs/sales.db/t1", "$REST_ROOT/t2"),
            )

        assertEquals(ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY, plan.roots.single { it.location == REST_ROOT }.rejection)
    }

    @Test
    fun `several tables sharing a parent and equivalent spellings make one root`() {
        val plan =
            plan(
                cleanupRoot = DATABASE_LOCATION,
                tables = listOf("$DATABASE_LOCATION/t1", "s3://bucket/lake/sales/t2", "$REST_ROOT//t3/", "$REST_ROOT/t4"),
                restNamespaceRoot = "$REST_ROOT/",
            )

        assertEquals(listOf(REST_ROOT, DATABASE_LOCATION), plan.roots.map { it.location })
    }

    @Test
    fun `nested roots are kept as separate roots`() {
        val plan = plan(cleanupRoot = "$DATABASE_LOCATION/archive", tables = listOf("$DATABASE_LOCATION/archive/t1"))

        assertEquals("$DATABASE_LOCATION/archive", plan.cleanupRoot?.location)
        assertEquals(listOf(DATABASE_LOCATION), plan.reportOnlyRoots.map { it.location })
    }

    @Test
    fun `a table at a bucket root never makes the bucket a root`() {
        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = listOf("$DATABASE_LOCATION/t1", "s3a://bucket2/t2", "s3a://bucket3"))

        assertEquals(ScanRootRejection.BUCKET_ROOT, plan.roots.single { it.location == "s3a://bucket2" }.rejection)
        assertEquals(ScanRootRejection.BUCKET_ROOT, plan.roots.single { it.location == "s3a://bucket3" }.rejection)
    }

    @Test
    fun `a parent that is the warehouse root is refused`() {
        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = listOf("$DATABASE_LOCATION/t1", "$WAREHOUSE/t2"))

        assertEquals(ScanRootRejection.CONTAINS_WAREHOUSE_ROOT, plan.roots.single { it.location == WAREHOUSE }.rejection)
    }

    @Test
    fun `a parent inside another table's location is refused`() {
        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = listOf("$DATABASE_LOCATION/t1", "$DATABASE_LOCATION/t1/nested/t2"))

        assertEquals(
            ScanRootRejection.INSIDE_TABLE_LOCATION,
            plan.roots.single { it.location == "$DATABASE_LOCATION/t1/nested" }.rejection,
        )
    }

    @Test
    fun `an adjacent database folder is not mistaken for this database`() {
        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = listOf("$DATABASE_LOCATION/t1", "s3a://bucket/lake/sales_v2/t2"))

        assertEquals(
            ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY,
            plan.roots.single { it.location == "s3a://bucket/lake/sales_v2" }.rejection,
        )
    }

    @Test
    fun `an unqualified table location is refused as invalid`() {
        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = listOf("$DATABASE_LOCATION/t1", "relative/path/t2"))

        assertEquals(ScanRootRejection.INVALID_LOCATION, plan.roots.single { it.location.endsWith("relative/path") }.rejection)
    }

    @Test
    fun `report-only roots are bounded`() {
        val tables = (1..MAX_REPORT_ONLY_SCAN_ROOTS + 3).map { "$DATABASE_LOCATION/part_${"%02d".format(it)}/t" }

        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = tables)

        assertEquals(MAX_REPORT_ONLY_SCAN_ROOTS, plan.reportOnlyRoots.size)
        assertEquals(3, plan.roots.count { it.rejection == ScanRootRejection.ROOT_LIMIT_REACHED })
    }

    @Test
    fun `only the cleanup root can delete, even when every other root is accepted`() {
        val plan =
            plan(
                cleanupRoot = DATABASE_LOCATION,
                tables = listOf("$DATABASE_LOCATION/t1", "$REST_ROOT/t2", "$DATABASE_LOCATION/archive/t3"),
                restNamespaceRoot = REST_ROOT,
            )

        assertEquals(listOf(DATABASE_LOCATION), plan.roots.filter { it.cleanupRoot }.map { it.location })
        assertTrue(plan.roots.filterNot { it.cleanupRoot }.all { it.reportOnly })
        assertFalse(plan.cleanupRoot!!.reportOnly)
    }

    @Test
    fun `a database with one parent plans only its cleanup root`() {
        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = listOf("$DATABASE_LOCATION/t1", "$DATABASE_LOCATION/t2"))

        assertEquals(listOf(DATABASE_LOCATION), plan.roots.map { it.location })
        assertNull(plan.cleanupRoot?.rejection)
    }

    @Test
    fun `an approved custom root with a visible table is listed for reporting only`() {
        val plan =
            plan(
                cleanupRoot = DATABASE_LOCATION,
                tables = listOf("$DATABASE_LOCATION/t1", "s3a://bucket2/ext/a/t2"),
                approvedRoots = listOf("s3a://bucket2/ext/a/"),
            )

        val approved = plan.roots.single { it.location == "s3a://bucket2/ext/a" }
        assertTrue(approved.reportOnly)
        assertTrue(approved.approved)
        assertFalse(approved.cleanupRoot)
    }

    @Test
    fun `an approved root needs a visible table of this database`() {
        val plan = plan(cleanupRoot = DATABASE_LOCATION, tables = listOf("$DATABASE_LOCATION/t1"), approvedRoots = listOf("s3a://bucket2/ext/a"))

        assertEquals(ScanRootRejection.NO_VISIBLE_TABLE, plan.roots.single { it.location == "s3a://bucket2/ext/a" }.rejection)
    }

    @Test
    fun `approval does not override structural checks`() {
        val plan =
            plan(
                cleanupRoot = "s3a://bucket2/dbs/sales.db",
                databaseLocation = "s3a://bucket2/dbs/sales.db",
                tables =
                    listOf(
                        "s3a://bucket2/dbs/sales.db/t1",
                        "s3a://bucket3/t2",
                        "s3a://bucket2/data/other_lake/t3",
                        "s3a://bucket2/dbs/x/t4",
                        "s3a://bucket2/ext/t5",
                    ),
                approvedRoots =
                    listOf("s3a://bucket3", "s3a://bucket2/data", "s3a://bucket2/dbs", "s3a://bucket2/ext/t5/inner"),
                warehouseRoots = listOf(WAREHOUSE, "s3a://bucket2/data/other_lake"),
            )

        val rejections = plan.roots.associate { it.location to it.rejection }
        assertEquals(ScanRootRejection.BUCKET_ROOT, rejections["s3a://bucket3"])
        assertEquals(ScanRootRejection.CONTAINS_WAREHOUSE_ROOT, rejections["s3a://bucket2/data"])
        assertEquals(ScanRootRejection.CONTAINS_DATABASE_STORAGE, rejections["s3a://bucket2/dbs"])
        assertEquals(ScanRootRejection.INSIDE_TABLE_LOCATION, rejections["s3a://bucket2/ext/t5/inner"])
    }

    @Test
    fun `a table parent inside an approved root is listed too`() {
        val plan =
            plan(
                cleanupRoot = DATABASE_LOCATION,
                tables = listOf("$DATABASE_LOCATION/t1", "s3a://bucket2/ext/a/t2", "s3a://bucket2/ext/a/sub/t3"),
                approvedRoots = listOf("s3a://bucket2/ext/a"),
            )

        assertTrue(plan.roots.single { it.location == "s3a://bucket2/ext/a/sub" }.reportOnly)
    }

    @Test
    fun `a refused database lists only the REST root and approved roots`() {
        val plan =
            plan(
                cleanupRoot = null,
                databaseLocation = null,
                tables = listOf("s3a://bucket3/sales_data/t1", "s3a://bucket3/ext/t2", "$REST_ROOT/t3"),
                restNamespaceRoot = REST_ROOT,
                approvedRoots = listOf("s3a://bucket3/ext"),
            )

        assertEquals(null, plan.cleanupRoot)
        assertEquals(listOf(REST_ROOT, "s3a://bucket3/ext"), plan.reportOnlyRoots.map { it.location }.sorted())
        assertEquals(
            ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY,
            plan.roots.single { it.location == "s3a://bucket3/sales_data" }.rejection,
        )
    }

    private fun plan(
        cleanupRoot: String?,
        tables: List<String>,
        databaseLocation: String? = DATABASE_LOCATION,
        restNamespaceRoot: String? = null,
        approvedRoots: List<String> = emptyList(),
        warehouseRoots: List<String> = listOf(WAREHOUSE),
    ): List<PlannedScanRoot> =
        planner.plan(
            cleanupRoot = cleanupRoot,
            databaseLocation = databaseLocation,
            restNamespaceRoot = restNamespaceRoot,
            approvedRoots = approvedRoots,
            activeTableLocations = tables,
            warehouseRoots = warehouseRoots,
        )
}

private val List<PlannedScanRoot>.roots: List<PlannedScanRoot> get() = this
private val List<PlannedScanRoot>.cleanupRoot: PlannedScanRoot? get() = singleOrNull { it.cleanupRoot }
private val List<PlannedScanRoot>.reportOnlyRoots: List<PlannedScanRoot> get() = filter { it.reportOnly }
