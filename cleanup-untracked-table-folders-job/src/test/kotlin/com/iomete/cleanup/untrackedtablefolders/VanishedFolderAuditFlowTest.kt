package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.ENDPOINT
import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.FileNotFoundException

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val DATABASE_LOCATION = "s3a://bucket/lake/sales.db"
private const val TABLE_LOCATION = "$DATABASE_LOCATION/orders"

class VanishedFolderAuditFlowTest {

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        putFolder(DATABASE_LOCATION)
        putFolder(TABLE_LOCATION, "part-0.parquet")
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `a folder that disappears during inspection reaches the audit row as folder_not_found`() {
        putFolder("$DATABASE_LOCATION/orphan", "part-0.parquet")
        putFolder("$DATABASE_LOCATION/gone", "part-0.parquet")
        InMemoryS3FileSystem.listFailures["$DATABASE_LOCATION/gone"] = FileNotFoundException("No such file or directory: gone")
        InMemoryS3FileSystem.statusFailures["$DATABASE_LOCATION/gone"] = FileNotFoundException("No such file or directory: gone")
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.candidateFolders)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        assertEquals("1", record.diagnosticDetails["folder_not_found_count"])
        assertEquals("$DATABASE_LOCATION/gone", record.diagnosticDetails["folder_not_found_paths_sample"])
        assertEquals("false", record.diagnosticDetails["folder_not_found_paths_truncated"])
        assertFalse(record.diagnosticDetails.containsKey("folder_not_found_at_deletion_count"))
    }

    @Test
    fun `a folder that disappears during deletion reaches the audit row as folder_not_found_at_deletion`() {
        putFolder("$DATABASE_LOCATION/orphan", "part-0.parquet")
        putFolder("$DATABASE_LOCATION/racing", "part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "$DATABASE_LOCATION/racing") {
                InMemoryS3FileSystem.store(ENDPOINT).keys.removeIf { it == target || it.startsWith("$target/") }
            }
        }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf("$DATABASE_LOCATION/orphan", "$DATABASE_LOCATION/racing"), record.candidateFolders.sorted())
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        assertEquals(1L, record.deletedFolderCount)
        assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan/part-0.parquet"))
        assertTrue(flow.objectExists("$TABLE_LOCATION/part-0.parquet"))
        assertEquals("1", record.diagnosticDetails["folder_not_found_at_deletion_count"])
        assertEquals("$DATABASE_LOCATION/racing", record.diagnosticDetails["folder_not_found_at_deletion_paths_sample"])
        assertEquals("false", record.diagnosticDetails["folder_not_found_at_deletion_paths_truncated"])
        assertFalse(record.diagnosticDetails.containsKey("folder_not_found_count"))
    }

    private fun flow(): CleanupFlowHarness =
        CleanupFlowHarness(
            ApplicationConfig(
                catalog = CATALOG,
                databases = listOf("sales"),
                dryRun = false,
                deleteEnabled = true,
            ),
        ).also {
            every { it.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns
                DiscoveredDatabase(
                    catalog = CATALOG,
                    database = "sales",
                    location = DATABASE_LOCATION,
                    tables = listOf(DiscoveredTable(CATALOG, "sales", "orders", isTemporary = false, location = TABLE_LOCATION)),
                )
            every { it.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        }
}
