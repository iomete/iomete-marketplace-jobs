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
        // Inspected (first listing), then removed before the listing that precedes deletion.
        InMemoryS3FileSystem.afterListFiles = { root, call ->
            if (root == "$DATABASE_LOCATION/racing" && call == 1) {
                InMemoryS3FileSystem.store(ENDPOINT).keys.removeIf { it == root || it.startsWith("$root/") }
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

    @Test
    fun `an object that disappears after the listing before deletion stops deletion and is not reported as deleted`() {
        putFolder("$DATABASE_LOCATION/orphan", "part-0.parquet")
        putFolder("$DATABASE_LOCATION/racing", "part-0.parquet", "part-1.parquet")
        putFolder("$DATABASE_LOCATION/zz_orphan", "part-0.parquet")
        // The first status read checks every key before the first delete; the object goes right before the second.
        InMemoryS3FileSystem.beforeGetFileStatus = { target ->
            if (target == "$DATABASE_LOCATION/racing/part-0.parquet" && InMemoryS3FileSystem.getFileStatusCalls[target] == 2) {
                InMemoryS3FileSystem.store(ENDPOINT).remove(target)
            }
        }
        val flow = flow()

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertEquals("deletion_failed", record.statusReason)
        assertEquals(listOf("$DATABASE_LOCATION/orphan"), record.deletedFolders)
        assertEquals("$DATABASE_LOCATION/racing", record.diagnosticDetails["deletion_failed_folder_path"])
        assertTrue(record.errorMessage.orEmpty().contains("folder $DATABASE_LOCATION/racing changed during deletion: 0 of 2 listed object(s) deleted, 1 kept"))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/racing/part-1.parquet"))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/zz_orphan/part-0.parquet"))
        assertFalse(record.diagnosticDetails.containsKey("folder_not_found_at_deletion_count"))
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
