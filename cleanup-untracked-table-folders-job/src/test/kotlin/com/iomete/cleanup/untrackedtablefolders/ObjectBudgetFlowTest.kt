package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.ENDPOINT
import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import com.iomete.cleanup.untrackedtablefolders.storage.MAX_OBJECTS_PER_FOLDER
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val DATABASE_LOCATION = "s3a://bucket/lake/sales.db"
private const val TABLE_LOCATION = "$DATABASE_LOCATION/orders"
private const val CROWDED = "$DATABASE_LOCATION/crowded"
private const val ORPHAN = "$DATABASE_LOCATION/orphan"

class ObjectBudgetFlowTest {

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        putFolder(DATABASE_LOCATION)
        putFolder(TABLE_LOCATION, "part-0.parquet")
        putFolder(ORPHAN, "part-0.parquet")
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `deletion mode holds a folder above the object budget and deletes the others object by object`() {
        putCrowded(MAX_OBJECTS_PER_FOLDER + 1)
        val flow = flow(dryRun = false)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf(ORPHAN), record.candidateFolders)
        assertEquals(listOf(ORPHAN), record.deletedFolders)
        assertEquals("1", record.diagnosticDetails["too_many_objects_count"])
        assertEquals(CROWDED, record.diagnosticDetails["too_many_objects_paths_sample"])
        assertEquals("false", record.diagnosticDetails["too_many_objects_paths_truncated"])
        assertEquals(MAX_OBJECTS_PER_FOLDER + 1, crowdedObjectCount())
        assertEquals(MAX_OBJECTS_PER_FOLDER + 1, InMemoryS3FileSystem.listFilesReturned[CROWDED])
        assertEquals(1, InMemoryS3FileSystem.listFilesCalls[CROWDED])
        assertFalse(flow.objectExists("$ORPHAN/part-0.parquet"))
        assertTrue(flow.objectExists("$TABLE_LOCATION/part-0.parquet"))
        assertEquals(listOf("$ORPHAN/part-0.parquet" to false, ORPHAN to false), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a dry run reports a folder above the object budget as too_many_objects`() {
        putCrowded(MAX_OBJECTS_PER_FOLDER + 1)
        val flow = flow(dryRun = true)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf(ORPHAN), record.candidateFolders)
        assertEquals("1", record.diagnosticDetails["too_many_objects_count"])
        val summary = flow.summaryLines()
        assertTrue("  Folders with too many objects                    1" in summary, summary.joinToString("\n"))
        val explanation = summary.indexOf("  too_many_objects: more than $MAX_OBJECTS_PER_FOLDER objects. The job deletes object by object, so it keeps larger folders.")
        assertTrue(explanation >= 0, summary.joinToString("\n"))
        assertEquals("- $CROWDED", summary[explanation + 2])
        assertEquals(emptyList<Pair<String, Boolean>>(), InMemoryS3FileSystem.deleteCalls.toList())
    }

    @Test
    fun `a folder that grows past the object budget before deletion is held and the other folders are still deleted`() {
        putCrowded(MAX_OBJECTS_PER_FOLDER)
        // Inspected at the budget (first listing), then one more object before the listing that precedes deletion.
        InMemoryS3FileSystem.afterListFiles = { root, call ->
            if (root == CROWDED && call == 1) InMemoryS3FileSystem.putFile(ENDPOINT, "$CROWDED/late.parquet", length = 1)
        }
        val flow = flow(dryRun = false)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(null, record.statusReason)
        assertEquals(listOf(CROWDED, ORPHAN), record.candidateFolders.sorted())
        assertEquals(listOf(ORPHAN), record.deletedFolders)
        assertEquals("1", record.diagnosticDetails["folder_kept_too_many_objects_count"])
        assertEquals(CROWDED, record.diagnosticDetails["folder_kept_too_many_objects_paths_sample"])
        assertFalse(record.diagnosticDetails.containsKey("deletion_failed_folder_path"))
        assertEquals(MAX_OBJECTS_PER_FOLDER + 1, crowdedObjectCount())
        assertFalse(flow.objectExists("$ORPHAN/part-0.parquet"))
        assertEquals(listOf("$ORPHAN/part-0.parquet" to false, ORPHAN to false), InMemoryS3FileSystem.deleteCalls.toList())
        assertTrue("  Folders over the object budget at deletion       1" in flow.summaryLines(), flow.summaryLines().joinToString("\n"))
    }

    private fun putCrowded(objects: Int) {
        putFolder(CROWDED)
        (1..objects).forEach { InMemoryS3FileSystem.putFile(ENDPOINT, "$CROWDED/part-$it.parquet", length = 1) }
    }

    private fun crowdedObjectCount(): Int = InMemoryS3FileSystem.paths(ENDPOINT).count { it.startsWith("$CROWDED/") }

    private fun flow(dryRun: Boolean): CleanupFlowHarness =
        CleanupFlowHarness(
            ApplicationConfig(
                catalog = CATALOG,
                databases = listOf("sales"),
                dryRun = dryRun,
                deleteEnabled = !dryRun,
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
