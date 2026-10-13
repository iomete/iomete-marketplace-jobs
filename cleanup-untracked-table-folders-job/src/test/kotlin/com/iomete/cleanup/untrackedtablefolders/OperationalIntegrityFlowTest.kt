package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.service.AuditIntegrityLostException
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"

class OperationalIntegrityFlowTest {

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `a failed deletion reports the folders already deleted before it`() {
        database("a", orphans = listOf("orphan_1", "orphan_2", "orphan_3"))
        InMemoryS3FileSystem.failDeleteFor = location("a", "orphan_2")
        val flow = flow(listOf("a"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertEquals(listOf(location("a", "orphan_1")), record.deletedFolders)
        assertEquals(1, record.deletedFolderCount)
        assertFalse(flow.objectExists("${location("a", "orphan_1")}/part-0.parquet"))
        assertTrue(flow.objectExists("${location("a", "orphan_3")}/part-0.parquet"))
    }

    @Test
    fun `a refused delete stops deletion and keeps earlier confirmed deletions`() {
        database("a", orphans = listOf("orphan_1", "orphan_2", "orphan_3"))
        InMemoryS3FileSystem.refuseDeleteFor = location("a", "orphan_2")
        val flow = flow(listOf("a"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertEquals(listOf(location("a", "orphan_1")), record.deletedFolders)
        assertTrue(flow.objectExists("${location("a", "orphan_3")}/part-0.parquet"))
    }

    @Test
    fun `an audit write failure blocks deletion in later databases`() {
        database("a", orphans = listOf("orphan"))
        database("b", orphans = listOf("orphan"))
        val flow = flow(listOf("a", "b"))
        failFirstAuditWriteFor(flow, "a")

        runCatching { flow.run() }

        assertFalse(flow.objectExists("${location("a", "orphan")}/part-0.parquet"))
        assertTrue(flow.objectExists("${location("b", "orphan")}/part-0.parquet"))
    }

    @Test
    fun `later databases are reconciled and recorded as audit_integrity_lost`() {
        database("a", orphans = listOf("orphan"))
        database("b", orphans = listOf("orphan"))
        val flow = flow(listOf("a", "b"))
        failFirstAuditWriteFor(flow, "a")

        runCatching { flow.run() }

        val recordForB = flow.auditRecords.single { it.databaseName == "b" }
        assertEquals("BLOCKED", recordForB.status)
        assertEquals("audit_integrity_lost", recordForB.statusReason)
        assertEquals(listOf(location("b", "orphan")), recordForB.candidateFolders)
        assertEquals(0, recordForB.deletedFolderCount)
    }

    @Test
    fun `a failed audit write is not followed by a misleading FAILED row for the same database`() {
        database("a", orphans = listOf("orphan"))
        val flow = flow(listOf("a"))
        failFirstAuditWriteFor(flow, "a")

        runCatching { flow.run() }

        assertFalse(flow.objectExists("${location("a", "orphan")}/part-0.parquet"))
        assertTrue(flow.auditRecords.none { it.databaseName == "a" })
    }

    @Test
    fun `the run fails once audit integrity is lost`() {
        database("a", orphans = listOf("orphan"))
        database("b", orphans = listOf("orphan"))
        val flow = flow(listOf("a", "b"))
        failFirstAuditWriteFor(flow, "a")

        val error = assertThrows(AuditIntegrityLostException::class.java) { flow.run() }

        assertEquals("audit table unavailable", error.cause?.message)
        assertFalse(flow.objectExists("${location("a", "orphan")}/part-0.parquet"))
    }

    @Test
    fun `an audit failure while recording a database failure does not stop later databases`() {
        database("b", orphans = listOf("orphan"))
        val flow = flow(listOf("a", "b"))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "a") } throws IllegalStateException("catalog unavailable")
        every { flow.service.cleanupAuditTableService.writeAuditRecord(any()) } answers {
            val record = firstArg<CleanupAuditRecord>()
            if (record.databaseName == "a") throw IllegalStateException("audit table unavailable")
            flow.auditRecords += record
        }

        runCatching { flow.run() }

        assertEquals("audit_integrity_lost", flow.auditRecords.single { it.databaseName == "b" }.statusReason)
        assertTrue(flow.objectExists("${location("b", "orphan")}/part-0.parquet"))
    }

    @Test
    fun `a failure to prepare the audit table deletes nothing`() {
        database("a", orphans = listOf("orphan"))
        val flow = flow(listOf("a"))
        every { flow.service.cleanupAuditTableService.ensureAuditTableExists() } throws IllegalStateException("cannot create audit table")

        assertThrows(IllegalStateException::class.java) { flow.run() }

        assertTrue(flow.objectExists("${location("a", "orphan")}/part-0.parquet"))
    }

    @Test
    fun `an audit failure while skipping an unsafe scan root blocks deletion in later databases`() {
        database("a", orphans = emptyList(), databaseLocation = WAREHOUSE)
        database("b", orphans = listOf("orphan"))
        val flow = flow(listOf("a", "b"))
        failFirstAuditWriteFor(flow, "a")

        assertThrows(AuditIntegrityLostException::class.java) { flow.run() }

        assertTrue(flow.objectExists("${location("b", "orphan")}/part-0.parquet"))
        assertTrue(flow.auditRecords.none { it.databaseName == "a" })
        assertEquals("audit_integrity_lost", flow.auditRecords.single { it.databaseName == "b" }.statusReason)
    }

    @Test
    fun `a failure after the scan keeps partial scan coverage on the FAILED row`() {
        database("a", orphans = listOf("orphan"), tablesOutsideRoot = listOf("s3a://bucket/other/events"))
        InMemoryS3FileSystem.failListFor = location("a", "orphan")
        val flow = flow(listOf("a"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertEquals("unexpected_error", record.statusReason)
        assertEquals("1", record.diagnosticDetails["unscanned_table_location_count"])
        assertEquals("s3a://bucket/other/events", record.diagnosticDetails["unscanned_table_locations_sample"])
    }

    @Test
    fun `a failure after a complete scan records zero unscanned table locations`() {
        database("a", orphans = listOf("orphan"))
        InMemoryS3FileSystem.failListFor = location("a", "orphan")
        val flow = flow(listOf("a"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertEquals("0", record.diagnosticDetails["unscanned_table_location_count"])
    }

    @Test
    fun `a failure before the scan records no scan coverage`() {
        val flow = flow(listOf("a"))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "a") } throws IllegalStateException("catalog unavailable")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("FAILED", record.status)
        assertFalse(record.diagnosticDetails.containsKey("unscanned_table_location_count"))
    }

    @Test
    fun `a partial deletion keeps partial scan coverage`() {
        database("a", orphans = listOf("orphan_1", "orphan_2"), tablesOutsideRoot = listOf("s3a://bucket/other/events"))
        InMemoryS3FileSystem.failDeleteFor = location("a", "orphan_2")
        val flow = flow(listOf("a"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("deletion_failed", record.statusReason)
        assertEquals("1", record.diagnosticDetails["unscanned_table_location_count"])
        assertEquals(location("a", "orphan_2"), record.diagnosticDetails["deletion_failed_folder_path"])
    }

    @Test
    fun `a partial deletion reports the size of the folders it deleted`() {
        database("a", orphans = listOf("orphan_1", "orphan_2"))
        InMemoryS3FileSystem.failDeleteFor = location("a", "orphan_2")
        val flow = flow(listOf("a"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf(location("a", "orphan_1")), record.deletedFolders)
        assertEquals(1L, record.deletedObjectCount)
        assertEquals(1L, record.deletedTotalSizeBytes)
    }

    @Test
    fun `audit integrity loss keeps partial scan coverage on later databases`() {
        database("a", orphans = listOf("orphan"))
        database("b", orphans = listOf("orphan"), tablesOutsideRoot = listOf("s3a://bucket/other/events"))
        val flow = flow(listOf("a", "b"))
        failFirstAuditWriteFor(flow, "a")

        runCatching { flow.run() }

        val recordForB = flow.auditRecords.single { it.databaseName == "b" }
        assertEquals("audit_integrity_lost", recordForB.statusReason)
        assertEquals("1", recordForB.diagnosticDetails["unscanned_table_location_count"])
    }

    @Test
    fun `a dry run after audit integrity is lost is recorded as audit_integrity_lost`() {
        database("a", orphans = listOf("orphan"))
        database("b", orphans = listOf("orphan"))
        val flow = flow(listOf("a", "b"), dryRun = true)
        failFirstAuditWriteFor(flow, "a")

        assertThrows(AuditIntegrityLostException::class.java) { flow.run() }

        val recordForB = flow.auditRecords.single { it.databaseName == "b" }
        assertEquals("BLOCKED", recordForB.status)
        assertEquals("audit_integrity_lost", recordForB.statusReason)
        assertTrue(flow.objectExists("${location("b", "orphan")}/part-0.parquet"))
    }

    @Test
    fun `excluded folders are never deleted around a partial deletion`() {
        database("a", orphans = listOf("orphan_1", "orphan_2", "orphan_keep"))
        InMemoryS3FileSystem.failDeleteFor = location("a", "orphan_2")
        val flow = flow(listOf("a"), excludePaths = listOf(location("a", "orphan_keep")))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("deletion_failed", record.statusReason)
        assertEquals(listOf(location("a", "orphan_1")), record.deletedFolders)
        assertTrue(flow.objectExists("${location("a", "orphan_keep")}/part-0.parquet"))
    }

    private val databases = mutableMapOf<String, DiscoveredDatabase>()

    private fun location(
        database: String,
        folder: String? = null,
    ): String = "s3a://bucket/lake/$database.db" + (folder?.let { "/$it" } ?: "")

    private fun database(
        name: String,
        orphans: List<String>,
        databaseLocation: String = location(name),
        tablesOutsideRoot: List<String> = emptyList(),
    ) {
        putFolder(databaseLocation)
        putFolder(location(name, "orders"), "part-0.parquet")
        orphans.forEach { putFolder(location(name, it), "part-0.parquet") }
        databases[name] =
            DiscoveredDatabase(
                CATALOG,
                name,
                databaseLocation,
                listOf(DiscoveredTable(CATALOG, name, "orders", isTemporary = false, location = location(name, "orders"))) +
                    tablesOutsideRoot.mapIndexed { index, tableLocation ->
                        DiscoveredTable(CATALOG, name, "outside_$index", isTemporary = false, location = tableLocation)
                    },
            )
    }

    private fun flow(
        names: List<String>,
        dryRun: Boolean = false,
        excludePaths: List<String> = emptyList(),
    ): CleanupFlowHarness {
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(
                    catalog = CATALOG,
                    databases = names,
                    dryRun = dryRun,
                    deleteEnabled = !dryRun,
                    excludePaths = excludePaths,
                ),
            )
        databases.forEach { (name, database) ->
            every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, name) } returns database
        }
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        return flow
    }

    private fun failFirstAuditWriteFor(
        flow: CleanupFlowHarness,
        database: String,
    ) {
        var failed = false
        every { flow.service.cleanupAuditTableService.writeAuditRecord(any()) } answers {
            val record = firstArg<CleanupAuditRecord>()
            if (record.databaseName == database && !failed) {
                failed = true
                throw IllegalStateException("audit table unavailable")
            }
            flow.auditRecords += record
        }
    }
}
