package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.CleanupRootListing
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.storage.PlannedScanRoot
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootOutcome
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootRejection
import com.iomete.cleanup.untrackedtablefolders.storage.ScanRootSource
import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ReportOnlyRootScannerTest {

    private val discovery = mockk<ObjectStorageDiscoveryService>()

    private val scanner =
        ReportOnlyRootScanner().apply {
            objectStorageDiscoveryService = discovery
            untrackedFolderCandidateDetector = UntrackedFolderCandidateDetector()
            excludePathResolver =
                ExcludePathResolver().apply { config = ApplicationConfig(catalog = "c", databases = listOf("sales")) }
        }

    @Test
    fun `a folder that is another accepted root is reported only by that root`() {
        every { discovery.listImmediateChildFoldersIfPresent("c", "s3a://b/lake/sales") } returns
            folders("s3a://b/lake/sales/inner", "s3a://b/lake/sales/orphan")
        every { discovery.listImmediateChildFoldersIfPresent("c", "s3a://b/lake/sales/inner") } returns
            folders("s3a://b/lake/sales/inner/old")

        val report =
            scanner.scan(
                "c",
                "sales",
                plan(cleanup("s3a://b/lake/sales.db"), reportOnly("s3a://b/lake/sales"), reportOnly("s3a://b/lake/sales/inner")),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1"),
            )

        assertEquals(listOf("s3a://b/lake/sales/inner/old", "s3a://b/lake/sales/orphan"), report.reportOnlyFolderPaths)
    }

    @Test
    fun `refused roots and the cleanup root are never listed here`() {
        val report =
            scanner.scan(
                "c",
                "sales",
                plan(
                    cleanup("s3a://b/lake/sales.db"),
                    PlannedScanRoot("s3a://other/x", setOf(ScanRootSource.TABLE_PARENT), cleanupRoot = false, rejection = ScanRootRejection.OUTSIDE_DATABASE_BOUNDARY),
                ),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1", "s3a://other/x/t2"),
            )

        verify(exactly = 0) { discovery.listImmediateChildFoldersIfPresent(any(), any()) }
        assertEquals(listOf(ScanRootOutcome.SCANNED, ScanRootOutcome.NOT_SCANNED), report.roots.map { it.outcome })
        assertEquals(listOf("s3a://other/x/t2"), report.uncoveredTableLocations)
        assertFalse(report.complete)
    }

    @Test
    fun `a listing failure marks only that root and leaves coverage partial`() {
        every { discovery.listImmediateChildFoldersIfPresent("c", "s3a://b/lake/sales") } throws IllegalStateException("denied")

        val report =
            scanner.scan(
                "c",
                "sales",
                plan(cleanup("s3a://b/lake/sales.db"), reportOnly("s3a://b/lake/sales")),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1"),
            )

        assertEquals(ScanRootOutcome.LISTING_FAILED, report.roots.single { it.root.location == "s3a://b/lake/sales" }.outcome)
        assertFalse(report.complete)
    }

    @Test
    fun `an absent root covers nothing and still counts as complete`() {
        every { discovery.listImmediateChildFoldersIfPresent("c", "s3a://b/lake/sales") } returns null

        val report =
            scanner.scan(
                "c",
                "sales",
                plan(cleanup("s3a://b/lake/sales.db"), reportOnly("s3a://b/lake/sales")),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1"),
            )

        assertEquals(ScanRootOutcome.ABSENT, report.roots.single { it.root.location == "s3a://b/lake/sales" }.outcome)
        assertTrue(report.complete)
    }

    @Test
    fun `a malformed root rejected by the planner is reported, not parsed`() {
        val report =
            scanner.scan(
                "c",
                "sales",
                plan(
                    cleanup("s3a://b/lake/sales.db"),
                    PlannedScanRoot("://bad", setOf(ScanRootSource.APPROVED), cleanupRoot = false, rejection = ScanRootRejection.INVALID_LOCATION),
                ),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1"),
            )

        val malformed = report.roots.single { it.root.location == "://bad" }
        assertEquals(ScanRootOutcome.NOT_SCANNED, malformed.outcome)
        assertEquals(0, malformed.visibleTableCount)
    }

    @Test
    fun `a table covered only by a report-only root leaves coverage partial`() {
        every { discovery.listImmediateChildFoldersIfPresent("c", "s3a://b/lake/sales") } returns folders("s3a://b/lake/sales/t2")

        val report =
            scanner.scan(
                "c",
                "sales",
                plan(cleanup("s3a://b/lake/sales.db"), reportOnly("s3a://b/lake/sales")),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1", "s3a://b/lake/sales/t2"),
            )

        assertEquals(emptyList<String>(), report.uncoveredTableLocations)
        assertEquals(1, report.visibleTablesOutsideCleanupRoot)
        assertFalse(report.complete)
    }

    @Test
    fun `a plan without a cleanup root never counts as complete`() {
        every { discovery.listImmediateChildFoldersIfPresent("c", "s3a://b/lake/sales") } returns emptyList()

        val report =
            scanner.scan("c", "sales", plan(reportOnly("s3a://b/lake/sales")), activeTableLocations = listOf("s3a://b/lake/sales/t1"))

        assertNull(report.visibleTablesOutsideCleanupRoot)
        assertFalse(report.complete)
    }

    @Test
    fun `listed roots record how many child folders they hold`() {
        every { discovery.listImmediateChildFoldersIfPresent("c", "s3a://b/lake/sales") } returns emptyList()

        val report =
            scanner.scan(
                "c",
                "sales",
                plan(cleanup("s3a://b/lake/sales.db"), reportOnly("s3a://b/lake/sales")),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1"),
                cleanupRootListings = mapOf("s3a://b/lake/sales.db" to CleanupRootListing(ScanRootOutcome.SCANNED, folderCount = 4)),
            )

        assertEquals(4, report.roots.single { it.root.cleanupRoot }.listedFolderCount)
        val empty = report.roots.single { it.root.location == "s3a://b/lake/sales" }
        assertEquals(ScanRootOutcome.SCANNED, empty.outcome)
        assertEquals(0, empty.listedFolderCount)
    }

    @Test
    fun `tables in a cleanup root that could not be listed count as outside every cleanup root`() {
        val report =
            scanner.scan(
                "c",
                "sales",
                plan(cleanup("s3a://b/lake/sales.db"), cleanup("s3a://b/lake/sales")),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1", "s3a://b/lake/sales/t2"),
                cleanupRootListings =
                    mapOf(
                        "s3a://b/lake/sales.db" to CleanupRootListing(ScanRootOutcome.SCANNED, folderCount = 2),
                        "s3a://b/lake/sales" to CleanupRootListing(ScanRootOutcome.LISTING_FAILED),
                    ),
            )

        assertEquals(1, report.visibleTablesOutsideCleanupRoot)
        assertEquals(ScanRootOutcome.LISTING_FAILED, report.roots.single { it.root.location == "s3a://b/lake/sales" }.outcome)
        assertFalse(report.complete)
        verify(exactly = 0) { discovery.listImmediateChildFoldersIfPresent(any(), any()) }
    }

    @Test
    fun `two listed cleanup roots cover the tables in both`() {
        val report =
            scanner.scan(
                "c",
                "sales",
                plan(cleanup("s3a://b/lake/sales.db"), cleanup("s3a://b/lake/sales")),
                activeTableLocations = listOf("s3a://b/lake/sales.db/t1", "s3a://b/lake/sales/t2"),
                cleanupRootListings =
                    mapOf(
                        "s3a://b/lake/sales.db" to CleanupRootListing(ScanRootOutcome.ABSENT),
                        "s3a://b/lake/sales" to CleanupRootListing(ScanRootOutcome.SCANNED, folderCount = 1),
                    ),
            )

        assertEquals(0, report.visibleTablesOutsideCleanupRoot)
        assertTrue(report.complete)
        assertEquals("absent", report.roots.single { it.root.location == "s3a://b/lake/sales.db" }.status)
    }

    private fun folders(vararg paths: String) = paths.map { StorageFolder(it) }

    private fun plan(vararg roots: PlannedScanRoot) = roots.toList()

    private fun cleanup(location: String) = PlannedScanRoot(location, setOf(ScanRootSource.DATABASE_LOCATION), cleanupRoot = true)

    private fun reportOnly(location: String) = PlannedScanRoot(location, setOf(ScanRootSource.TABLE_PARENT), cleanupRoot = false)
}
