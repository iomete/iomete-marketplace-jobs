package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
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

    private fun folders(vararg paths: String) = paths.map { StorageFolder(it) }

    private fun plan(vararg roots: PlannedScanRoot) = roots.toList()

    private fun cleanup(location: String) = PlannedScanRoot(location, setOf(ScanRootSource.DATABASE_LOCATION), cleanupRoot = true)

    private fun reportOnly(location: String) = PlannedScanRoot(location, setOf(ScanRootSource.TABLE_PARENT), cleanupRoot = false)
}
