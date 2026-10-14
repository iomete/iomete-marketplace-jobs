package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditDiagnosticDetailsBuilder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecorder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummary
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummaryLogger
import com.iomete.cleanup.untrackedtablefolders.service.CleanupUntrackedTableFoldersService
import com.iomete.cleanup.untrackedtablefolders.service.ReportOnlyRootScanner
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateDeletionGate
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateSizeStatCollector
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogStorageTestHarness
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import com.iomete.cleanup.untrackedtablefolders.storage.StorageScanLocationPlanner
import com.iomete.cleanup.untrackedtablefolders.storage.StorageScanLocationResolver
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.spyk

/** Real cleanup flow over [InMemoryS3FileSystem]; only catalog discovery and the audit table are faked. */
class CleanupFlowHarness(
    config: ApplicationConfig,
) {
    val catalogDiscoveryService: CatalogDiscoveryService = mockk()
    val auditRecords = mutableListOf<CleanupAuditRecord>()
    val summaries = mutableListOf<CleanupSummary>()

    private val auditTableService: CleanupAuditTableService = mockk(relaxed = true)
    private val storage =
        CatalogStorageTestHarness(
            catalogProperties = mapOf(config.catalog to CatalogStorageTestHarness.s3CompatibleCatalogProperties()),
        )

    val service: CleanupUntrackedTableFoldersService

    init {
        every { auditTableService.writeAuditRecord(capture(auditRecords)) } just Runs
        every { catalogDiscoveryService.restNamespaceRoot(any(), any()) } returns null
        every { catalogDiscoveryService.knownWarehouseRoots() } returns emptyList()

        val excludePathResolver = ExcludePathResolver().also { it.config = config }
        val discovery = storage.discoveryService()

        service =
            CleanupUntrackedTableFoldersService().also {
                it.config = config
                it.catalogDiscoveryService = catalogDiscoveryService
                it.objectStorageDiscoveryService = discovery
                it.untrackedFolderCandidateDetector = UntrackedFolderCandidateDetector()
                it.cleanupAuditTableService = auditTableService
                it.cleanupAuditRecorder =
                    CleanupAuditRecorder().also { recorder ->
                        recorder.config = config
                        recorder.cleanupAuditTableService = auditTableService
                        recorder.auditDiagnosticDetailsBuilder = CleanupAuditDiagnosticDetailsBuilder()
                        recorder.excludePathResolver = excludePathResolver
                    }
                it.cleanupSummaryLogger =
                    spyk(CleanupSummaryLogger()).also { logger ->
                        every { logger.logCleanupSummary(capture(summaries)) } answers { callOriginal() }
                    }
                it.storageScanLocationResolver = StorageScanLocationResolver()
                it.excludePathResolver = excludePathResolver
                it.candidateSizeStatCollector =
                    CandidateSizeStatCollector().also { collector ->
                        collector.config = config
                        collector.objectStorageDiscoveryService = discovery
                    }
                it.candidateDeletionGate =
                    CandidateDeletionGate().also { gate ->
                        gate.config = config
                        gate.catalogDiscoveryService = catalogDiscoveryService
                        gate.objectStorageDeletionService = storage.deletionService()
                    }
                it.storageScanLocationPlanner = StorageScanLocationPlanner()
                it.reportOnlyRootScanner =
                    ReportOnlyRootScanner().also { scanner ->
                        scanner.objectStorageDiscoveryService = discovery
                        scanner.untrackedFolderCandidateDetector = UntrackedFolderCandidateDetector()
                        scanner.excludePathResolver = excludePathResolver
                    }
            }
    }

    fun run() = service.run()

    fun summaryLines(): List<String> = CleanupSummaryLogger().buildReportLines(summaries.single())

    fun objectExists(path: String): Boolean = path in InMemoryS3FileSystem.paths(ENDPOINT)

    companion object {
        const val ENDPOINT = CatalogStorageTestHarness.COMPAT_ENDPOINT

        fun putFolder(
            path: String,
            vararg files: String,
        ) {
            InMemoryS3FileSystem.putDirectory(ENDPOINT, path)
            files.forEach { InMemoryS3FileSystem.putFile(ENDPOINT, "$path/$it", length = 1) }
        }
    }
}
