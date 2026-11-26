package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditDiagnosticDetailsBuilder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecorder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.candidate.StorageHoldChecker
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.candidate.passThroughStorageHoldChecker
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.NamedTableStorage
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
import com.iomete.cleanup.untrackedtablefolders.storage.OWNERSHIP_SNAPSHOT_MAX_AGE
import com.iomete.cleanup.untrackedtablefolders.storage.StorageScanLocationPlanner
import com.iomete.cleanup.untrackedtablefolders.storage.StorageScanLocationResolver
import com.iomete.cleanup.untrackedtablefolders.storage.Ticker
import io.mockk.every
import io.mockk.mockk
import io.mockk.spyk
import org.apache.hadoop.conf.Configuration
import java.time.Duration

/**
 * Real cleanup flow over [InMemoryS3FileSystem]; only catalog discovery and the audit table are faked. The storage
 * holds pass every folder unless [realStorageHolds] is set; then [auditTableService] serves the job id and history.
 */
class CleanupFlowHarness(
    config: ApplicationConfig,
    catalogProperties: Map<String, String> = CatalogStorageTestHarness.s3CompatibleCatalogProperties(),
    sparkHadoopConfiguration: Configuration = CatalogStorageTestHarness.baseHadoopConfiguration(),
    ticker: Ticker = Ticker.SYSTEM,
    snapshotMaxAge: Duration = OWNERSHIP_SNAPSHOT_MAX_AGE,
    realStorageHolds: Boolean = false,
) {
    val catalogDiscoveryService: CatalogDiscoveryService = mockk()

    /** Every row but deletion intents, which are in [intentRecords]. */
    val auditRecords = mutableListOf<CleanupAuditRecord>()
    val intentRecords = mutableListOf<CleanupAuditRecord>()
    val summaries = mutableListOf<CleanupSummary>()

    val auditTableService: CleanupAuditTableService = mockk(relaxed = true)
    private val storage =
        CatalogStorageTestHarness(
            catalogProperties = mapOf(config.catalog to catalogProperties),
            sparkHadoopConfiguration = sparkHadoopConfiguration,
        )

    val service: CleanupUntrackedTableFoldersService

    init {
        every { auditTableService.writeAuditRecord(any()) } answers {
            val record = firstArg<CleanupAuditRecord>()
            if (record.status == "INTENT") intentRecords += record else auditRecords += record
        }
        every { auditTableService.intentRecorded(any(), any(), any(), any()) } answers {
            intentRecords.any { it.runId == firstArg<String>() && it.catalogName == thirdArg<String>() && it.databaseName == arg<String>(3) }
        }
        every { catalogDiscoveryService.restNamespaceRoot(any(), any()) } returns null
        every { catalogDiscoveryService.knownWarehouseRoots() } returns emptyList()
        every { catalogDiscoveryService.visibleDatabaseLocations(any()) } returns emptyMap()
        every { catalogDiscoveryService.tableLocationIfExists(any(), any(), any()) } returns null
        every { catalogDiscoveryService.tableStorageIfExists(any(), any(), any()) } returns NamedTableStorage.NotFound
        every { catalogDiscoveryService.sessionCatalogWarehouses() } returns emptyMap()
        every { catalogDiscoveryService.discoverTables(any(), any()) } returns emptyList()
        every { catalogDiscoveryService.catalogsWithUnverifiedVisibility(any()) } returns emptyList()
        every { catalogDiscoveryService.concurrentReadSafe(any()) } returns false

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
                        gate.ticker = ticker
                        gate.snapshotMaxAge = snapshotMaxAge
                    }
                it.storageScanLocationPlanner = StorageScanLocationPlanner()
                it.storageHoldChecker =
                    if (realStorageHolds) {
                        StorageHoldChecker().also { checker ->
                            checker.objectStorageDiscoveryService = discovery
                            checker.cleanupAuditTableService = auditTableService
                        }
                    } else {
                        passThroughStorageHoldChecker()
                    }
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
