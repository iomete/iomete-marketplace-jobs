package com.iomete.cleanup.untrackedtablefolders.service

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecorder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.candidate.UntrackedFolderCandidateDetector
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.DatabaseNotFoundException
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummaryLogger
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateDeletionGate
import com.iomete.cleanup.untrackedtablefolders.storage.CandidateSizeStatCollector
import com.iomete.cleanup.untrackedtablefolders.storage.ExcludePathResolver
import com.iomete.cleanup.untrackedtablefolders.storage.ObjectStorageDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.storage.StorageFolder
import com.iomete.cleanup.untrackedtablefolders.storage.StorageScanLocationResolver
import com.iomete.cleanup.untrackedtablefolders.storage.StorageSizeStats
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Wiring/integration test for [CleanupUntrackedTableFoldersService.run].
 *
 * I/O collaborators (catalog discovery, storage discovery, audit table writer,
 * size collector, deletion gate) are mocked. Pure-logic collaborators (detector,
 * scan resolver, exclude resolver, summary logger, audit recorder) are real
 * instances. The test verifies that for each input scenario the orchestrator
 * routes through the correct audit recorder method and gates deletion correctly.
 */
class CleanupUntrackedTableFoldersServiceTest {

    private val catalogDiscoveryService = mockk<CatalogDiscoveryService>(relaxed = true)
    private val objectStorageDiscoveryService = mockk<ObjectStorageDiscoveryService>(relaxed = true)
    private val cleanupAuditTableService = mockk<CleanupAuditTableService>(relaxed = true)
    private val cleanupAuditRecorder = mockk<CleanupAuditRecorder>(relaxed = true)
    private val candidateSizeStatCollector = mockk<CandidateSizeStatCollector>(relaxed = true)
    private val candidateDeletionGate = mockk<CandidateDeletionGate>(relaxed = true)

    @BeforeEach
    fun resetMocks() {
        clearMocks(
            catalogDiscoveryService,
            objectStorageDiscoveryService,
            cleanupAuditTableService,
            cleanupAuditRecorder,
            candidateSizeStatCollector,
            candidateDeletionGate,
        )
        every { candidateSizeStatCollector.collectPerFolder(any()) } returns emptyMap()
        every { candidateSizeStatCollector.sum(any()) } returns StorageSizeStats.ZERO
        every { candidateDeletionGate.deleteCandidates(any(), any(), any()) } returns emptyList()
    }

    @Test
    fun `dry-run happy path detects untracked folder, gates deletion, records success`() {
        val service = serviceFor(
            applicationConfig(dryRun = true, deleteEnabled = false, databases = listOf("analytics")),
        )
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            discoveredDatabase(activeTableLocations = listOf("s3a://bucket/db/active_table"))
        every { objectStorageDiscoveryService.listImmediateChildFolders("s3a://bucket/db") } returns
            listOf(
                storageFolder("s3a://bucket/db/active_table"),
                storageFolder("s3a://bucket/db/orphan"),
            )

        service.run()

        verify(exactly = 1) {
            candidateDeletionGate.deleteCandidates(
                catalog = "spark_catalog",
                database = "analytics",
                candidateFolders = match { folders ->
                    folders.map { it.path } == listOf("s3a://bucket/db/orphan")
                },
            )
        }
        verify(exactly = 1) {
            cleanupAuditRecorder.recordSuccess(
                runId = any(),
                databaseStartTime = any(),
                catalogName = "spark_catalog",
                databaseName = "analytics",
                discoveredDatabaseLocation = "s3a://bucket/db",
                storageScanLocation = "s3a://bucket/db",
                activeTableCount = 1,
                activeTableLocations = listOf("s3a://bucket/db/active_table"),
                storageFolderPaths = listOf("s3a://bucket/db/active_table", "s3a://bucket/db/orphan"),
                candidateFolderPaths = listOf("s3a://bucket/db/orphan"),
                candidateSizeStats = any(),
                deletedFolderPaths = emptyList(),
                deletedSizeStats = any(),
                cutoffTime = any(),
                excludedPaths = emptyList(),
            )
        }
        verifyNoOtherAuditCalls()
    }

    @Test
    fun `delete-enabled happy path passes candidates to gate and records gate result in audit`() {
        val service = serviceFor(
            applicationConfig(dryRun = false, deleteEnabled = true, databases = listOf("analytics")),
        )
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            discoveredDatabase(activeTableLocations = listOf("s3a://bucket/db/active_table"))
        every { objectStorageDiscoveryService.listImmediateChildFolders("s3a://bucket/db") } returns
            listOf(
                storageFolder("s3a://bucket/db/active_table"),
                storageFolder("s3a://bucket/db/orphan_a"),
                storageFolder("s3a://bucket/db/orphan_b"),
            )
        every {
            candidateDeletionGate.deleteCandidates(
                catalog = "spark_catalog",
                database = "analytics",
                candidateFolders = any(),
            )
        } returns listOf("s3a://bucket/db/orphan_a", "s3a://bucket/db/orphan_b")

        service.run()

        verify(exactly = 1) {
            cleanupAuditRecorder.recordSuccess(
                runId = any(),
                databaseStartTime = any(),
                catalogName = "spark_catalog",
                databaseName = "analytics",
                discoveredDatabaseLocation = any(),
                storageScanLocation = any(),
                activeTableCount = any(),
                activeTableLocations = any(),
                storageFolderPaths = any(),
                candidateFolderPaths = listOf("s3a://bucket/db/orphan_a", "s3a://bucket/db/orphan_b"),
                candidateSizeStats = any(),
                deletedFolderPaths = listOf("s3a://bucket/db/orphan_a", "s3a://bucket/db/orphan_b"),
                deletedSizeStats = any(),
                cutoffTime = any(),
                excludedPaths = any(),
            )
        }
    }

    @Test
    fun `empty database records SKIPPED and never invokes deletion gate`() {
        val service = serviceFor(
            applicationConfig(dryRun = false, deleteEnabled = true, databases = listOf("empty_db")),
        )
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "empty_db") } returns
            DiscoveredDatabase(
                catalog = "spark_catalog",
                database = "empty_db",
                location = "s3a://bucket/empty",
                tables = emptyList(),
            )

        service.run()

        verify(exactly = 1) {
            cleanupAuditRecorder.recordNoActiveTables(
                runId = any(),
                databaseStartTime = any(),
                catalogName = "spark_catalog",
                databaseName = "empty_db",
                discoveredDatabaseLocation = "s3a://bucket/empty",
                excludedPaths = any(),
            )
        }
        verify(exactly = 0) { candidateDeletionGate.deleteCandidates(any(), any(), any()) }
        verify(exactly = 0) { objectStorageDiscoveryService.listImmediateChildFolders(any()) }
        verifyNoOtherAuditCalls(except = "recordNoActiveTables")
    }

    @Test
    fun `missing database location records SKIPPED with database_location_missing and skips storage scan`() {
        val service = serviceFor(
            applicationConfig(dryRun = false, deleteEnabled = true, databases = listOf("analytics")),
        )
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            DiscoveredDatabase(
                catalog = "spark_catalog",
                database = "analytics",
                location = null,
                tables = listOf(activeTable("s3a://bucket/db/active_table")),
            )

        service.run()

        verify(exactly = 1) {
            cleanupAuditRecorder.recordDatabaseLocationMissing(
                runId = any(),
                databaseStartTime = any(),
                catalogName = "spark_catalog",
                databaseName = "analytics",
                discoveredDatabaseLocation = null,
                activeTableCount = 1,
                activeTableLocations = listOf("s3a://bucket/db/active_table"),
                errorMessage = any(),
                excludedPaths = any(),
            )
        }
        verify(exactly = 0) { objectStorageDiscoveryService.listImmediateChildFolders(any()) }
        verify(exactly = 0) { candidateDeletionGate.deleteCandidates(any(), any(), any()) }
        verifyNoOtherAuditCalls(except = "recordDatabaseLocationMissing")
    }

    @Test
    fun `database not found exception routes to recordDatabaseNotFound`() {
        val service = serviceFor(
            applicationConfig(dryRun = true, deleteEnabled = false, databases = listOf("missing_db")),
        )
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "missing_db") } throws
            DatabaseNotFoundException(
                catalog = "spark_catalog",
                database = "missing_db",
                cause = RuntimeException("simulated catalog miss"),
            )

        service.run()

        verify(exactly = 1) {
            cleanupAuditRecorder.recordDatabaseNotFound(
                runId = any(),
                database = "missing_db",
                databaseStartTime = any(),
                error = any(),
            )
        }
        verify(exactly = 0) { candidateDeletionGate.deleteCandidates(any(), any(), any()) }
        verifyNoOtherAuditCalls(except = "recordDatabaseNotFound")
    }

    @Test
    fun `unexpected exception during processing routes to recordFailed`() {
        val service = serviceFor(
            applicationConfig(dryRun = true, deleteEnabled = false, databases = listOf("analytics")),
        )
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            discoveredDatabase(activeTableLocations = listOf("s3a://bucket/db/active_table"))
        every { objectStorageDiscoveryService.listImmediateChildFolders(any()) } throws
            IllegalStateException("simulated storage list failure")

        service.run()

        verify(exactly = 1) {
            cleanupAuditRecorder.recordFailed(
                runId = any(),
                database = "analytics",
                databaseStartTime = any(),
                excludedPaths = any(),
                error = any(),
            )
        }
        verify(exactly = 0) { candidateDeletionGate.deleteCandidates(any(), any(), any()) }
        verifyNoOtherAuditCalls(except = "recordFailed")
    }

    @Test
    fun `multiple databases isolate failures and produce one audit row per database with shared run id`() {
        val service = serviceFor(
            applicationConfig(
                dryRun = true,
                deleteEnabled = false,
                databases = listOf("good_db", "missing_db", "another_good_db"),
            ),
        )
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "good_db") } returns
            discoveredDatabase(
                database = "good_db",
                location = "s3a://bucket/good",
                activeTableLocations = listOf("s3a://bucket/good/active_table"),
            )
        every { objectStorageDiscoveryService.listImmediateChildFolders("s3a://bucket/good") } returns
            listOf(storageFolder("s3a://bucket/good/active_table"))
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "missing_db") } throws
            DatabaseNotFoundException(
                catalog = "spark_catalog",
                database = "missing_db",
                cause = RuntimeException("simulated catalog miss"),
            )
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "another_good_db") } returns
            discoveredDatabase(
                database = "another_good_db",
                location = "s3a://bucket/another_good",
                activeTableLocations = listOf("s3a://bucket/another_good/active_table"),
            )
        every { objectStorageDiscoveryService.listImmediateChildFolders("s3a://bucket/another_good") } returns
            listOf(storageFolder("s3a://bucket/another_good/active_table"))

        val capturedRunIds = mutableSetOf<String>()
        every {
            cleanupAuditRecorder.recordSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
        } answers {
            capturedRunIds.add(firstArg<String>())
            Unit
        }
        every {
            cleanupAuditRecorder.recordDatabaseNotFound(any(), any(), any(), any())
        } answers {
            capturedRunIds.add(firstArg<String>())
            Unit
        }

        service.run()

        verify(exactly = 2) {
            cleanupAuditRecorder.recordSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
        }
        verify(exactly = 1) {
            cleanupAuditRecorder.recordDatabaseNotFound(any(), any(), any(), any())
        }
        assertEquals(1, capturedRunIds.size, "All databases in one run share one runId")
    }

    private fun verifyNoOtherAuditCalls(except: String = "recordSuccess") {
        if (except != "recordNoActiveTables") {
            verify(exactly = 0) {
                cleanupAuditRecorder.recordNoActiveTables(any(), any(), any(), any(), any(), any())
            }
        }
        if (except != "recordDatabaseLocationMissing") {
            verify(exactly = 0) {
                cleanupAuditRecorder.recordDatabaseLocationMissing(any(), any(), any(), any(), any(), any(), any(), any(), any())
            }
        }
        if (except != "recordTooManyCandidateFolders") {
            verify(exactly = 0) {
                cleanupAuditRecorder.recordTooManyCandidateFolders(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
            }
        }
        if (except != "recordDatabaseNotFound") {
            verify(exactly = 0) {
                cleanupAuditRecorder.recordDatabaseNotFound(any(), any(), any(), any())
            }
        }
        if (except != "recordFailed") {
            verify(exactly = 0) {
                cleanupAuditRecorder.recordFailed(any(), any(), any(), any(), any())
            }
        }
        if (except != "recordSuccess") {
            verify(exactly = 0) {
                cleanupAuditRecorder.recordSuccess(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any())
            }
        }
    }

    private fun serviceFor(applicationConfig: ApplicationConfig): CleanupUntrackedTableFoldersService {
        val excludePathResolver = ExcludePathResolver().apply { config = applicationConfig }
        return CleanupUntrackedTableFoldersService().apply {
            config = applicationConfig
            catalogDiscoveryService = this@CleanupUntrackedTableFoldersServiceTest.catalogDiscoveryService
            objectStorageDiscoveryService = this@CleanupUntrackedTableFoldersServiceTest.objectStorageDiscoveryService
            untrackedFolderCandidateDetector = UntrackedFolderCandidateDetector()
            cleanupAuditTableService = this@CleanupUntrackedTableFoldersServiceTest.cleanupAuditTableService
            cleanupAuditRecorder = this@CleanupUntrackedTableFoldersServiceTest.cleanupAuditRecorder
            cleanupSummaryLogger = CleanupSummaryLogger()
            storageScanLocationResolver = StorageScanLocationResolver()
            this.excludePathResolver = excludePathResolver
            candidateSizeStatCollector = this@CleanupUntrackedTableFoldersServiceTest.candidateSizeStatCollector
            candidateDeletionGate = this@CleanupUntrackedTableFoldersServiceTest.candidateDeletionGate
        }
    }

    private fun applicationConfig(
        dryRun: Boolean,
        deleteEnabled: Boolean,
        databases: List<String>,
    ): ApplicationConfig =
        ApplicationConfig(
            catalog = "spark_catalog",
            databases = databases,
            dryRun = dryRun,
            deleteEnabled = deleteEnabled,
            // older_than_hours defaults to 24, but we want every storage folder eligible
            // regardless of mod time in these tests, so pretend everything is ancient.
            olderThanHours = 0,
        )

    private fun storageFolder(path: String): StorageFolder =
        StorageFolder(
            path = path,
            // mod time deep in the past so the detector never filters on age
            modificationTimeMillis = 0,
        )

    private fun discoveredDatabase(
        database: String = "analytics",
        location: String = "s3a://bucket/db",
        activeTableLocations: List<String>,
    ): DiscoveredDatabase =
        DiscoveredDatabase(
            catalog = "spark_catalog",
            database = database,
            location = location,
            tables = activeTableLocations.mapIndexed { index, tableLocation ->
                activeTable(tableLocation, name = "table_$index", database = database)
            },
        )

    private fun activeTable(
        location: String,
        name: String = "table_0",
        database: String = "analytics",
    ): DiscoveredTable =
        DiscoveredTable(
            catalog = "spark_catalog",
            database = database,
            table = name,
            isTemporary = false,
            location = location,
        )
}
