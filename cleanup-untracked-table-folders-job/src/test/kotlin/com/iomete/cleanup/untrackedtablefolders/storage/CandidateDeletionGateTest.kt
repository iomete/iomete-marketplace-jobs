package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class CandidateDeletionGateTest {

    private val catalogDiscoveryService = mockk<CatalogDiscoveryService>(relaxed = true)
    private val objectStorageDeletionService = mockk<ObjectStorageDeletionService>(relaxed = true)

    @Test
    fun `returns empty list and performs no IO when dry_run is true`() {
        val gate = gateFor(
            config = applicationConfig(dryRun = true, deleteEnabled = true),
        )

        val result = gate.deleteCandidates(
            catalog = "spark_catalog",
            database = "analytics",
            candidateFolders = listOf(
                storageFolder("s3a://bucket/db/orphan_a"),
                storageFolder("s3a://bucket/db/orphan_b"),
            ),
        )

        assertEquals(emptyList<String>(), result)
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        verify(exactly = 0) { objectStorageDeletionService.deleteFolderRecursively(any()) }
    }

    @Test
    fun `throws when delete is not dry_run but delete_enabled is false`() {
        val gate = gateFor(
            config = applicationConfig(dryRun = false, deleteEnabled = false),
        )

        val error = assertThrows(IllegalStateException::class.java) {
            gate.deleteCandidates(
                catalog = "spark_catalog",
                database = "analytics",
                candidateFolders = listOf(storageFolder("s3a://bucket/db/orphan_a")),
            )
        }

        assertEquals(
            "delete_enabled must be true before deleting candidate folders",
            error.message,
        )
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        verify(exactly = 0) { objectStorageDeletionService.deleteFolderRecursively(any()) }
    }

    @Test
    fun `returns empty list and performs no catalog or storage calls when candidate list is empty`() {
        val gate = gateFor(
            config = applicationConfig(dryRun = false, deleteEnabled = true),
        )

        val result = gate.deleteCandidates(
            catalog = "spark_catalog",
            database = "analytics",
            candidateFolders = emptyList(),
        )

        assertEquals(emptyList<String>(), result)
        verify(exactly = 0) { catalogDiscoveryService.discoverDatabase(any(), any()) }
        verify(exactly = 0) { objectStorageDeletionService.deleteFolderRecursively(any()) }
    }

    @Test
    fun `deletes every candidate when none are reclaimed by the catalog`() {
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            discoveredDatabase(activeTableLocations = listOf("s3a://bucket/db/active_table"))
        every { objectStorageDeletionService.deleteFolderRecursively(any()) } answers {
            DeletedStorageFolder(path = firstArg(), deleted = true)
        }

        val gate = gateFor(
            config = applicationConfig(dryRun = false, deleteEnabled = true),
        )

        val result = gate.deleteCandidates(
            catalog = "spark_catalog",
            database = "analytics",
            candidateFolders = listOf(
                storageFolder("s3a://bucket/db/orphan_b"),
                storageFolder("s3a://bucket/db/orphan_a"),
            ),
        )

        assertEquals(
            listOf("s3a://bucket/db/orphan_a", "s3a://bucket/db/orphan_b"),
            result,
        )
        verify(exactly = 1) { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/orphan_a") }
        verify(exactly = 1) { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/orphan_b") }
    }

    @Test
    fun `skips candidate when catalog now claims that exact path as an active table`() {
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            discoveredDatabase(
                activeTableLocations = listOf(
                    "s3a://bucket/db/active_table",
                    "s3a://bucket/db/now_active",
                ),
            )
        every { objectStorageDeletionService.deleteFolderRecursively(any()) } answers {
            DeletedStorageFolder(path = firstArg(), deleted = true)
        }

        val gate = gateFor(
            config = applicationConfig(dryRun = false, deleteEnabled = true),
        )

        val result = gate.deleteCandidates(
            catalog = "spark_catalog",
            database = "analytics",
            candidateFolders = listOf(
                storageFolder("s3a://bucket/db/now_active"),
                storageFolder("s3a://bucket/db/still_orphan"),
            ),
        )

        assertEquals(listOf("s3a://bucket/db/still_orphan"), result)
        verify(exactly = 0) { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/now_active") }
        verify(exactly = 1) { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/still_orphan") }
    }

    @Test
    fun `skips candidate when catalog now has an active table nested below the candidate`() {
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            discoveredDatabase(
                activeTableLocations = listOf("s3a://bucket/db/team_a/orders"),
            )
        every { objectStorageDeletionService.deleteFolderRecursively(any()) } answers {
            DeletedStorageFolder(path = firstArg(), deleted = true)
        }

        val gate = gateFor(
            config = applicationConfig(dryRun = false, deleteEnabled = true),
        )

        val result = gate.deleteCandidates(
            catalog = "spark_catalog",
            database = "analytics",
            candidateFolders = listOf(
                storageFolder("s3a://bucket/db/team_a"),
                storageFolder("s3a://bucket/db/abandoned"),
            ),
        )

        assertEquals(listOf("s3a://bucket/db/abandoned"), result)
        verify(exactly = 0) { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/team_a") }
        verify(exactly = 1) { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/abandoned") }
    }

    @Test
    fun `excludes folder from returned list when deletion service reports not deleted`() {
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            discoveredDatabase(activeTableLocations = emptyList())
        every { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/missing") } returns
            DeletedStorageFolder(path = "s3a://bucket/db/missing", deleted = false)
        every { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/present") } returns
            DeletedStorageFolder(path = "s3a://bucket/db/present", deleted = true)

        val gate = gateFor(
            config = applicationConfig(dryRun = false, deleteEnabled = true),
        )

        val result = gate.deleteCandidates(
            catalog = "spark_catalog",
            database = "analytics",
            candidateFolders = listOf(
                storageFolder("s3a://bucket/db/missing"),
                storageFolder("s3a://bucket/db/present"),
            ),
        )

        assertEquals(listOf("s3a://bucket/db/present"), result)
    }

    @Test
    fun `propagates deletion service exceptions without partial cleanup of remaining candidates`() {
        every { catalogDiscoveryService.discoverDatabase("spark_catalog", "analytics") } returns
            discoveredDatabase(activeTableLocations = emptyList())
        every { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/orphan_a") } returns
            DeletedStorageFolder(path = "s3a://bucket/db/orphan_a", deleted = true)
        every { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/orphan_b") } throws
            IllegalStateException("simulated FS error on delete")

        val gate = gateFor(
            config = applicationConfig(dryRun = false, deleteEnabled = true),
        )

        assertThrows(IllegalStateException::class.java) {
            gate.deleteCandidates(
                catalog = "spark_catalog",
                database = "analytics",
                candidateFolders = listOf(
                    storageFolder("s3a://bucket/db/orphan_a"),
                    storageFolder("s3a://bucket/db/orphan_b"),
                    storageFolder("s3a://bucket/db/orphan_c"),
                ),
            )
        }

        verify(exactly = 0) { objectStorageDeletionService.deleteFolderRecursively("s3a://bucket/db/orphan_c") }
    }

    private fun gateFor(config: ApplicationConfig): CandidateDeletionGate =
        CandidateDeletionGate().apply {
            this.config = config
            this.catalogDiscoveryService = this@CandidateDeletionGateTest.catalogDiscoveryService
            this.objectStorageDeletionService = this@CandidateDeletionGateTest.objectStorageDeletionService
        }

    private fun applicationConfig(
        dryRun: Boolean,
        deleteEnabled: Boolean,
    ): ApplicationConfig =
        ApplicationConfig(
            catalog = "spark_catalog",
            databases = listOf("analytics"),
            dryRun = dryRun,
            deleteEnabled = deleteEnabled,
        )

    private fun storageFolder(path: String): StorageFolder =
        StorageFolder(path = path, modificationTimeMillis = 0)

    private fun discoveredDatabase(activeTableLocations: List<String>): DiscoveredDatabase =
        DiscoveredDatabase(
            catalog = "spark_catalog",
            database = "analytics",
            location = "s3a://bucket/db",
            tables = activeTableLocations.mapIndexed { index, location ->
                DiscoveredTable(
                    catalog = "spark_catalog",
                    database = "analytics",
                    table = "table_$index",
                    isTemporary = false,
                    location = location,
                )
            },
        )
}
