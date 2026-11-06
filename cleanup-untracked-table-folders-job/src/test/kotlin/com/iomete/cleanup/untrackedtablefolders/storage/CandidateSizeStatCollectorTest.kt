package com.iomete.cleanup.untrackedtablefolders.storage

import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CandidateSizeStatCollectorTest {

    private val objectStorageDiscoveryService = mockk<ObjectStorageDiscoveryService>()

    private fun collectorFor(collectSizeStatistics: Boolean = true): CandidateSizeStatCollector =
        CandidateSizeStatCollector().also {
            it.config =
                ApplicationConfig(
                    catalog = "example_catalog",
                    databases = listOf("analytics"),
                    collectSizeStatistics = collectSizeStatistics,
                )
            it.objectStorageDiscoveryService = objectStorageDiscoveryService
        }

    @Test
    fun `collects the whole candidate list in one call to the discovery service`() {
        every { objectStorageDiscoveryService.collectSizeStatsPerFolder("example_catalog", listOf("s3a://b/a")) } returns
            SizeStatsBatch(mapOf("s3a://b/a" to StorageSizeStats(objectCount = 1, totalSizeBytes = 10)), emptyMap())

        val result = collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a"))

        assertEquals(StorageSizeStats(1, 10), result.statsByFolder["s3a://b/a"])
        verify(exactly = 1) { objectStorageDiscoveryService.collectSizeStatsPerFolder("example_catalog", listOf("s3a://b/a")) }
    }

    @Test
    fun `propagates a catalog storage configuration failure instead of recording unknown sizes`() {
        every { objectStorageDiscoveryService.collectSizeStatsPerFolder(any(), any()) } throws
            CatalogStorageConfigurationException("example_catalog", "catalog is not registered")

        assertThrows(CatalogStorageConfigurationException::class.java) {
            collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c"))
        }
    }

    @Test
    fun `fails when every candidate folder fails, because that is a storage access failure`() {
        every { objectStorageDiscoveryService.collectSizeStatsPerFolder(any(), any()) } returns
            SizeStatsBatch(
                emptyMap(),
                mapOf(
                    "s3a://b/a" to IllegalStateException("connection refused"),
                    "s3a://b/c" to IllegalStateException("connection refused"),
                ),
            )

        val error =
            assertThrows(IllegalStateException::class.java) {
                collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c"))
            }

        assertTrue(error.message!!.contains("all 2 candidate folder(s)"))
    }

    @Test
    fun `tolerates a single unreadable folder and records the rest`() {
        every { objectStorageDiscoveryService.collectSizeStatsPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c")) } returns
            SizeStatsBatch(
                mapOf("s3a://b/c" to StorageSizeStats(objectCount = 2, totalSizeBytes = 20)),
                mapOf("s3a://b/a" to IllegalStateException("one bad folder")),
            )

        val result = collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c"))

        assertEquals(setOf("s3a://b/c"), result.statsByFolder.keys)
        assertEquals(CandidateInspectionOutcome.STATISTICS_UNAVAILABLE, result.outcome("s3a://b/a"))
    }

    @Test
    fun `collects statistics even when collect_size_statistics is false`() {
        every { objectStorageDiscoveryService.collectSizeStatsPerFolder("example_catalog", listOf("s3a://b/a")) } returns
            SizeStatsBatch(mapOf("s3a://b/a" to StorageSizeStats(objectCount = 1, totalSizeBytes = 5)), emptyMap())

        val result = collectorFor(collectSizeStatistics = false).collectPerFolder("example_catalog", listOf("s3a://b/a"))

        assertEquals(setOf("s3a://b/a"), result.statsByFolder.keys)
        verify(exactly = 1) { objectStorageDiscoveryService.collectSizeStatsPerFolder("example_catalog", listOf("s3a://b/a")) }
    }

    @Test
    fun `does not fail when every candidate folder was confirmed absent`() {
        every { objectStorageDiscoveryService.collectSizeStatsPerFolder(any(), any()) } returns
            SizeStatsBatch(emptyMap(), emptyMap(), absentFolderPaths = setOf("s3a://b/a", "s3a://b/c"))

        val result = collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c"))

        assertEquals(setOf("s3a://b/a", "s3a://b/c"), result.absentFolderPaths)
        assertEquals(CandidateInspectionOutcome.FOLDER_NOT_FOUND, result.outcome("s3a://b/a"))
    }

    @Test
    fun `fails when every candidate folder that still exists fails, even if others were absent`() {
        every { objectStorageDiscoveryService.collectSizeStatsPerFolder(any(), any()) } returns
            SizeStatsBatch(
                emptyMap(),
                mapOf("s3a://b/c" to IllegalStateException("access denied")),
                absentFolderPaths = setOf("s3a://b/a"),
            )

        val error =
            assertThrows(IllegalStateException::class.java) {
                collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c"))
            }

        assertTrue(error.message!!.contains("all 1 candidate folder(s) that still exist"))
    }

    @Test
    fun `continues with mixed absent, failed, and readable folders`() {
        every { objectStorageDiscoveryService.collectSizeStatsPerFolder(any(), any()) } returns
            SizeStatsBatch(
                mapOf("s3a://b/d" to StorageSizeStats(objectCount = 1, totalSizeBytes = 3, newestObjectModificationTimeMillis = 1)),
                mapOf("s3a://b/c" to IllegalStateException("one bad folder")),
                absentFolderPaths = setOf("s3a://b/a"),
            )

        val result = collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c", "s3a://b/d"))

        assertEquals(
            mapOf(
                CandidateInspectionOutcome.FOLDER_NOT_FOUND to listOf("s3a://b/a"),
                CandidateInspectionOutcome.STATISTICS_UNAVAILABLE to listOf("s3a://b/c"),
            ),
            result.folderPathsByOutcome(listOf("s3a://b/a", "s3a://b/c", "s3a://b/d")),
        )
    }
}
