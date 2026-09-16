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
    fun `passes the catalog to the discovery service`() {
        every { objectStorageDiscoveryService.collectSizeStats("example_catalog", listOf("s3a://b/a")) } returns
            StorageSizeStats(objectCount = 1, totalSizeBytes = 10)

        val result = collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a"))

        assertEquals(StorageSizeStats(1, 10), result["s3a://b/a"])
        verify(exactly = 1) { objectStorageDiscoveryService.collectSizeStats("example_catalog", listOf("s3a://b/a")) }
    }

    @Test
    fun `propagates a catalog storage configuration failure instead of recording unknown sizes`() {
        every { objectStorageDiscoveryService.collectSizeStats(any(), any()) } throws
            CatalogStorageConfigurationException("example_catalog", "catalog is not registered")

        assertThrows(CatalogStorageConfigurationException::class.java) {
            collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c"))
        }
    }

    @Test
    fun `fails when every candidate folder fails, because that is a storage access failure`() {
        every { objectStorageDiscoveryService.collectSizeStats(any(), any()) } throws
            IllegalStateException("connection refused")

        val error =
            assertThrows(IllegalStateException::class.java) {
                collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c"))
            }

        assertTrue(error.message!!.contains("all 2 candidate folder(s)"))
    }

    @Test
    fun `tolerates a single unreadable folder and records the rest`() {
        every { objectStorageDiscoveryService.collectSizeStats("example_catalog", listOf("s3a://b/a")) } throws
            IllegalStateException("one bad folder")
        every { objectStorageDiscoveryService.collectSizeStats("example_catalog", listOf("s3a://b/c")) } returns
            StorageSizeStats(objectCount = 2, totalSizeBytes = 20)

        val result = collectorFor().collectPerFolder("example_catalog", listOf("s3a://b/a", "s3a://b/c"))

        assertEquals(setOf("s3a://b/c"), result.keys)
    }

    @Test
    fun `performs no storage access when size statistics are disabled`() {
        val result = collectorFor(collectSizeStatistics = false).collectPerFolder("example_catalog", listOf("s3a://b/a"))

        assertEquals(emptyMap<String, StorageSizeStats>(), result)
        verify(exactly = 0) { objectStorageDiscoveryService.collectSizeStats(any(), any()) }
    }
}
