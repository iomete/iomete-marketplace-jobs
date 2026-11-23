package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.capturingLogs
import com.iomete.cleanup.untrackedtablefolders.service.AuditIntegrityLostException
import com.iomete.cleanup.untrackedtablefolders.service.CleanupUntrackedTableFoldersService
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.Runs
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import org.apache.hadoop.fs.FileSystem
import org.jboss.logging.Logger
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.IOException

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val DATABASE_A = "$WAREHOUSE/a.db"

class AppTest {
    private val service: CleanupUntrackedTableFoldersService = mockk()

    @Test
    fun `a finished run exits with 0`() {
        every { service.run() } just Runs

        capturingLogs { logs ->
            assertEquals(0, app().run())
            assertTrue(logs.none { it.level == Logger.Level.ERROR })
        }
    }

    @Test
    fun `a failed run exits with 1 and logs a sanitized summary instead of the exception`() {
        every { service.run() } throws
            IllegalStateException(
                "Audit table write failed\u001B[2J: password=CANARY_APP_PASSWORD",
                IOException("Connection refused: token=CANARY_APP_TOKEN"),
            )

        capturingLogs { logs ->
            assertEquals(1, app().run())

            val error = logs.single { it.level == Logger.Level.ERROR }
            assertEquals(
                "Cleanup untracked table folders job failed (java.lang.IllegalStateException: Audit table write failed\\u001B[2J: " +
                    "password=***; caused by java.io.IOException: Connection refused: token=***)",
                error.message,
            )
            assertTrue(logs.all { it.thrown == null })
            assertTrue(logs.none { "CANARY" in it.message || '\u001B' in it.message })
            assertFalse(logs.any { it.message == "Cleanup untracked table folders job finished" })
        }
    }

    @Test
    fun `an Error exits with 1 as well`() {
        every { service.run() } throws StackOverflowError("deep recursion")

        capturingLogs { logs ->
            assertEquals(1, app().run())
            assertEquals(
                "Cleanup untracked table folders job failed (java.lang.StackOverflowError: deep recursion)",
                logs.single { it.level == Logger.Level.ERROR }.message,
            )
        }
    }

    // FAILED rows are the per-database result; only a failure of the job itself changes the exit code.
    @Test
    fun `a real run whose every database row is FAILED still exits with 0`() {
        val flow = flowWithDatabaseA(listOf("a", "b"))
        InMemoryS3FileSystem.failDeleteFor = "$DATABASE_A/orphan/part-0.parquet"
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "b") } throws IllegalStateException("catalog unavailable")

        capturingLogs { logs ->
            assertEquals(0, App().also { it.cleanupService = flow.service }.run())

            assertEquals(
                listOf(Triple("a", "FAILED", "deletion_failed"), Triple("b", "FAILED", "unexpected_error")),
                flow.auditRecords.map { Triple(it.databaseName, it.status, it.statusReason) },
            )
            assertTrue(flow.objectExists("$DATABASE_A/orphan/part-0.parquet"))
            assertTrue(logs.any { it.message == "Cleanup untracked table folders job finished" })
            assertFalse(logs.any { it.message.startsWith("Cleanup untracked table folders job failed") })
        }
    }

    @Test
    fun `a real run whose audit write fails exits with 1`() {
        val flow = flowWithDatabaseA(listOf("a"))
        every { flow.service.cleanupAuditTableService.writeAuditRecord(any()) } throws IllegalStateException("audit table unavailable")

        capturingLogs { logs ->
            assertEquals(1, App().also { it.cleanupService = flow.service }.run())

            assertTrue(
                logs.single { it.level == Logger.Level.ERROR && it.message.startsWith("Cleanup untracked table folders job failed") }
                    .message.contains(AuditIntegrityLostException::class.java.name),
            )
            assertFalse(logs.any { it.message == "Cleanup untracked table folders job finished" })
        }
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    private fun flowWithDatabaseA(databases: List<String>): CleanupFlowHarness {
        InMemoryS3FileSystem.reset()
        putFolder(DATABASE_A)
        putFolder("$DATABASE_A/orders", "part-0.parquet")
        putFolder("$DATABASE_A/orphan", "part-0.parquet")
        val flow = CleanupFlowHarness(ApplicationConfig(catalog = CATALOG, databases = databases, dryRun = false, deleteEnabled = true))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "a") } returns
            DiscoveredDatabase(
                CATALOG,
                "a",
                DATABASE_A,
                listOf(DiscoveredTable(CATALOG, "a", "orders", isTemporary = false, location = "$DATABASE_A/orders")),
            )
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        return flow
    }

    private fun app(): App = App().also { it.cleanupService = service }
}
