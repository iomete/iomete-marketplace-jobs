package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditTableService
import com.iomete.cleanup.untrackedtablefolders.catalog.DatabaseNotFoundException
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.CapturedLog
import com.iomete.cleanup.untrackedtablefolders.logging.CleanupSummaryLogger
import com.iomete.cleanup.untrackedtablefolders.logging.SafeText
import com.iomete.cleanup.untrackedtablefolders.logging.capturingLogs
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogStorageProperties
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogStorageTestHarness
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.jboss.logging.Logger
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.IOException

private const val CATALOG = "canary_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val SALES = "$WAREHOUSE/sales.db"
private const val ARCHIVE = "$WAREHOUSE/archive.db"
private const val CROWDED = "$WAREHOUSE/crowded.db"

// Every canary holds this marker, so one search finds any of them in any output.
private const val CANARY = "CANARY"
private const val CATALOG_ACCESS_KEY = "CANARY_CATALOG_ACCESS_KEY"
private const val CATALOG_SECRET = "CANARY_CATALOG_SECRET_7f3a" // gitleaks:allow
private const val HADOOP_SECRET = "CANARY_HADOOP_SECRET_51c2" // gitleaks:allow

// One folder name with every class of character SafeText escapes. After "\r\n" comes a line shaped like a log line.
private const val FORGED_LINE_START = "26-11-20 10:00:00 WARN"
private const val CONTROL_NAME =
    "orphan\u001B[2J\r\n$FORGED_LINE_START App: done\u202Egpj.exe\u0000a\u0085b\u2028c\u2066d\u007Fe\u009Bf\u200Fg\tend" +
        "\u200Bh\uFEFFi\uDB40\uDC41j\u00ADk"

class OutputCanaryFlowTest {

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
    fun `no canary secret and no raw control character reaches a log, audit row, audit SQL or report`() {
        putFolder(SALES)
        putFolder("$SALES/t1", "part-0.parquet")
        putFolder("$SALES/$CONTROL_NAME", "part-0.parquet")
        putFolder(ARCHIVE)
        putFolder("$ARCHIVE/t1", "part-0.parquet")
        putFolder("$ARCHIVE/stale", "part-0.parquet")
        putFolder(CROWDED)
        putFolder("$CROWDED/t1", "part-0.parquet")
        putFolder("$CROWDED/a\u001Bx", "part-0.parquet")
        putFolder("$CROWDED/b", "part-0.parquet")
        InMemoryS3FileSystem.beforeDelete = { target ->
            if (target == "$ARCHIVE/stale/part-0.parquet") throw IOException("delete failed: X-Amz-Signature=CANARY_DELETE_SIGNATURE")
        }

        capturingLogs { logs ->
            val flow = canaryFlow()

            flow.run()

            val records = flow.auditRecords.associateBy { it.databaseName }
            assertStatuses(records)
            assertUsefulDetailSurvives(flow, records, logs)

            val auditSql = flow.auditRecords.map { CleanupAuditTableService().buildInsertAuditRecordSql(it) }
            val reports = flow.summaries.flatMap { CleanupSummaryLogger().buildReportLines(it) }
            val outputs =
                logs.map { "log ${it.level}" to it.message } +
                    flow.auditRecords.map { "audit row ${it.databaseName}" to it.toString() } +
                    auditSql.map { "audit SQL" to it } +
                    reports.map { "report" to it }

            assertTrue(logs.any { it.level == Logger.Level.DEBUG }, "DEBUG output was captured")
            assertTrue(logs.all { it.thrown == null }, "a Throwable reached the logging backend")
            outputs.forEach { (sink, text) ->
                assertFalse(CANARY in text, "$sink holds a canary")
                assertEquals("", unsafeCodePoints(text), "$sink holds a raw control character")
                assertTrue(text.lines().none { it.trimStart('-', ' ').startsWith(FORGED_LINE_START) }, "$sink starts a forged line")
            }
        }
    }

    private fun assertStatuses(records: Map<String, CleanupAuditRecord>) {
        // The first database fails, and every database after it is still processed, including a deletion.
        assertEquals(
            mapOf(
                "broken" to ("FAILED" to "unexpected_error"),
                "gone" to ("SKIPPED" to "database_not_found"),
                "sales" to ("SUCCESS" to null),
                "archive" to ("FAILED" to "deletion_failed"),
                "crowded" to ("SKIPPED" to "too_many_candidate_folders"),
                "pending" to ("BLOCKED" to "unresolved_catalog_ownership"),
                "refused" to ("SKIPPED" to "database_location_is_bucket_root"),
            ),
            records.mapValues { (_, record) -> record.status to record.statusReason },
        )
    }

    private fun assertUsefulDetailSurvives(
        flow: CleanupFlowHarness,
        records: Map<String, CleanupAuditRecord>,
        logs: List<CapturedLog>,
    ) {
        // The canary credentials are the ones the job used, so a leak would be of real configuration.
        assertTrue(InMemoryS3FileSystem.opened.any { it.accessKey == CATALOG_ACCESS_KEY })

        val escapedOrphan = SafeText.path("$SALES/$CONTROL_NAME")
        assertFalse(flow.objectExists("$SALES/$CONTROL_NAME/part-0.parquet"))
        assertEquals(listOf(escapedOrphan), records.getValue("sales").deletedFolders)
        assertTrue(logs.any { it.level == Logger.Level.WARN && it.message == "Deleted untracked table folder: path=${SafeText.text("$SALES/$CONTROL_NAME")}" })

        val broken = records.getValue("broken").errorMessage.orEmpty()
        assertTrue(broken.startsWith("Catalog request failed: GET https://rest:8181/v1/config?*** Authorization: *** ***"), broken)
        assertTrue(broken.endsWith("; caused by java.io.IOException; caused by java.lang.RuntimeException"), broken)
        assertFalse(broken.contains("Connection reset"), "cause messages stay out of the audit row")
        val brokenError = logs.single { it.level == Logger.Level.ERROR && it.message.startsWith("Cleanup failed for catalog=$CATALOG, database=broken") }
        assertTrue(brokenError.message.contains("; caused by java.io.IOException: Connection reset; fs.s3a.secret.key=***"), brokenError.message)
        val brokenTrace = logs.single { it.level == Logger.Level.DEBUG && it.message.startsWith("Cleanup failed for catalog=$CATALOG, database=broken\n") }
        assertTrue(brokenTrace.message.contains("\nCaused by: java.io.IOException: Connection reset"))
        assertTrue(brokenTrace.message.contains("\n    at "))

        assertEquals(
            "Folder deletion stopped after 0 confirmed deletion(s); deleting an object of folder $ARCHIVE/stale failed: " +
                "0 of 1 listed object(s) deleted, 0 kept, 1 in an unknown state (a delete may still complete), 0 already gone, 0 unlisted object(s) found; " +
                "caused by com.iomete.cleanup.untrackedtablefolders.storage.CatalogStorageOperationException; caused by java.io.IOException",
            records.getValue("archive").errorMessage,
        )
        assertTrue(flow.objectExists("$ARCHIVE/stale/part-0.parquet"))

        assertEquals(
            "java.lang.IllegalStateException: conf unavailable: spark.hadoop.fs.s3a.secret.key=***; caused by java.io.IOException",
            records.getValue("sales").diagnosticDetails["scan_plan_error"],
        )
        assertEquals(
            listOf("$CROWDED/a\\u001Bx", "$CROWDED/b"),
            records.getValue("crowded").candidateFolders,
        )
        assertTrue(logs.any { it.level == Logger.Level.INFO && it.message.contains("\n    reason: Table properties could not be read: password=***\n") })
        assertTrue(logs.any { it.level == Logger.Level.INFO && it.message.contains("\n  Error: java.lang.IllegalStateException: conf unavailable: spark.hadoop.fs.s3a.secret.key=***; caused by java.io.IOException\n") })
    }

    private fun canaryFlow(): CleanupFlowHarness {
        val catalogProperties =
            CatalogStorageTestHarness.s3CompatibleCatalogProperties() +
                mapOf(
                    CatalogStorageProperties.ACCESS_KEY_ID to CATALOG_ACCESS_KEY,
                    CatalogStorageProperties.SECRET_ACCESS_KEY to CATALOG_SECRET,
                )
        val hadoopConfiguration =
            CatalogStorageTestHarness.baseHadoopConfiguration().apply {
                set(CatalogStorageProperties.FS_ACCESS_KEY, "CANARY_HADOOP_ACCESS_KEY")
                set(CatalogStorageProperties.FS_SECRET_KEY, HADOOP_SECRET)
                set(CatalogStorageProperties.FS_SESSION_TOKEN, "CANARY_HADOOP_SESSION_TOKEN")
            }
        val flow =
            CleanupFlowHarness(
                ApplicationConfig(
                    catalog = CATALOG,
                    databases = listOf("broken", "gone", "sales", "archive", "crowded", "pending", "refused"),
                    dryRun = false,
                    deleteEnabled = true,
                    maxCandidateFoldersPerDatabase = 1,
                ),
                catalogProperties = catalogProperties,
                sparkHadoopConfiguration = hadoopConfiguration,
            )
        val discovery = flow.catalogDiscoveryService

        every { discovery.discoverDatabase(CATALOG, "broken") } throws
            IllegalStateException(
                "Catalog request failed: GET https://rest:8181/v1/config?token=CANARY_URL_TOKEN Authorization: Bearer CANARY_BEARER",
                IOException(
                    "Connection reset; fs.s3a.secret.key=$HADOOP_SECRET; s3.secret-access-key=$CATALOG_SECRET\u001B[31m " +
                        "s3a://AKIAIOSFODNN7EXAMPLE:CANARY_USERINFO@bucket/x",
                    RuntimeException("{\"sessionToken\":\"CANARY_JSON_TOKEN\"}"),
                ),
            )
        every { discovery.discoverDatabase(CATALOG, "gone") } throws
            DatabaseNotFoundException(CATALOG, "gone", RuntimeException("[SCHEMA_NOT_FOUND] password=CANARY_NOT_FOUND\r\nnext"))
        every { discovery.discoverDatabase(CATALOG, "sales") } returns database("sales", SALES)
        every { discovery.discoverDatabase(CATALOG, "archive") } returns database("archive", ARCHIVE)
        every { discovery.discoverDatabase(CATALOG, "crowded") } returns database("crowded", CROWDED)
        every { discovery.discoverDatabase(CATALOG, "pending") } returns
            DiscoveredDatabase(
                CATALOG,
                "pending",
                "$WAREHOUSE/pending.db",
                listOf(
                    DiscoveredTable(
                        CATALOG,
                        "pending",
                        "t1",
                        isTemporary = false,
                        location = null,
                        unresolvedReason = "Table properties could not be read: password=CANARY_REASON\n$FORGED_LINE_START forged",
                    ),
                ),
            )
        every { discovery.discoverDatabase(CATALOG, "refused") } returns
            DiscoveredDatabase(CATALOG, "refused", "s3a://bucket", listOf(DiscoveredTable(CATALOG, "refused", "t1", isTemporary = false, location = "s3a://bucket/t1")))
        every { discovery.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        every { discovery.visibleDatabaseLocations(CATALOG) } returns mapOf("sales" to SALES, "archive" to ARCHIVE, "crowded" to CROWDED)
        every { discovery.knownWarehouseRoots() } throws
            IllegalStateException("conf unavailable: spark.hadoop.fs.s3a.secret.key=CANARY_PLAN_SECRET", IOException("Connection refused: secret=CANARY_PLAN_CAUSE"))
        return flow
    }

    private fun database(
        name: String,
        location: String,
    ): DiscoveredDatabase = DiscoveredDatabase(CATALOG, name, location, listOf(DiscoveredTable(CATALOG, name, "t1", isTemporary = false, location = "$location/t1")))

    // The only raw control character allowed is the newline that separates the lines of a report, a trace or a
    // multi-line audit value. A newline inside a name is caught by the forged-line check.
    private fun unsafeCodePoints(text: String): String =
        text.codePoints().toArray().filter { it != '\n'.code && isUnsafe(it) }.joinToString { "U+%04X".format(it) }

    private fun isUnsafe(codePoint: Int): Boolean =
        when (Character.getType(codePoint).toByte()) {
            Character.CONTROL, Character.FORMAT, Character.LINE_SEPARATOR, Character.PARAGRAPH_SEPARATOR, Character.SURROGATE -> true
            else -> codePoint in 0xE0000..0xE007F
        }
}
