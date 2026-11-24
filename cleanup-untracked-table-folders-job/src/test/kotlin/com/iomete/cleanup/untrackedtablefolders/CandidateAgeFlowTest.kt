package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.catalog.NamedTableStorage
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.logging.CapturedLog
import com.iomete.cleanup.untrackedtablefolders.logging.capturingLogs
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.jboss.logging.Logger
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.io.FileNotFoundException
import java.time.Duration
import java.time.Instant

private const val CATALOG = "example_catalog"
private const val ENDPOINT = CleanupFlowHarness.ENDPOINT
private const val DATABASE_LOCATION = "s3a://bucket/lake/sales.db"
private const val WAREHOUSE = "s3a://bucket/lake"

class CandidateAgeFlowTest {

    private val now = System.currentTimeMillis()
    private val tenDaysAgo = now - Duration.ofDays(10).toMillis()

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        InMemoryS3FileSystem.putDirectory(ENDPOINT, DATABASE_LOCATION)
        folder("orders", tenDaysAgo)
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `folder with a recently written object survives`() {
        folder("orphan_old", tenDaysAgo)
        folder("orphan_fresh", now - 5_000)
        val flow = flow(deleteEnabled = true)

        flow.run()

        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_fresh/part-0.parquet"))
        assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan_old/part-0.parquet"))
    }

    @Test
    fun `empty folder is never a candidate`() {
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$DATABASE_LOCATION/empty")
        val flow = flow(deleteEnabled = false)

        flow.run()

        assertFalse("$DATABASE_LOCATION/empty" in flow.auditRecords.single().candidateFolders)
    }

    @Test
    fun `folder whose listing fails survives`() {
        folder("orphan_old", tenDaysAgo)
        folder("orphan_unreadable", tenDaysAgo)
        InMemoryS3FileSystem.failListFor = "$DATABASE_LOCATION/orphan_unreadable"
        val flow = flow(deleteEnabled = true)

        flow.run()

        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_unreadable/part-0.parquet"))
        assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan_old/part-0.parquet"))
    }

    @Test
    fun `object with an invalid modification time keeps its folder out of the candidates`() {
        folder("orphan_invalid", 0)
        val flow = flow(deleteEnabled = false)

        flow.run()

        assertFalse("$DATABASE_LOCATION/orphan_invalid" in flow.auditRecords.single().candidateFolders)
    }

    @Test
    fun `an invalid modification time is not hidden by a valid older one`() {
        folder("orphan_mixed", tenDaysAgo)
        InMemoryS3FileSystem.putFile(ENDPOINT, "$DATABASE_LOCATION/orphan_mixed/part-1.parquet", length = 1, modificationTimeMillis = 0)
        val flow = flow(deleteEnabled = true)

        flow.run()

        assertFalse("$DATABASE_LOCATION/orphan_mixed" in flow.auditRecords.single().candidateFolders)
        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_mixed/part-0.parquet"))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_mixed/part-1.parquet"))
    }

    @Test
    fun `collect_size_statistics false still collects statistics and checks age`() {
        folder("orphan_old", tenDaysAgo)
        folder("orphan_fresh", now - 5_000)
        val flow = flow(deleteEnabled = true, collectSizeStatistics = false)

        flow.run()

        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_fresh/part-0.parquet"))
        assertNotNull(flow.auditRecords.single().candidateObjectCount)
    }

    @Test
    fun `partial scan coverage is reported on success`() {
        folder("orphan_old", tenDaysAgo)
        val flow = flow(deleteEnabled = false, tableLocations = listOf("$DATABASE_LOCATION/orders", "s3a://bucket/other/events"))

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals("partial_scan_coverage", record.statusReason)
    }

    @Test
    fun `blocked database keeps its reason and reports partial coverage`() {
        folder("orphan_old", tenDaysAgo)
        val flow =
            flow(
                deleteEnabled = true,
                tableLocations = listOf("$DATABASE_LOCATION/orders", "s3a://bucket/other/events"),
                unresolvedTable = "broken",
            )

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals("1", record.diagnosticDetails["unscanned_table_location_count"])
    }

    @Test
    fun `the inspection limit is enforced before any recursive traversal`() {
        val orphans = (0..100).map { "orphan_%03d".format(it) }.onEach { folder(it, tenDaysAgo) }
        val flow = flow(deleteEnabled = false, maxCandidateFolders = 1)

        flow.run()

        assertEquals("too_many_folders_to_inspect", flow.auditRecords.single().statusReason)
        orphans.forEach { assertNull(InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/$it"], it) }
    }

    @Test
    fun `a cap above 100 raises the inspection limit to the cap`() {
        val orphans = (0..100).map { "orphan_%03d".format(it) }.onEach { folder(it, tenDaysAgo) }
        val flow = flow(deleteEnabled = false, maxCandidateFolders = 101)

        flow.run()

        assertEquals("SUCCESS", flow.auditRecords.single().status)
        orphans.forEach { assertEquals(1, InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/$it"], it) }
    }

    @Test
    fun `at the inspection limit every folder is inspected`() {
        val orphans = (0..99).map { "orphan_%03d".format(it) }.onEach { folder(it, tenDaysAgo) }
        val flow = flow(deleteEnabled = false, maxCandidateFolders = 100)

        flow.run()

        assertEquals("SUCCESS", flow.auditRecords.single().status)
        orphans.forEach { assertEquals(1, InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/$it"], it) }
    }

    @Test
    fun `each candidate is traversed once for both statistics and age`() {
        folder("orphan_a", tenDaysAgo)
        folder("orphan_b", tenDaysAgo)
        val flow = flow(deleteEnabled = false)

        flow.run()

        assertEquals(1, InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/orphan_a"])
        assertEquals(1, InMemoryS3FileSystem.listStatusCalls["$DATABASE_LOCATION/orphan_b"])
    }

    @Test
    fun `deletion mode keeps a 48-hour-old orphan even with older_than_hours=0`() {
        folder("orphan_48h", hoursAgo(48.0))
        folder("orphan_73h", hoursAgo(73.0))

        capturingLogs { logs ->
            val flow = flow(deleteEnabled = true, olderThanHours = 0)
            val before = Instant.now()
            flow.run()
            val after = Instant.now()

            assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_48h/part-0.parquet"))
            assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan_73h/part-0.parquet"))
            val record = flow.auditRecords.single()
            assertEquals("SUCCESS", record.status)
            assertEquals(listOf("$DATABASE_LOCATION/orphan_73h"), record.candidateFolders)
            assertEquals(listOf("$DATABASE_LOCATION/orphan_73h"), record.deletedFolders)
            assertEquals(72, record.olderThanHours)
            assertEquals("0", record.diagnosticDetails["configured_older_than_hours"])
            assertEquals("72", record.diagnosticDetails["deletion_min_age_hours"])
            assertCutoff(record, before, after, hours = 72)
            assertTrue(record.diagnosticDetails.keys.none { it.startsWith("would_be_held_by_deletion_floor") })
            assertEquals(
                listOf(
                    "older_than_hours=0 is below the 72-hour minimum for deletion mode, so this run uses 72 hours. " +
                        "configured_older_than_hours=0, effective_older_than_hours=72.",
                ),
                floorWarnings(logs),
            )
            assertTrue(
                logs.any {
                    it.level == Logger.Level.INFO &&
                        it.message.startsWith("Applying older_than_hours=72 (configured 0, deletion-mode minimum 72);")
                },
            )
        }
    }

    @ParameterizedTest(name = "older_than_hours={0} in deletion mode runs at {1} hours")
    @CsvSource("0, 72", "24, 72", "71, 72", "72, 72", "100, 100")
    fun `deletion mode deletes only folders at least as old as the effective age`(
        configured: Long,
        effective: Long,
    ) {
        val ages = listOf(48.0, 71.5, 73.0, 80.0, 101.0)
        ages.forEach { folder(ageFolderName(it), hoursAgo(it)) }

        capturingLogs { logs ->
            val flow = flow(deleteEnabled = true, olderThanHours = configured)
            val before = Instant.now()
            flow.run()
            val after = Instant.now()

            val (deletedAges, keptAges) = ages.partition { it >= effective }
            keptAges.forEach { assertTrue(flow.objectExists("$DATABASE_LOCATION/${ageFolderName(it)}/part-0.parquet"), "$it h") }
            deletedAges.forEach { assertFalse(flow.objectExists("$DATABASE_LOCATION/${ageFolderName(it)}/part-0.parquet"), "$it h") }
            val record = flow.auditRecords.single()
            assertEquals(deletedAges.map { "$DATABASE_LOCATION/${ageFolderName(it)}" }, record.deletedFolders)
            assertEquals(effective, record.olderThanHours)
            assertEquals(configured.toString(), record.diagnosticDetails["configured_older_than_hours"])
            assertCutoff(record, before, after, hours = effective)
            assertEquals(if (configured < 72) 1 else 0, floorWarnings(logs).size)
        }
    }

    @Test
    fun `deletion mode with older_than_hours unset runs at 72 hours`() {
        folder("orphan_30h", hoursAgo(30.0))
        folder("orphan_73h", hoursAgo(73.0))
        val flow = flow(deleteEnabled = true)

        flow.run()

        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_30h/part-0.parquet"))
        assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan_73h/part-0.parquet"))
        val record = flow.auditRecords.single()
        assertEquals(72, record.olderThanHours)
        assertEquals("24", record.diagnosticDetails["configured_older_than_hours"])
    }

    @Test
    fun `folders of unknown age stay held in deletion mode at the minimum age`() {
        folder("orphan_invalid", 0)
        folder("orphan_mixed", hoursAgo(100.0))
        InMemoryS3FileSystem.putFile(ENDPOINT, "$DATABASE_LOCATION/orphan_mixed/part-1.parquet", length = 1, modificationTimeMillis = 0)
        folder("orphan_73h", hoursAgo(73.0))
        val flow = flow(deleteEnabled = true, olderThanHours = 0)

        flow.run()

        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_invalid/part-0.parquet"))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_mixed/part-0.parquet"))
        assertTrue(flow.objectExists("$DATABASE_LOCATION/orphan_mixed/part-1.parquet"))
        assertFalse(flow.objectExists("$DATABASE_LOCATION/orphan_73h/part-0.parquet"))
        val record = flow.auditRecords.single()
        assertEquals(listOf("$DATABASE_LOCATION/orphan_73h"), record.deletedFolders)
        assertEquals("2", record.diagnosticDetails["unknown_age_count"])
    }

    @Test
    fun `a dry run with older_than_hours=0 still lists a 48-hour-old folder`() {
        folder("orphan_48h", hoursAgo(48.0))
        folder("orphan_73h", hoursAgo(73.0))
        folder("orphan_fresh", now - 5_000)

        capturingLogs { logs ->
            val flow = flow(deleteEnabled = false, olderThanHours = 0)
            val before = Instant.now()
            flow.run()
            val after = Instant.now()

            val record = flow.auditRecords.single()
            assertEquals("SUCCESS", record.status)
            assertEquals(
                listOf("$DATABASE_LOCATION/orphan_48h", "$DATABASE_LOCATION/orphan_73h", "$DATABASE_LOCATION/orphan_fresh"),
                record.candidateFolders,
            )
            assertEquals(emptyList<String>(), record.deletedFolders)
            listOf("orphan_48h", "orphan_73h", "orphan_fresh").forEach {
                assertTrue(flow.objectExists("$DATABASE_LOCATION/$it/part-0.parquet"), it)
            }
            assertEquals(0, record.olderThanHours)
            assertEquals("0", record.diagnosticDetails["configured_older_than_hours"])
            assertCutoff(record, before, after, hours = 0)
            assertEquals("2", record.diagnosticDetails["would_be_held_by_deletion_floor_count"])
            assertEquals(
                "$DATABASE_LOCATION/orphan_48h\n$DATABASE_LOCATION/orphan_fresh",
                record.diagnosticDetails["would_be_held_by_deletion_floor_paths_sample"],
            )
            assertEquals("false", record.diagnosticDetails["would_be_held_by_deletion_floor_paths_truncated"])
            assertEquals(emptyList<String>(), floorWarnings(logs))
            assertEquals(
                1,
                logs.count { it.level == Logger.Level.INFO && it.message.startsWith("Dry run uses older_than_hours=0 as configured.") },
            )
            assertTrue(flow.summaryLines().any { it.startsWith("  would_be_held_by_deletion_floor: 2 of these folders") })
        }
    }

    @Test
    fun `a dry run at the default age counts only the folders between 24 and 72 hours old`() {
        folder("orphan_10h", hoursAgo(10.0))
        folder("orphan_30h", hoursAgo(30.0))
        folder("orphan_71h", hoursAgo(71.5))
        folder("orphan_73h", hoursAgo(73.0))
        val flow = flow(deleteEnabled = false)

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(
            listOf("$DATABASE_LOCATION/orphan_30h", "$DATABASE_LOCATION/orphan_71h", "$DATABASE_LOCATION/orphan_73h"),
            record.candidateFolders,
        )
        assertEquals(24, record.olderThanHours)
        assertEquals("2", record.diagnosticDetails["would_be_held_by_deletion_floor_count"])
        assertEquals(
            "$DATABASE_LOCATION/orphan_30h\n$DATABASE_LOCATION/orphan_71h",
            record.diagnosticDetails["would_be_held_by_deletion_floor_paths_sample"],
        )
    }

    @Test
    fun `a dry run at 72 hours or more reports no floor holds`() {
        folder("orphan_30h", hoursAgo(30.0))
        folder("orphan_73h", hoursAgo(73.0))

        capturingLogs { logs ->
            val flow = flow(deleteEnabled = false, olderThanHours = 72)
            flow.run()

            val record = flow.auditRecords.single()
            assertEquals(listOf("$DATABASE_LOCATION/orphan_73h"), record.candidateFolders)
            assertTrue(record.diagnosticDetails.keys.none { it.startsWith("would_be_held_by_deletion_floor") })
            assertTrue(logs.none { it.message.startsWith("Dry run uses older_than_hours=") })
            assertEquals(emptyList<String>(), floorWarnings(logs))
        }
    }

    @Test
    fun `a blocked dry run also counts the folders deletion mode would hold`() {
        folder("orphan_48h", hoursAgo(48.0))
        folder("orphan_73h", hoursAgo(73.0))
        val flow = flow(deleteEnabled = false, olderThanHours = 0, unresolvedTable = "broken")

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("BLOCKED", record.status)
        assertEquals("unresolved_catalog_ownership", record.statusReason)
        assertEquals("1", record.diagnosticDetails["would_be_held_by_deletion_floor_count"])
        assertEquals("$DATABASE_LOCATION/orphan_48h", record.diagnosticDetails["would_be_held_by_deletion_floor_paths_sample"])
    }

    // The cap counts only the folders the run would delete, so folders the age check holds never count.
    @ParameterizedTest(name = "deletion={0}: {2} old and {3} folders {1} h old with cap {4}")
    @CsvSource("true, 48.0, 3, 9, 3", "true, 48.0, 1, 1, 1", "false, 1.0, 3, 9, 3")
    fun `folders the age check holds do not count toward the candidate cap`(
        deleteEnabled: Boolean,
        youngHours: Double,
        oldCount: Int,
        youngCount: Int,
        cap: Int,
    ) {
        val old = (1..oldCount).map { "$DATABASE_LOCATION/orphan_old_$it" }.onEach { folder(it.substringAfterLast('/'), hoursAgo(100.0)) }
        val young = (1..youngCount).map { "$DATABASE_LOCATION/orphan_young_$it" }.onEach { folder(it.substringAfterLast('/'), hoursAgo(youngHours)) }

        val allowed = flow(deleteEnabled = deleteEnabled, maxCandidateFolders = cap)
        allowed.run()

        val record = allowed.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(old, record.candidateFolders)
        assertEquals(if (deleteEnabled) old else emptyList(), record.deletedFolders)
        assertEquals(youngCount.toString(), record.diagnosticDetails["too_new_count"])
        assertEquals("too_new=$youngCount", record.diagnosticDetails["held_folder_counts"])
        young.forEach { assertTrue(allowed.objectExists("$it/part-0.parquet"), it) }

        // One fewer than the folders due: the database is skipped after inspection, and nothing is deleted.
        setUp()
        old.forEach { folder(it.substringAfterLast('/'), hoursAgo(100.0)) }
        young.forEach { folder(it.substringAfterLast('/'), hoursAgo(youngHours)) }
        val skipped = flow(deleteEnabled = deleteEnabled, maxCandidateFolders = cap - 1)
        skipped.run()

        val skip = skipped.auditRecords.single()
        assertEquals("SKIPPED" to "too_many_candidate_folders", skip.status to skip.statusReason)
        assertEquals(old, skip.candidateFolders)
        assertEquals(emptyList<String>(), skip.deletedFolders)
        assertEquals(oldCount.toString(), skip.diagnosticDetails["due_for_deletion_count"])
        assertEquals("too_new=$youngCount", skip.diagnosticDetails["held_folder_counts"])
        assertEquals("$oldCount folder(s) are due for deletion, which exceeds max_candidate_folders_per_database=${cap - 1}", skip.errorMessage)
        (old + young).forEach { path ->
            assertTrue(skipped.objectExists("$path/part-0.parquet"), path)
            assertEquals(1, InMemoryS3FileSystem.listStatusCalls[path], path)
        }
        assertTrue(skipped.intentRecords.isEmpty())
    }

    @Test
    fun `the audit row and the report count every held folder by reason`() {
        folder("orphan_old", hoursAgo(100.0))
        folder("orphan_48h", hoursAgo(48.0))
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$DATABASE_LOCATION/empty")
        folder("orphan_invalid", 0)
        folder("orphan_unreadable", hoursAgo(100.0))
        InMemoryS3FileSystem.failListFor = "$DATABASE_LOCATION/orphan_unreadable"
        folder("orphan_named", hoursAgo(100.0))

        capturingLogs { logs ->
            val flow = flow(deleteEnabled = true)
            every { flow.catalogDiscoveryService.tableStorageIfExists(CATALOG, "sales", "orphan_named") } returns
                NamedTableStorage.Found("$DATABASE_LOCATION/orphan_named")
            flow.run()

            val record = flow.auditRecords.single()
            assertEquals("SUCCESS", record.status)
            assertEquals(listOf("$DATABASE_LOCATION/orphan_named", "$DATABASE_LOCATION/orphan_old"), record.candidateFolders)
            assertEquals(listOf("$DATABASE_LOCATION/orphan_old"), record.deletedFolders)
            assertEquals("1", record.diagnosticDetails["too_new_count"])
            assertEquals("$DATABASE_LOCATION/orphan_48h", record.diagnosticDetails["too_new_paths_sample"])
            assertEquals("false", record.diagnosticDetails["too_new_paths_truncated"])
            assertEquals("5", record.diagnosticDetails["held_folder_count"])
            assertEquals(
                "statistics_unavailable=1\nempty_folder=1\ntoo_new=1\nunknown_age=1\nfolder_kept_table_named_like_folder=1",
                record.diagnosticDetails["held_folder_counts"],
            )
            assertEquals("1", record.diagnosticDetails["folder_kept_table_named_like_folder_count"])
            listOf("orphan_48h", "orphan_invalid", "orphan_unreadable", "orphan_named").forEach {
                assertTrue(flow.objectExists("$DATABASE_LOCATION/$it/part-0.parquet"), it)
            }
            assertEquals(emptyList<String>(), heldWarnings(logs))

            val lines = flow.summaryLines()
            assertTrue("  Folders newer than the age cutoff                1" in lines, lines.joinToString("\n"))
            val held = lines.subList(lines.indexOf("FOLDERS HELD, BY REASON"), lines.indexOf("DELETION"))
            assertEquals(
                listOf(
                    "  statistics_unavailable                           1",
                    "  empty_folder                                     1",
                    "  too_new                                          1",
                    "  unknown_age                                      1",
                    "  folder_kept_table_named_like_folder              1",
                ),
                held.filter { it.matches(Regex("  [a-z_]+ +\\d+")) },
            )
            val tooNew = lines.indexOf("  too_new: the newest object is newer than the age cutoff of this run.")
            assertEquals("- $DATABASE_LOCATION/orphan_48h", lines[tooNew + 2])
            assertEquals(listOf("- $DATABASE_LOCATION/orphan_48h"), lines.filter { "orphan_48h" in it })
        }
    }

    @Test
    fun `one warning names each reason when every folder with no verified owner is held`() {
        folder("orphan_48h", hoursAgo(48.0))
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$DATABASE_LOCATION/empty")
        folder("orphan_named", hoursAgo(100.0))
        folder("gone", hoursAgo(100.0))
        InMemoryS3FileSystem.listFailures["$DATABASE_LOCATION/gone"] = FileNotFoundException("No such file or directory: gone")
        InMemoryS3FileSystem.statusFailures["$DATABASE_LOCATION/gone"] = FileNotFoundException("No such file or directory: gone")

        capturingLogs { logs ->
            val flow = flow(deleteEnabled = true)
            every { flow.catalogDiscoveryService.tableStorageIfExists(CATALOG, "sales", "orphan_named") } returns
                NamedTableStorage.Found("$DATABASE_LOCATION/orphan_named")
            flow.run()

            val record = flow.auditRecords.single()
            assertEquals("SUCCESS", record.status)
            assertEquals(emptyList<String>(), record.deletedFolders)
            assertEquals("3", record.diagnosticDetails["held_folder_count"])
            assertEquals("1", record.diagnosticDetails["folder_not_found_count"])
            assertEquals(
                listOf(
                    "WARN: None of the 4 folder(s) with no verified catalog owner is eligible for deletion for catalog=example_catalog, " +
                        "database=sales. Held by reason: empty_folder=1, too_new=1, folder_kept_table_named_like_folder=1. Already gone: 1.",
                ),
                heldWarnings(logs),
            )
        }
    }

    @Test
    fun `a dry run warns when the age check holds every folder with no verified owner`() {
        folder("orphan_fresh", now - 5_000)

        capturingLogs { logs ->
            val flow = flow(deleteEnabled = false)
            flow.run()

            val record = flow.auditRecords.single()
            assertEquals(emptyList<String>(), record.candidateFolders)
            assertEquals("1", record.diagnosticDetails["too_new_count"])
            assertEquals(
                listOf(
                    "WARN: None of the 1 folder(s) with no verified catalog owner is eligible for deletion for catalog=example_catalog, " +
                        "database=sales. Held by reason: too_new=1.",
                ),
                heldWarnings(logs),
            )
        }
    }

    @Test
    fun `a database with unresolved ownership counts its held folders but does not warn about them`() {
        folder("orphan_48h", hoursAgo(48.0))

        capturingLogs { logs ->
            val flow = flow(deleteEnabled = true, unresolvedTable = "broken")
            flow.run()

            val record = flow.auditRecords.single()
            assertEquals("BLOCKED" to "unresolved_catalog_ownership", record.status to record.statusReason)
            assertEquals("1", record.diagnosticDetails["too_new_count"])
            assertEquals("$DATABASE_LOCATION/orphan_48h", record.diagnosticDetails["too_new_paths_sample"])
            assertEquals("too_new=1", record.diagnosticDetails["held_folder_counts"])
            assertEquals(emptyList<String>(), heldWarnings(logs))
        }
    }

    private fun hoursAgo(hours: Double): Long = now - (hours * Duration.ofHours(1).toMillis()).toLong()

    private fun ageFolderName(hours: Double): String = "orphan_${(hours * 60).toLong()}m"

    private fun floorWarnings(logs: List<CapturedLog>): List<String> =
        logs.filter { it.level == Logger.Level.WARN && "minimum for deletion mode" in it.message }.map { it.message }

    private fun heldWarnings(logs: List<CapturedLog>): List<String> =
        logs.filter { "with no verified catalog owner is eligible" in it.message }.map { "${it.level}: ${it.message}" }

    // The cutoff is taken once per database during the run, so it lies within the run's start and end minus the age.
    private fun assertCutoff(
        record: CleanupAuditRecord,
        before: Instant,
        after: Instant,
        hours: Long,
    ) {
        assertNotNull(record.cutoffTime)
        val cutoff = record.cutoffTime!!
        assertFalse(cutoff.isBefore(before.minus(Duration.ofHours(hours))), "cutoff=$cutoff")
        assertFalse(cutoff.isAfter(after.minus(Duration.ofHours(hours))), "cutoff=$cutoff")
    }

    private fun folder(
        name: String,
        objectModificationTimeMillis: Long,
    ) {
        InMemoryS3FileSystem.putDirectory(ENDPOINT, "$DATABASE_LOCATION/$name")
        InMemoryS3FileSystem.putFile(
            ENDPOINT,
            "$DATABASE_LOCATION/$name/part-0.parquet",
            length = 1,
            modificationTimeMillis = objectModificationTimeMillis,
        )
    }

    private fun flow(
        deleteEnabled: Boolean,
        collectSizeStatistics: Boolean = true,
        maxCandidateFolders: Int = 10,
        tableLocations: List<String> = listOf("$DATABASE_LOCATION/orders"),
        unresolvedTable: String? = null,
        olderThanHours: Long? = null,
    ): CleanupFlowHarness {
        val config =
            ApplicationConfig(
                catalog = CATALOG,
                databases = listOf("sales"),
                dryRun = !deleteEnabled,
                deleteEnabled = deleteEnabled,
                maxCandidateFoldersPerDatabase = maxCandidateFolders,
                collectSizeStatistics = collectSizeStatistics,
            )
        val flow = CleanupFlowHarness(olderThanHours?.let { config.copy(olderThanHours = it) } ?: config)
        val tables =
            tableLocations.mapIndexed { index, location ->
                DiscoveredTable(CATALOG, "sales", "table_$index", isTemporary = false, location = location)
            } +
                listOfNotNull(
                    unresolvedTable?.let {
                        DiscoveredTable(CATALOG, "sales", it, isTemporary = false, location = null, unresolvedReason = "metadata missing")
                    },
                )
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns
            DiscoveredDatabase(CATALOG, "sales", DATABASE_LOCATION, tables)
        every { flow.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
        return flow
    }
}
