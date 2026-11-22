package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.ENDPOINT
import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.audit.CleanupAuditRecord
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.catalog.NamedTableStorage
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import com.iomete.cleanup.untrackedtablefolders.storage.OWNERSHIP_SNAPSHOT_MAX_AGE
import io.mockk.every
import org.apache.hadoop.fs.FileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.junit.jupiter.params.provider.EnumSource
import org.junit.jupiter.params.provider.MethodSource
import org.junit.jupiter.params.provider.ValueSource

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val SALES = "s3a://bucket/lake/sales.db"
private const val ARCHIVE = "s3a://bucket/lake/archive.db"
private const val ORPHAN_A = "$SALES/orphan_a"
private const val ORPHAN_B = "$SALES/orphan_b"
private const val NEW_KEY = "$ORPHAN_B/new.parquet"

/**
 * A table or database that starts to own a candidate folder after the last ownership read the fence used and before
 * the folder's first delete. Within the snapshot limit nothing is read again, so only the name probe and the listing
 * before deletion can see it. Tests named "known gap" assert what the job does today, not what it should do.
 */
class OwnershipRaceFlowTest {

    private val clock = TestClock()
    private val catalog = Catalog()

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
        putFolder(SALES)
        putFolder("$SALES/t1", "part-0.parquet")
    }

    @AfterEach
    fun tearDown() {
        val recursive = InMemoryS3FileSystem.deleteCalls.filter { it.second }
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
        assertEquals(emptyList<Pair<String, Boolean>>(), recursive, "no delete may be recursive")
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("readBeforeTheFolder")
    fun `an owner registered before the read again keeps the folder`(race: Race) {
        runAndAssert(race)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("namedLikeTheFolderBeforeTheProbe")
    fun `a table named like the folder and registered before the name probe keeps it`(race: Race) {
        runAndAssert(race)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("unresolvedNamedTableBeforeTheFolder")
    fun `a table named like the folder whose storage cannot be established stops the rest of the database`(race: Race) {
        runAndAssert(race)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("newObjectBeforeTheListing")
    fun `an owner registered after the last read that writes into the folder before its listing keeps it as changed`(race: Race) {
        runAndAssert(race)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("metadataOnlyAfterTheLastRead")
    fun `known gap - an owner registered after the last read with no new object is not seen and its folder is deleted`(race: Race) {
        runAndAssert(race)
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("newObjectAfterTheListing")
    fun `known gap - an owner that writes into the folder after its listing loses the listed keys and the row reports the folder partly deleted`(
        race: Race,
    ) {
        runAndAssert(race)
    }

    @Test
    fun `a read again that finds no new owner deletes the folder, so a kept folder above is kept by its owner`() {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val flow = flow()
        onDelete(ORPHAN_A) { clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1)) }

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS" to null, record.status to record.statusReason)
        assertEquals(listOf(ORPHAN_A, ORPHAN_B), record.deletedFolders)
        assertEquals("1", record.diagnosticDetails["ownership_refresh_count"])
    }

    @Test
    fun `every owner, window and content is asserted by exactly one test`() {
        val sources =
            listOf(
                readBeforeTheFolder(),
                namedLikeTheFolderBeforeTheProbe(),
                unresolvedNamedTableBeforeTheFolder(),
                newObjectBeforeTheListing(),
                metadataOnlyAfterTheLastRead(),
                newObjectAfterTheListing(),
            )
        assertEquals(Race.all().toSet(), sources.flatten().toSet())
        assertEquals(Race.all().size, sources.sumOf { it.size })
        assertEquals(Owner.entries.size * Window.entries.size * Content.entries.size, Race.all().size)
    }

    // A cheaper check than a full read would list names again (SHOW NAMESPACES, SHOW TABLES) and describe only new
    // ones. These owners keep every name, so such a check cannot see them. Only a read of every table and database can.
    @ParameterizedTest(name = "{0}")
    @EnumSource(
        names = ["SAME_DATABASE_SET_LOCATION", "SAME_DATABASE_WRITE_DATA_PATH", "OTHER_DATABASE_SET_LOCATION", "OTHER_DATABASE_WRITE_DATA_PATH", "OTHER_DATABASE_LOCATION_SET_TO_FOLDER"],
    )
    fun `a check of new names only cannot see an owner that keeps every name`(owner: Owner) {
        val names = catalog.names()
        val state = catalog.state()

        owner.register(catalog)

        assertEquals(names, catalog.names())
        assertTrue(catalog.state() != state)
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(
        strings = [
            "s3a://bucket/lake/sales.db/orphan_b/",
            "s3://bucket/lake/sales.db/orphan_b",
            "s3n://bucket/lake/sales.db/orphan_b",
            "S3A://BUCKET/lake/sales.db/orphan_b",
            "s3a://bucket/lake//sales.db/orphan_b",
            "s3a://bucket/lake/sales.db/x/../orphan_b",
            "s3://Bucket/lake/sales.db/orphan_b/sub/",
        ],
    )
    fun `an owner of another database written in another form of the folder path is matched at the read again`(location: String) {
        runAndAssert(Race(Owner.OTHER_DATABASE_AT_FOLDER, Window.BEFORE_READ_AGAIN, Content.METADATA_ONLY)) { add("archive", "adopted", location) }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(
        strings = [
            "s3a://bucket/lake/sales.db/orphan_b/",
            "s3://bucket/lake/sales.db/orphan_b",
            "S3A://BUCKET/lake/sales.db/orphan_b",
            "s3a://bucket/lake//sales.db/orphan_b",
            "s3a://bucket/lake/sales.db/x/../orphan_b",
        ],
    )
    fun `a table named like the folder written in another form of the folder path is matched by the name probe`(location: String) {
        runAndAssert(Race(Owner.TABLE_NAMED_LIKE_FOLDER, Window.WHILE_EARLIER_FOLDER_IS_DELETED, Content.METADATA_ONLY)) {
            add("sales", "orphan_b", location)
        }
    }

    // Folder names come from the listing and table locations from the catalog. Both keep percent signs and other
    // characters literal, so the same raw text is the same key.
    @ParameterizedTest(name = "folder {0}")
    @ValueSource(strings = ["orphan b", "orphan#b", "orphan+b", "orphän_b", "orphan%20b"])
    fun `an owner at the same raw path as a folder with special characters keeps it at the read again`(name: String) {
        val folder = "$SALES/$name"
        putFolder(FIRST, "part-0.parquet")
        putFolder(folder, "part-0.parquet")
        val flow = flow()
        onDelete(FIRST) {
            catalog.add("archive", "adopted", folder)
            clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
        }

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf(FIRST), record.deletedFolders)
        assertKept(record, "at_final_check", folder)
        assertTrue(flow.objectExists("$folder/part-0.parquet"))
    }

    @ParameterizedTest(name = "folder {0}")
    @ValueSource(strings = ["orphan b", "orphan#b", "orphan+b", "orphän_b", "orphan%20b"])
    fun `a table named like a folder with special characters and stored at its raw path keeps it at the name probe`(name: String) {
        val folder = "$SALES/$name"
        putFolder(FIRST, "part-0.parquet")
        putFolder(folder, "part-0.parquet")
        val flow = flow()
        onDelete("$FIRST/part-0.parquet") { catalog.add("sales", name, folder) }

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals(listOf(FIRST), record.deletedFolders)
        assertKept(record, "table_named_like_folder", folder)
        assertTrue(flow.objectExists("$folder/part-0.parquet"))
    }

    // An encoded name and a raw name are two different keys in S3, so neither owns the other's folder.
    @ParameterizedTest(name = "folder {0}, owner {1}")
    @CsvSource("'orphan b', 'orphan%20b'", "'orphan%20b', 'orphan b'", "'orphän_b', 'orph%C3%A4n_b'")
    fun `an owner whose path encodes the folder name differently names another key, so the folder is deleted`(
        folderName: String,
        ownerName: String,
    ) {
        val folder = "$SALES/$folderName"
        putFolder(FIRST, "part-0.parquet")
        putFolder(folder, "part-0.parquet")
        putFolder("$SALES/$ownerName")
        val flow = flow()
        onDelete(FIRST) {
            catalog.add("archive", "adopted", "$SALES/$ownerName")
            clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1))
        }

        flow.run()

        val record = flow.auditRecords.single()
        assertEquals("SUCCESS", record.status)
        assertEquals(listOf(FIRST, folder).sorted(), record.deletedFolders.sorted())
        assertFalse(flow.objectExists("$folder/part-0.parquet"))
        assertTrue(flow.objectExists("$SALES/$ownerName"))
    }

    private fun runAndAssert(
        race: Race,
        register: Catalog.() -> Unit = race.owner.register,
    ) {
        putFolder(ORPHAN_A, "part-0.parquet")
        putFolder(ORPHAN_B, "part-0.parquet")
        val flow = flow()
        val before = catalog.state()
        arrange(race) { catalog.register() }

        flow.run()

        assertTrue(catalog.state() != before, "the owner was never registered: $race")
        val record = flow.auditRecords.single()
        val details = record.diagnosticDetails
        assertEquals(if (race.window.olderThanLimit) "1" else null, details["ownership_refresh_count"], race.toString())
        val bKeys = InMemoryS3FileSystem.paths(ENDPOINT).filter { it.startsWith("$ORPHAN_B/") }.sorted()
        when (race.outcome) {
            Outcome.DELETED -> {
                assertEquals("SUCCESS" to null, record.status to record.statusReason, details.toString())
                assertEquals(listOf(ORPHAN_A, ORPHAN_B), record.deletedFolders)
                assertEquals(emptyList<String>(), bKeys)
            }
            Outcome.KEPT_CLAIMED, Outcome.KEPT_NAMED_LIKE_FOLDER, Outcome.KEPT_CHANGED -> {
                assertEquals("SUCCESS" to null, record.status to record.statusReason, details.toString())
                assertEquals(listOf(ORPHAN_A), record.deletedFolders)
                assertKept(record, race.outcome.keptReason!!, ORPHAN_B)
                assertEquals(race.content.keysIn(), bKeys)
                assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first.startsWith("$ORPHAN_B/") }, InMemoryS3FileSystem.deleteCalls.toString())
            }
            Outcome.STOPPED_AT_READ_AGAIN, Outcome.STOPPED_AT_NAME_PROBE -> {
                assertEquals("FAILED" to "deletion_stopped_at_recheck", record.status to record.statusReason, details.toString())
                assertEquals(listOf(ORPHAN_A), record.deletedFolders)
                assertEquals(race.outcome.stopCause, details["deletion_stop_cause"])
                assertEquals(ORPHAN_B, details["folder_not_attempted_paths_sample"])
                assertEquals(null, details["partially_deleted_folder_path"])
                assertEquals(race.content.keysIn(), bKeys)
                assertTrue(InMemoryS3FileSystem.deleteCalls.none { it.first.startsWith(ORPHAN_B) }, InMemoryS3FileSystem.deleteCalls.toString())
            }
            Outcome.LISTED_KEYS_DELETED -> {
                assertEquals("FAILED" to "deletion_failed", record.status to record.statusReason, details.toString())
                assertEquals(listOf(ORPHAN_A), record.deletedFolders)
                assertEquals(ORPHAN_B, details["partially_deleted_folder_path"])
                assertEquals("1", details["partially_deleted_folder_deleted_object_count"])
                assertEquals("part-0.parquet", details["partially_deleted_folder_deleted_keys_sample"])
                assertEquals("new.parquet", details["partially_deleted_folder_unlisted_keys_sample"])
                assertEquals(null, details["partially_deleted_folder_remaining_keys_sample"])
                assertEquals(null, details["partially_deleted_folder_unknown_state_keys_sample"])
                assertEquals(listOf(NEW_KEY), bKeys)
            }
        }
        assertFalse(flow.objectExists("$ORPHAN_A/part-0.parquet"))
    }

    private fun arrange(
        race: Race,
        register: () -> Unit,
    ) {
        val registerOwner = {
            register()
            if (race.content == Content.NEW_OBJECT) InMemoryS3FileSystem.putFile(ENDPOINT, NEW_KEY, length = 1)
        }
        val expire = { clock.advance(OWNERSHIP_SNAPSHOT_MAX_AGE.plusSeconds(1)) }
        when (race.window) {
            Window.WHILE_EARLIER_FOLDER_IS_DELETED -> onDelete("$ORPHAN_A/part-0.parquet", registerOwner)
            Window.AFTER_NAME_PROBE -> catalog.afterNameProbe("sales", "orphan_b", registerOwner)
            Window.AFTER_LISTING_BEFORE_DELETION -> afterListingBeforeDeletion(ORPHAN_B, registerOwner)
            Window.BEFORE_READ_AGAIN ->
                onDelete(ORPHAN_A) {
                    registerOwner()
                    expire()
                }
            Window.AFTER_READ_AGAIN_AND_LISTING -> {
                onDelete(ORPHAN_A, expire)
                afterListingBeforeDeletion(ORPHAN_B, registerOwner)
            }
        }
    }

    // The snapshot age is checked before each delete call, so an action inside the call of a folder's own directory,
    // its last delete, lets the folder finish and ages the snapshot before the next folder.
    private fun onDelete(
        objectPath: String,
        action: () -> Unit,
    ) {
        InMemoryS3FileSystem.beforeDelete = { path -> if (path == objectPath) action() }
    }

    // The first listing of a candidate is its inspection; the second is the listing before deletion.
    private fun afterListingBeforeDeletion(
        folder: String,
        action: () -> Unit,
    ) {
        InMemoryS3FileSystem.afterListFiles = { root, call -> if (root == folder && call == 2) action() }
    }

    private fun assertKept(
        record: CleanupAuditRecord,
        reason: String,
        path: String,
    ) {
        assertEquals("1", record.diagnosticDetails["folder_kept_${reason}_count"], record.diagnosticDetails.toString())
        assertEquals(path, record.diagnosticDetails["folder_kept_${reason}_paths_sample"])
        assertFalse(path in record.deletedFolders)
    }

    private fun flow(): CleanupFlowHarness =
        CleanupFlowHarness(
            ApplicationConfig(catalog = CATALOG, databases = listOf("sales"), dryRun = false, deleteEnabled = true),
            ticker = clock,
        ).also {
            every { it.catalogDiscoveryService.sharedWarehouseRoots(CATALOG) } returns listOf(WAREHOUSE)
            every { it.catalogDiscoveryService.visibleDatabaseLocations(CATALOG) } answers { catalog.databaseLocations() }
            every { it.catalogDiscoveryService.discoverTables(CATALOG, any()) } answers { catalog.tablesOf(secondArg()) }
            every { it.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } answers {
                DiscoveredDatabase(CATALOG, "sales", SALES, catalog.tablesOf("sales"))
            }
            every { it.catalogDiscoveryService.tableStorageIfExists(CATALOG, any(), any()) } answers {
                catalog.nameProbe(secondArg(), thirdArg())
            }
        }

    /** The catalog as one state, so an owner registered in a window is what every later read of it returns. */
    class Catalog {
        private val databases = linkedMapOf<String, String?>()
        private val tables = linkedMapOf<String, MutableList<DiscoveredTable>>()
        private val probeHooks = mutableMapOf<String, () -> Unit>()

        init {
            reset()
        }

        @Synchronized
        fun reset() {
            databases.clear()
            tables.clear()
            probeHooks.clear()
            databases += mapOf("sales" to SALES, "archive" to ARCHIVE)
            add("sales", "t1", "$SALES/t1")
            add("archive", "events", "$ARCHIVE/events")
        }

        @Synchronized
        fun databaseLocations(): Map<String, String?> = LinkedHashMap(databases)

        @Synchronized
        fun state(): String = "$databases $tables"

        @Synchronized
        fun names(): Set<String> = databases.keys + tables.flatMap { (database, list) -> list.map { "$database.${it.table}" } }

        @Synchronized
        fun tablesOf(database: String): List<DiscoveredTable> = tables[database].orEmpty().toList()

        @Synchronized
        fun createDatabase(
            database: String,
            location: String,
        ) {
            databases[database] = location
        }

        @Synchronized
        fun add(
            database: String,
            table: String,
            location: String,
            writeLocations: List<String> = emptyList(),
        ) {
            tables.getOrPut(database) { mutableListOf() } +=
                DiscoveredTable(CATALOG, database, table, isTemporary = false, location = location, writeLocations = writeLocations, provider = "iceberg")
        }

        // As the full read reports a table whose files may lie outside its declared paths.
        @Synchronized
        fun addUnresolved(
            database: String,
            table: String,
            reason: String,
            knownLocation: String,
        ) {
            tables.getOrPut(database) { mutableListOf() } +=
                DiscoveredTable(CATALOG, database, table, isTemporary = false, location = null, unresolvedReason = reason, knownLocation = knownLocation)
        }

        @Synchronized
        fun drop(
            database: String,
            table: String,
        ) {
            tables.getValue(database).removeIf { it.table == table }
        }

        @Synchronized
        fun update(
            database: String,
            table: String,
            change: (DiscoveredTable) -> DiscoveredTable,
        ) {
            tables.getValue(database).replaceAll { if (it.table == table) change(it) else it }
        }

        @Synchronized
        fun afterNameProbe(
            database: String,
            table: String,
            action: () -> Unit,
        ) {
            probeHooks["$database.$table"] = action
        }

        // The answer is read first, so an owner registered by the hook is not in it.
        @Synchronized
        fun nameProbe(
            database: String,
            table: String,
        ): NamedTableStorage {
            val found = tables[database]?.firstOrNull { it.table == table }
            probeHooks.remove("$database.$table")?.invoke()
            return when {
                found == null -> NamedTableStorage.NotFound
                found.isUnresolved -> NamedTableStorage.Unresolved(found.unresolvedReason!!)
                else -> NamedTableStorage.Found(found.location, found.writeLocations)
            }
        }
    }

    enum class Owner(
        val named: Boolean = false,
        val unresolved: Boolean = false,
        val register: Catalog.() -> Unit,
    ) {
        TABLE_NAMED_LIKE_FOLDER(named = true, register = { add("sales", "orphan_b", ORPHAN_B) }),

        // Stored elsewhere, so only its write path claims the folder.
        TABLE_NAMED_LIKE_FOLDER_WITH_WRITE_PATH(named = true, register = {
            add("sales", "orphan_b", "$SALES/orphan_b_v2", writeLocations = listOf("$ORPHAN_B/data"))
        }),
        TABLE_NAMED_LIKE_FOLDER_GC_DISABLED(unresolved = true, register = {
            addUnresolved("sales", "orphan_b", "gc.enabled is not true, so the table may use files outside its location", "$SALES/orphan_b_v2")
        }),
        TABLE_NAMED_LIKE_FOLDER_LOCATION_PROVIDER(unresolved = true, register = {
            addUnresolved("sales", "orphan_b", "write.location-provider.impl is set, so the table may write outside its declared paths", "$SALES/orphan_b_v2")
        }),
        SAME_DATABASE_AT_FOLDER(register = { add("sales", "adopted", ORPHAN_B) }),
        SAME_DATABASE_UNDER_FOLDER(register = { add("sales", "adopted", "$ORPHAN_B/sub") }),
        SAME_DATABASE_ABOVE_FOLDER(register = { add("sales", "adopted", SALES) }),
        SAME_DATABASE_SET_LOCATION(register = { update("sales", "t1") { it.copy(location = ORPHAN_B) } }),
        SAME_DATABASE_WRITE_DATA_PATH(register = { update("sales", "t1") { it.copy(writeLocations = listOf("$ORPHAN_B/data")) } }),
        OTHER_DATABASE_AT_FOLDER(register = { add("archive", "adopted", ORPHAN_B) }),
        OTHER_DATABASE_UNDER_FOLDER(register = { add("archive", "adopted", "$ORPHAN_B/sub") }),
        OTHER_DATABASE_ABOVE_FOLDER(register = { add("archive", "adopted", WAREHOUSE) }),
        OTHER_DATABASE_SET_LOCATION(register = { update("archive", "events") { it.copy(location = ORPHAN_B) } }),
        OTHER_DATABASE_RENAMED_INTO_FOLDER(register = {
            drop("archive", "events")
            add("archive", "events_v2", ORPHAN_B)
        }),
        OTHER_DATABASE_WRITE_DATA_PATH(register = { update("archive", "events") { it.copy(writeLocations = listOf(ORPHAN_B)) } }),
        OTHER_DATABASE_NEW_TABLE_WRITE_METADATA_PATH(register = { add("archive", "logs", "$ARCHIVE/logs", writeLocations = listOf("$ORPHAN_B/metadata")) }),
        OTHER_DATABASE_LOCATION_SET_TO_FOLDER(register = { createDatabase("archive", ORPHAN_B) }),
        NEW_DATABASE_AT_FOLDER(register = { createDatabase("staging", ORPHAN_B) }),
        NEW_DATABASE_TABLE_IN_FOLDER(register = {
            createDatabase("staging", "$WAREHOUSE/staging.db")
            add("staging", "adopted", ORPHAN_B)
        }),
    }

    enum class Window(val olderThanLimit: Boolean) {
        WHILE_EARLIER_FOLDER_IS_DELETED(false),
        AFTER_NAME_PROBE(false),
        AFTER_LISTING_BEFORE_DELETION(false),
        BEFORE_READ_AGAIN(true),
        AFTER_READ_AGAIN_AND_LISTING(true),
    }

    enum class Content {
        METADATA_ONLY,
        NEW_OBJECT,
        ;

        fun keysIn(): List<String> = listOfNotNull("$ORPHAN_B/part-0.parquet", NEW_KEY.takeIf { this == NEW_OBJECT }).sorted()
    }

    enum class Outcome(
        val keptReason: String? = null,
        val stopCause: String? = null,
    ) {
        DELETED,
        KEPT_CLAIMED("at_final_check"),
        KEPT_NAMED_LIKE_FOLDER("table_named_like_folder"),
        KEPT_CHANGED("changed_since_inspection"),
        STOPPED_AT_READ_AGAIN(stopCause = "own_tables_unresolved"),
        STOPPED_AT_NAME_PROBE(stopCause = "name_probe_failed"),
        LISTED_KEYS_DELETED,
    }

    data class Race(
        val owner: Owner,
        val window: Window,
        val content: Content,
    ) {
        // What the job does today. The name probe sees only a table named like the folder in this database, with its
        // location and write paths, and stops on one whose storage cannot be established; the listing before deletion
        // sees only a new object listed by it.
        val outcome: Outcome
            get() =
                when (window) {
                    Window.BEFORE_READ_AGAIN ->
                        when {
                            owner.unresolved -> Outcome.STOPPED_AT_READ_AGAIN
                            owner.named -> Outcome.KEPT_NAMED_LIKE_FOLDER
                            else -> Outcome.KEPT_CLAIMED
                        }
                    Window.WHILE_EARLIER_FOLDER_IS_DELETED ->
                        when {
                            owner.unresolved -> Outcome.STOPPED_AT_NAME_PROBE
                            owner.named -> Outcome.KEPT_NAMED_LIKE_FOLDER
                            content == Content.NEW_OBJECT -> Outcome.KEPT_CHANGED
                            else -> Outcome.DELETED
                        }
                    Window.AFTER_NAME_PROBE -> if (content == Content.NEW_OBJECT) Outcome.KEPT_CHANGED else Outcome.DELETED
                    Window.AFTER_LISTING_BEFORE_DELETION, Window.AFTER_READ_AGAIN_AND_LISTING ->
                        if (content == Content.NEW_OBJECT) Outcome.LISTED_KEYS_DELETED else Outcome.DELETED
                }

        override fun toString(): String = "$owner, $window, $content -> $outcome"

        companion object {
            fun all(): List<Race> =
                Owner.entries.flatMap { owner -> Window.entries.flatMap { window -> Content.entries.map { Race(owner, window, it) } } }
        }
    }

    companion object {
        // Sorts before every other folder name used here, so it is always deleted first.
        private const val FIRST = "$SALES/a_first"

        @JvmStatic
        fun readBeforeTheFolder(): List<Race> =
            Race.all().filter { it.outcome == Outcome.KEPT_CLAIMED || (it.outcome == Outcome.KEPT_NAMED_LIKE_FOLDER && it.window.olderThanLimit) }

        @JvmStatic
        fun namedLikeTheFolderBeforeTheProbe(): List<Race> =
            Race.all().filter { it.outcome == Outcome.KEPT_NAMED_LIKE_FOLDER && !it.window.olderThanLimit }

        @JvmStatic
        fun unresolvedNamedTableBeforeTheFolder(): List<Race> =
            Race.all().filter { it.outcome == Outcome.STOPPED_AT_READ_AGAIN || it.outcome == Outcome.STOPPED_AT_NAME_PROBE }

        @JvmStatic
        fun newObjectBeforeTheListing(): List<Race> = Race.all().filter { it.outcome == Outcome.KEPT_CHANGED }

        @JvmStatic
        fun metadataOnlyAfterTheLastRead(): List<Race> = Race.all().filter { it.outcome == Outcome.DELETED }

        @JvmStatic
        fun newObjectAfterTheListing(): List<Race> = Race.all().filter { it.outcome == Outcome.LISTED_KEYS_DELETED }
    }
}
