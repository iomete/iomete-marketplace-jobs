package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.catalog.readers.CatalogReadResult
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.HmsCatalogReader
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.IcebergCatalogReader
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.LocationRole
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.OwnedLocation
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.ReadGap
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.ReadGapKind
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.RestCatalogReader
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.TestRestCatalogServer
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogFileSystemProvider
import io.mockk.every
import io.mockk.mockk
import java.nio.file.Path
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.apache.iceberg.rest.RESTCatalog
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.types.Types
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.io.TempDir

private const val EXTERNAL_TOKEN = "external-test-token"

private const val MANAGED_URI = "http://iom-rest-catalog.iomete-system.svc.cluster.local/internal/catalogs/spark_catalog"

class CatalogReaderDiscoveryTest {
    @TempDir
    lateinit var dir: Path

    private val schema = Schema(Types.NestedField.required(1, "id", Types.LongType.get()))
    private val closeables = mutableListOf<AutoCloseable>()
    private val sessionConf = mutableMapOf<String, String>()

    @AfterEach
    fun close() = closeables.forEach { it.close() }

    @Test
    fun `spark_catalog reads databases from the metastore and the REST catalog, and labels tables instead of naming them`() {
        val hms = hms()
        hms.database("sales", "s3a://lake/wh/sales.db")
        hms.table("sales", "orders", "s3a://lake/wh/sales.db/orders")
        val server = server()
        server.backend.createNamespace(Namespace.of("analytics"), mapOf("location" to "s3a://lake/wh/analytics"))
        server.backend.buildTable(TableIdentifier.of("analytics", "clicks"), schema).withLocation("s3a://lake/wh/analytics/clicks").create()
        val service = service(hms, server)

        val databases = service.visibleDatabaseLocations("spark_catalog")
        val sales = service.discoverDatabase("spark_catalog", "sales")
        val analytics = service.discoverDatabase("spark_catalog", "analytics")

        assertEquals(mapOf("sales" to "s3a://lake/wh/sales.db", "analytics" to "s3a://lake/wh/analytics"), databases)
        assertEquals("s3a://lake/wh/sales.db", sales.location)
        assertEquals(listOf("table-1"), sales.tables.map { it.table })
        assertEquals("s3a://lake/wh/sales.db/orders", sales.tables.single().location)
        assertEquals("s3a://lake/wh/analytics/clicks", analytics.tables.single().location)
        assertTrue(analytics.tables.single().iceberg!!.uuid != null, "a REST table is a live Iceberg table for the UUID veto")
        listOf(sales, analytics).flatMap { it.tables }.forEach { table ->
            assertFalse(table.qualifiedName.contains("orders") || table.qualifiedName.contains("clicks"), table.qualifiedName)
        }
    }

    @Test
    fun `a table with a variant column is read from its metadata, so the database is not failed`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("events"), mapOf("location" to "s3a://lake/wh/events"))
        server.backend
            .buildTable(
                TableIdentifier.of("events", "payloads"),
                Schema(Types.NestedField.required(1, "id", Types.LongType.get()), Types.NestedField.optional(2, "body", Types.VariantType.get())),
            ).withLocation("s3a://lake/wh/events/payloads")
            .withProperty("format-version", "3")
            .create()
        val service = service(hms(), server)

        val events = service.discoverDatabase("spark_catalog", "events")

        assertEquals(listOf("s3a://lake/wh/events/payloads"), events.fileTableLocations)
        assertTrue(events.unresolvedTables.isEmpty())
        assertEquals("s3a://lake/wh/events/payloads", service.tableLocationIfExists("spark_catalog", "events", "payloads"))
    }

    @Test
    fun `a metastore database or table location without scheme and bucket keeps its path and is never treated as qualified`() {
        val hms = hms()
        hms.database("bare", "/warehouse/bare.db")
        hms.table("bare", "orders", "/warehouse/bare.db/orders")
        val service = service(hms, server())

        val bare = service.discoverDatabase("spark_catalog", "bare")
        val orders = bare.tables.single()

        assertEquals("/warehouse/bare.db", service.visibleDatabaseLocations("spark_catalog")["bare"])
        assertEquals("/warehouse/bare.db", bare.location)
        assertNull(orders.location)
        assertFalse(orders.isUnresolved)
        assertEquals(listOf("s3a://other/warehouse/bare.db/orders"), orders.unqualifiedPathsIn("s3a://other/warehouse"))
        assertEquals(listOf("s3a://other/warehouse/bare.db/orders"), orders.pathsUnder("s3a://other/warehouse/bare.db"))
        assertTrue(orders.pathsUnder("s3a://other/elsewhere").isEmpty())
        assertThrows<IllegalStateException> { service.tableLocationIfExists("spark_catalog", "bare", "orders") }
        val storage = service.tableStorageIfExists("spark_catalog", "bare", "orders") as NamedTableStorage.Found
        assertEquals(listOf("/warehouse/bare.db/orders"), storage.writeLocations)
    }

    @Test
    fun `a REST namespace without a location uses the managed catalog's derived folder, and none for an unrecognized catalog`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("rest_only"))
        configureSessionCatalog(MANAGED_URI)

        assertEquals("s3a://lake/wh/rest_only", service(hms(), server).visibleDatabaseLocations("spark_catalog")["rest_only"])

        configureSessionCatalog("https://other.example.com/catalog")
        val unrecognized = service(hms(), server).visibleDatabaseLocations("spark_catalog")
        assertTrue(unrecognized.containsKey("rest_only"))
        assertNull(unrecognized["rest_only"])
    }

    @Test
    fun `a JDBC table's location is protected but stores no files, and one without a location is not listed`() {
        val hms = hms()
        hms.database("sales", "s3a://lake/wh/sales.db")
        hms.table("sales", "pg_orders", "s3a://lake/wh/sales.db/pg_orders", provider = "org.apache.spark.sql.jdbc")
        hms.table("sales", "mssql", "s3a://lake/wh/sales.db/mssql", provider = "com.microsoft.sqlserver.jdbc.spark")
        hms.table("sales", "pg_nowhere", null, provider = "jdbc")
        hms.table("sales", "orders", "s3a://lake/wh/sales.db/orders", provider = "parquet")

        val sales = service(hms, server()).discoverDatabase("spark_catalog", "sales")

        assertEquals(3, sales.tables.size)
        assertEquals(listOf("s3a://lake/wh/sales.db/orders"), sales.fileTableLocations)
        val jdbc = sales.tables.filterNot { it.storesFiles }
        assertEquals(setOf("s3a://lake/wh/sales.db/pg_orders", "s3a://lake/wh/sales.db/mssql"), jdbc.mapNotNull { it.location }.toSet())
        assertTrue(sales.unresolvedTables.isEmpty())
    }

    @Test
    fun `storage use a complete read can't list makes the table unresolved and keeps its location known`() {
        val hms = hms()
        hms.database("sales", "s3a://lake/wh/sales.db")
        hms.table("sales", "manifest", "s3a://lake/wh/sales.db/manifest", inputFormat = "org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat")
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"))
        server.backend.buildTable(TableIdentifier.of("sales", "kept"), schema)
            .withLocation("s3a://lake/wh/sales/kept")
            .withProperty("gc.enabled", "false")
            .create()
        val service = service(hms, server)

        val tables = service.discoverDatabase("spark_catalog", "sales").tables

        assertEquals(
            setOf("unknown_external_storage_use:symlink_manifest_table", "unknown_external_storage_use:gc_disabled"),
            tables.mapNotNull { it.unresolvedReason }.toSet(),
        )
        assertTrue(tables.all { it.location == null })
        assertEquals(setOf("s3a://lake/wh/sales.db/manifest", "s3a://lake/wh/sales/kept"), tables.mapNotNull { it.knownLocation }.toSet())
        assertTrue(service.tableStorageIfExists("spark_catalog", "sales", "kept") is NamedTableStorage.Unresolved)
        assertThrows<IllegalStateException> { service.tableLocationIfExists("spark_catalog", "sales", "manifest") }
    }

    @Test
    fun `a view protects the paths it reads and has no table location`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://lake/wh/sales"))
        server.backend.buildView(TableIdentifier.of("sales", "raw_events"))
            .withSchema(schema)
            .withDefaultNamespace(Namespace.of("sales"))
            .withQuery("spark", "SELECT * FROM parquet.`s3a://lake/raw/events`")
            .create()
        val service = service(hms(), server)

        val view = service.discoverDatabase("spark_catalog", "sales").tables.single()

        assertTrue(view.isView)
        assertEquals("view-1", view.table)
        assertEquals(listOf("s3a://lake/raw/events"), view.pathsUnder("s3a://lake/raw"))
        assertNull(service.tableLocationIfExists("spark_catalog", "sales", "raw_events"))
    }

    @Test
    fun `a failed listing, database read or table read is never an empty catalog or a table without storage`() {
        val listing = server()
        listing.fault = { path -> if (path == "namespaces") 500 else null }
        assertThrows<IllegalStateException> { service(hms(), listing).visibleDatabaseLocations("spark_catalog") }

        val database = server()
        database.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://lake/wh/sales"))
        database.fault = { path -> if (path == "namespaces/sales/tables") 500 else null }
        assertThrows<IllegalStateException> { service(hms(), database).discoverDatabase("spark_catalog", "sales") }

        val table = server()
        table.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://lake/wh/sales"))
        table.backend.buildTable(TableIdentifier.of("sales", "bad"), schema).withLocation("s3a://lake/wh/sales/bad").create()
        table.fault = { path -> if (path.endsWith("/tables/bad")) 500 else null }
        val bad = service(hms(), table).discoverDatabase("spark_catalog", "sales").tables.single()
        assertTrue(bad.isUnresolved, "a table that failed to load is unresolved")
        assertNull(bad.location)

        val brokenMetastore =
            object : org.apache.spark.sql.catalyst.catalog.InMemoryCatalog(org.apache.spark.SparkConf(), Configuration()) {
                override fun listDatabases(): scala.collection.Seq<String> = throw IllegalStateException("metastore down")
            }
        val service = service(sources = { listOf(HmsSource(HmsCatalogReader("spark_catalog", brokenMetastore, null))) })
        assertThrows<IllegalStateException> { service.visibleDatabaseLocations("spark_catalog") }
    }

    @Test
    fun `a failure in one store of spark_catalog is not hidden by the other store's complete read`() {
        val hms = hms()
        hms.database("sales", "s3a://lake/wh/sales.db")
        hms.table("sales", "orders", "s3a://lake/wh/sales.db/orders")
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://lake/wh/sales"))
        server.backend.buildTable(TableIdentifier.of("sales", "orders"), schema).withLocation("s3a://lake/wh/sales/orders").create()
        val service = service(hms, server)

        val both = service.tableStorageIfExists("spark_catalog", "sales", "orders") as NamedTableStorage.Found
        assertEquals("s3a://lake/wh/sales.db/orders", both.location)
        assertEquals(listOf("s3a://lake/wh/sales/orders"), both.writeLocations)
        assertThrows<IllegalStateException> { service.tableLocationIfExists("spark_catalog", "sales", "orders") }
        assertEquals(2, service.discoverDatabase("spark_catalog", "sales").tables.size)

        server.fault = { path -> if (path.endsWith("/tables/orders")) 500 else null }
        assertTrue(service.tableStorageIfExists("spark_catalog", "sales", "orders") is NamedTableStorage.Unresolved)
        assertTrue(service.discoverDatabase("spark_catalog", "sales").tables.any { it.isUnresolved })
    }

    @Test
    fun `each catalog is read from its own stores, so one catalog's failure does not reach another`() {
        val good = server(catalogName = "good")
        good.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://lake/good/sales"))
        val broken = server(catalogName = "broken")
        broken.fault = { path -> if (path == "namespaces") 500 else null }
        val sources = mapOf("good" to listOf(rest(good)), "broken" to listOf(rest(broken)))
        val service = service(sources = { sources.getValue(it) })

        assertEquals(mapOf("sales" to "s3a://lake/good/sales"), service.visibleDatabaseLocations("good"))
        assertThrows<IllegalStateException> { service.visibleDatabaseLocations("broken") }
        assertThrows<DatabaseNotFoundException> { service.discoverDatabase("good", "missing") }
    }

    @Test
    fun `a nested REST namespace is read again by its levels`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://lake/wh/sales"))
        server.backend.createNamespace(Namespace.of("sales", "eu"), mapOf("location" to "s3a://lake/wh/sales/eu"))
        server.backend.buildTable(TableIdentifier.of(Namespace.of("sales", "eu"), "vat"), schema).withLocation("s3a://lake/wh/sales/eu/vat").create()
        val service = service(hms(), server)

        assertEquals("s3a://lake/wh/sales/eu", service.visibleDatabaseLocations("spark_catalog")["sales.eu"])
        assertEquals(listOf("s3a://lake/wh/sales/eu/vat"), service.discoverDatabase("spark_catalog", "sales.eu").fileTableLocations)
    }

    @Test
    fun `a catalog the readers can't read fails closed, and a JDBC catalog has no storage`() {
        sessionConf["spark.sql.catalog.unity"] = "io.unitycatalog.spark.UCSingleCatalog"
        sessionConf["spark.sql.catalog.pg"] = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"
        val service = service()

        assertThrows<UnsupportedCatalogException> { service.visibleDatabaseLocations("unity") }
        assertThrows<UnsupportedCatalogException> { service.discoverDatabase("unity", "sales") }
        assertEquals(emptyMap<String, String?>(), service.visibleDatabaseLocations("pg"))
    }

    @Test
    fun `objects a store files under another namespace than the one read fail the read, so they are never dropped`() {
        val store =
            FakeSource(
                namespaces = listOf(listOf("sales")),
                database = read(OwnedLocation(listOf("sales"), LocationRole.NAMESPACE, "s3a://lake/wh/sales"), OwnedLocation(listOf("Sales", "orders"), LocationRole.TABLE, "s3a://lake/wh/sales/orders")),
                gapsOnObject = listOf(ReadGap(listOf("sales.eu", "orders"), ReadGapKind.UNVERIFIED, "load_table_failed")),
            )
        val service = service(sources = { listOf(store) })

        assertThrows<IllegalStateException> { service.discoverDatabase("spark_catalog", "sales") }
        assertThrows<IllegalStateException> { service.tableStorageIfExists("spark_catalog", "sales", "orders") }
    }

    @Test
    fun `a gap for a namespace the listing did not return fails the database listing`() {
        val store = FakeSource(namespaces = listOf(listOf("a")), listingGaps = listOf(ReadGap(listOf("a", "b"), ReadGapKind.UNVERIFIED, "unexpected_namespace_listing")))

        assertThrows<IllegalStateException> { service(sources = { listOf(store) }).visibleDatabaseLocations("spark_catalog") }
    }

    @Test
    fun `a store that lists a database and then reports it missing fails the read, even when another store has it`() {
        val hms = hms()
        hms.database("sales", "s3a://lake/wh/sales.db")
        hms.table("sales", "orders", "s3a://lake/wh/sales.db/orders")
        val flaky = FakeSource(namespaces = listOf(listOf("sales")), database = null)
        val service = service(sources = { listOf(HmsSource(HmsCatalogReader("spark_catalog", hms.catalog, null)), flaky) })

        service.visibleDatabaseLocations("spark_catalog")
        assertThrows<IllegalStateException> { service.discoverDatabase("spark_catalog", "sales") }

        val fresh = service(sources = { listOf(HmsSource(HmsCatalogReader("spark_catalog", hms.catalog, null)), flaky) })
        assertThrows<IllegalStateException> { fresh.discoverDatabase("spark_catalog", "sales") }
    }

    @Test
    fun `a metastore database without a location never takes the REST catalog's derived folder`() {
        val hms = hms()
        hms.database("sales", "")
        configureSessionCatalog(MANAGED_URI)

        assertNull(service(hms, server()).visibleDatabaseLocations("spark_catalog")["sales"])
    }

    @Test
    fun `an Iceberg session catalog of type hive that names another metastore fails closed`() {
        sessionConf["spark.sql.catalog.spark_catalog"] = "org.apache.iceberg.spark.SparkSessionCatalog"
        sessionConf["spark.sql.catalog.spark_catalog.type"] = "hive"
        sessionConf["spark.sql.catalog.spark_catalog.uri"] = "thrift://other-metastore:9083"
        val service =
            service().apply {
                val spark = mockk<SparkSession>(relaxed = true)
                every { spark.conf() } returns sessionConfigOf(sessionConf)
                every { spark.sparkContext().hadoopConfiguration() } returns Configuration(false).apply { set("hive.metastore.uris", "thrift://metastore:9083") }
                sparkSessionProvider = mockk<SparkSessionProvider> { every { getOrCreate() } returns spark }
                catalogFileSystemProvider = mockk<CatalogFileSystemProvider> { every { catalogReadConfiguration(any()) } returns Configuration(false) }
            }

        assertThrows<UnsupportedCatalogException> { service.visibleDatabaseLocations("spark_catalog") }
    }

    @Test
    fun `every Iceberg write path property protects its path, and a custom location provider makes the table unresolved`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://lake/wh/sales"))
        server.backend.buildTable(TableIdentifier.of("sales", "paths"), schema)
            .withLocation("s3a://lake/wh/sales/paths")
            .withProperty("write.data.path", "s3a://lake/data/paths")
            .withProperty("write.metadata.path", "s3a://lake/meta/paths")
            .withProperty("write.object-storage.path", "s3a://lake/objects/paths")
            .withProperty("write.folder-storage.path", "s3a://lake/folders/paths")
            .create()
        server.backend.buildTable(TableIdentifier.of("sales", "custom"), schema).withLocation("s3a://lake/wh/sales/custom").create()
            .updateProperties().set("write.location-provider.impl", "com.example.Provider").commit()
        val service = service(hms(), server)

        val paths = service.tableStorageIfExists("spark_catalog", "sales", "paths") as NamedTableStorage.Found

        assertEquals(
            setOf("s3a://lake/data/paths", "s3a://lake/meta/paths", "s3a://lake/objects/paths", "s3a://lake/folders/paths"),
            paths.writeLocations.toSet(),
        )
        assertEquals(
            NamedTableStorage.Unresolved("unknown_external_storage_use:custom_location_provider"),
            service.tableStorageIfExists("spark_catalog", "sales", "custom"),
        )
    }

    @Test
    fun `JDBC provider names match in any letter case`() {
        val hms = hms()
        hms.database("sales", "s3a://lake/wh/sales.db")
        hms.table("sales", "pg", "s3a://lake/wh/sales.db/pg", provider = "ORG.APACHE.SPARK.SQL.JDBC")
        hms.table("sales", "mssql", "s3a://lake/wh/sales.db/mssql", provider = "Com.Microsoft.SqlServer.Jdbc.Spark")

        val sales = service(hms, server()).discoverDatabase("spark_catalog", "sales")

        assertTrue(sales.tables.none { it.storesFiles })
        assertTrue(sales.fileTableLocations.isEmpty())
    }

    @Test
    fun `a table read through a caching Iceberg catalog is read fresh, so a changed write path is seen`() {
        val backend = InMemoryCatalog().apply { initialize("backend", mapOf("warehouse" to "s3a://lake/wh")) }
        backend.createNamespace(Namespace.of("sales"))
        val table = backend.buildTable(TableIdentifier.of("sales", "orders"), schema).withLocation("s3a://lake/wh/sales/orders").create()
        val cached = CachingCatalog.wrap(backend)
        val reader = IcebergCatalogReader("analytics", cached)
        reader.readObject(listOf("sales", "orders"))

        table.updateProperties().set("write.data.path", "s3a://lake/moved/orders").commit()
        cached.loadTable(TableIdentifier.of("sales", "orders"))
        val again = reader.readObject(listOf("sales", "orders"))!!

        assertTrue(again.locations.any { it.role == LocationRole.WRITE_PATH && it.location == "s3a://lake/moved/orders" }, again.locations.toString())
    }

    @Test
    fun `an external REST catalog is read through Spark's own client, so its token is sent, and its cache is bypassed`() {
        val server = externalServer()
        server.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://ext/sales"))
        val table = server.backend.buildTable(TableIdentifier.of("sales", "orders"), schema).withLocation("s3a://ext/sales/orders").create()
        val sparkClient = sparkCatalogClient(server, "view-endpoints-supported" to "true")
        val service = externalService(server, sparkClient)

        assertEquals(mapOf("sales" to "s3a://ext/sales"), service.visibleDatabaseLocations("ext"))
        assertEquals(listOf("s3a://ext/sales/orders"), service.discoverDatabase("ext", "sales").fileTableLocations)
        // A change after Spark's cache loaded the table is still seen.
        sparkClient.loadTable(TableIdentifier.of("sales", "orders"))
        table.updateProperties().set("write.data.path", "s3a://ext/moved/orders").commit()
        assertTrue("s3a://ext/moved/orders" in service.discoverDatabase("ext", "sales").tables.single().writeLocations)

        assertTrue(server.requests.isNotEmpty())
        assertTrue(server.requests.all { it.authorization == "Bearer $EXTERNAL_TOKEN" }, server.requests.map { it.authorization }.toString())
        assertNull(RestCatalogReader.clientProperties("ext", mapOf("type" to "rest", "uri" to server.uri)))
    }

    @Test
    fun `an external REST catalog whose views can't be listed fails the database read`() {
        val server = externalServer()
        server.backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://ext/sales"))
        val service = externalService(server, sparkCatalogClient(server))

        val error = assertThrows<IllegalStateException> { service.discoverDatabase("ext", "sales") }

        assertTrue("missing_namespace_listed_views" in error.message.orEmpty(), error.message)
    }

    @Test
    fun `an external REST namespace without a location has no known folder`() {
        val server = externalServer()
        server.backend.createNamespace(Namespace.of("sales"))
        val service = externalService(server, sparkCatalogClient(server, "view-endpoints-supported" to "true"))

        assertEquals(mapOf("sales" to null), service.visibleDatabaseLocations("ext"))
    }

    private fun externalServer(): TestRestCatalogServer {
        val backend = InMemoryCatalog().apply { initialize("backend", mapOf("warehouse" to "s3a://ext")) }
        return TestRestCatalogServer(backend, "ext", basePath = "/api/iceberg").also { closeables += it }
    }

    // The client SparkCatalog builds from the session's settings, with its table cache.
    private fun sparkCatalogClient(
        server: TestRestCatalogServer,
        vararg extra: Pair<String, String>,
    ): CachingCatalog {
        val client =
            RESTCatalog().apply {
                setConf(Configuration())
                initialize("ext", mapOf("uri" to server.uri, "token" to EXTERNAL_TOKEN) + extra)
            }
        closeables += client
        return CachingCatalog.wrap(client) as CachingCatalog
    }

    private fun externalService(
        server: TestRestCatalogServer,
        sparkClient: CachingCatalog,
    ): CatalogDiscoveryService {
        sessionConf["spark.sql.catalog.ext"] = "org.apache.iceberg.spark.SparkCatalog"
        sessionConf["spark.sql.catalog.ext.type"] = "rest"
        sessionConf["spark.sql.catalog.ext.uri"] = server.uri
        sessionConf["spark.sql.catalog.ext.token"] = EXTERNAL_TOKEN
        val plugin = mockk<SparkCatalog> { every { icebergCatalog() } returns sparkClient }
        val spark = mockk<SparkSession>()
        every { spark.conf() } returns sessionConfigOf(sessionConf)
        every { spark.sessionState().catalogManager().catalog("ext") } returns plugin
        return CatalogDiscoveryService().apply {
            sparkSessionProvider = mockk<SparkSessionProvider> { every { getOrCreate() } returns spark }
            closeables += AutoCloseable { close() }
        }
    }

    private class FakeSource(
        private val namespaces: List<List<String>>,
        private val database: CatalogReadResult? = null,
        private val listingGaps: List<ReadGap> = emptyList(),
        private val gapsOnObject: List<ReadGap> = emptyList(),
    ) : CatalogSource {
        override fun readDatabases() = CatalogReadResult("spark_catalog", emptyList(), emptyList(), emptyList(), listingGaps, namespaces)

        override fun readDatabase(levels: List<String>) = database

        override fun readObject(name: List<String>) = CatalogReadResult("spark_catalog", emptyList(), emptyList(), emptyList(), gapsOnObject)
    }

    private fun read(vararg locations: OwnedLocation) =
        CatalogReadResult("spark_catalog", locations.toList(), emptyList(), emptyList(), emptyList(), listOf(listOf("sales")))

    private fun hms() = HmsTestCatalog(dir)

    private fun server(catalogName: String = "spark_catalog"): TestRestCatalogServer {
        val backend = InMemoryCatalog().apply { initialize("backend", mapOf("warehouse" to "s3a://lake/wh")) }
        return TestRestCatalogServer(backend, catalogName).also { closeables += it }
    }

    private fun rest(server: TestRestCatalogServer): CatalogSource {
        val opened =
            RestCatalogReader.open(server.catalogName, mapOf("type" to "rest", "uri" to server.uri, "warehouse" to "s3a://lake/wh"), Configuration())
                as RestCatalogReader.Opened.Ready
        closeables += opened
        return IcebergSource(opened.reader)
    }

    private fun service(
        hms: HmsTestCatalog,
        server: TestRestCatalogServer,
    ): CatalogDiscoveryService {
        val sources = listOf(HmsSource(HmsCatalogReader("spark_catalog", hms.catalog, null)), rest(server))
        return service(sources = { sources })
    }

    private fun service(sources: ((String) -> List<CatalogSource>)? = null) =
        CatalogDiscoveryService().apply {
            sparkSessionProvider =
                mockk<SparkSessionProvider>().also { provider ->
                    val spark = mockk<SparkSession>()
                    every { spark.conf() } returns sessionConfigOf(sessionConf)
                    every { provider.getOrCreate() } returns spark
                }
            catalogSources = sources
        }

    private fun configureSessionCatalog(uri: String) {
        sessionConf["spark.sql.catalog.spark_catalog"] = "org.apache.iceberg.spark.SparkSessionCatalog"
        sessionConf["spark.sql.catalog.spark_catalog.type"] = "rest"
        sessionConf["spark.sql.catalog.spark_catalog.uri"] = uri
        sessionConf["spark.sql.catalog.spark_catalog.warehouse"] = "s3a://lake/wh"
    }
}
