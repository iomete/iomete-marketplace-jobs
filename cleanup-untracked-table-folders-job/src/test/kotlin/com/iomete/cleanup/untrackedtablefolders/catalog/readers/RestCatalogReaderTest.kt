package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import io.mockk.every
import io.mockk.mockk
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.DataFiles
import org.apache.iceberg.FileFormat
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.apache.iceberg.rest.Endpoint
import org.apache.iceberg.rest.RESTUtil
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.spark.SparkSessionCatalog
import org.apache.iceberg.types.Types
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class RestCatalogReaderTest {
    private val servers = mutableListOf<TestRestCatalogServer>()
    private val schema = Schema(Types.NestedField.required(1, "id", Types.LongType.get()))

    @AfterEach
    fun stop() = servers.forEach { it.close() }

    @Test
    fun `a complete read lists nested namespaces, tables, views and their evidence`() {
        val server = server()
        val backend = server.backend
        backend.createNamespace(Namespace.of("sales"), mapOf("location" to "s3a://lake/wh/sales"))
        backend.createNamespace(Namespace.of("sales", "eu"))
        backend.createNamespace(Namespace.of("rest_only"))
        val orders = backend.buildTable(TableIdentifier.of("sales", "orders"), schema)
            .withLocation("s3a://lake/wh/sales/orders")
            .withProperty("write.data.path", "s3a://lake/elsewhere/orders_data")
            .create()
        backend.buildTable(TableIdentifier.of(Namespace.of("sales", "eu"), "vat"), schema).withLocation("s3a://lake/wh/sales/vat").create()
        backend.buildTable(TableIdentifier.of("rest_only", "imported"), schema)
            .withLocation("s3a://lake/wh/rest_only/imported")
            .withProperty("schema.name-mapping.default", "[]")
            .create()
        backend.buildView(TableIdentifier.of("sales", "raw_events"))
            .withSchema(schema)
            .withDefaultNamespace(Namespace.of("sales"))
            .withQuery("spark", "SELECT * FROM parquet.`s3a://lake/raw/events`")
            .create()

        val result = read(server)

        assertEquals(CatalogReadStatus.COMPLETE, result.status, result.gaps.toString())
        assertTrue(OwnedLocation(listOf("sales"), LocationRole.NAMESPACE, "s3a://lake/wh/sales") in result.locations)
        assertTrue(OwnedLocation(listOf("sales", "eu", "vat"), LocationRole.TABLE, "s3a://lake/wh/sales/vat") in result.locations)
        assertTrue(OwnedLocation(listOf("sales", "orders"), LocationRole.WRITE_PATH, "s3a://lake/elsewhere/orders_data") in result.locations)
        val ordersEvidence = result.liveIceberg.single { it.name == listOf("sales", "orders") }
        assertEquals(orders.uuid().toString(), ordersEvidence.uuid)
        assertTrue(ordersEvidence.metadataFileLocation!!.startsWith("s3a://lake/wh/sales/orders/metadata/"))
        assertFalse(ordersEvidence.nameMapped)
        assertTrue(result.liveIceberg.single { it.name == listOf("rest_only", "imported") }.nameMapped)
        val view = result.liveIceberg.single { it.isView }
        assertEquals(listOf("sales", "raw_events"), view.name)
        assertTrue(view.uuid != null && view.metadataFileLocation != null)
        assertEquals(listOf(ExternalPathUse(listOf("sales", "raw_events"), listOf("s3a://lake/raw/events"))), result.externalPathUse)
    }

    @Test
    fun `an Iceberg table or write path without scheme and bucket is a gap, qualified ones are kept`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"))
        server.backend.buildTable(TableIdentifier.of("sales", "good"), schema).withLocation("s3a://lake/wh/sales/good").create()
        server.backend.buildTable(TableIdentifier.of("sales", "bare"), schema).withLocation("/warehouse/sales/bare").create()
        server.backend.buildTable(TableIdentifier.of("sales", "bare_data"), schema)
            .withLocation("s3a://lake/wh/sales/bare_data")
            .withProperty("write.data.path", "/warehouse/sales/bare_data/data")
            .create()
        // Hadoop's Path can't parse this at all.
        server.backend.buildTable(TableIdentifier.of("sales", "broken_path"), schema)
            .withLocation("s3a://lake/wh/sales/broken_path")
            .withProperty("write.data.path", "://broken")
            .create()

        val result = read(server)

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(
            setOf(
                ReadGap(listOf("sales", "bare"), ReadGapKind.UNVERIFIED, "table_location_not_qualified"),
                ReadGap(listOf("sales", "bare_data"), ReadGapKind.UNVERIFIED, "write_path_location_not_qualified"),
                ReadGap(listOf("sales", "broken_path"), ReadGapKind.UNVERIFIED, "write_path_location_not_qualified"),
            ),
            result.gaps.toSet(),
        )
        assertTrue(result.locations.none { it.location.startsWith("/warehouse") || it.location == "://broken" })
        assertTrue(OwnedLocation(listOf("sales", "good"), LocationRole.TABLE, "s3a://lake/wh/sales/good") in result.locations)
        assertTrue(OwnedLocation(listOf("sales", "bare_data"), LocationRole.TABLE, "s3a://lake/wh/sales/bare_data") in result.locations)
    }

    @Test
    fun `child namespaces are listed with the parent parameter and every list asks for one full page`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("a"))
        server.backend.createNamespace(Namespace.of("a", "b"))
        server.backend.createNamespace(Namespace.of("a", "b", "c"))

        val result = read(server)

        assertEquals(CatalogReadStatus.COMPLETE, result.status, result.gaps.toString())
        assertEquals(setOf(listOf("a"), listOf("a", "b"), listOf("a", "b", "c")), namespacesWithTablesListed(server))
        val parents = server.requests.filter { it.path == "namespaces" }.map { it.query["parent"] }
        assertEquals(listOf(null, "a", "a\u001fb", "a\u001fb\u001fc"), parents)
        val lists = server.requests.filter { it.path == "namespaces" || it.path.endsWith("/tables") || it.path.endsWith("/views") }
        assertTrue(lists.all { it.query["pageSize"] == Int.MAX_VALUE.toString() }, lists.toString())
    }

    @Test
    fun `a server that lists an unknown namespace as empty never gives a complete read`() {
        val server = server(lenientMissingNamespace = true)
        server.backend.createNamespace(Namespace.of("sales"))

        val result = read(server)

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(
            setOf("missing_namespace_listed_tables", "missing_namespace_listed_views"),
            result.gaps.filter { it.kind == ReadGapKind.UNVERIFIED }.map { it.reason }.toSet(),
        )
    }

    @Test
    fun `a server that advertises no view endpoints gives an unverified view listing, not an empty one`() {
        val server = server(
            endpoints = listOf(Endpoint.V1_LIST_NAMESPACES, Endpoint.V1_LOAD_NAMESPACE, Endpoint.V1_LIST_TABLES, Endpoint.V1_LOAD_TABLE),
        )
        server.backend.createNamespace(Namespace.of("sales"))

        val result = read(server)

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(listOf(ReadGap(emptyList(), ReadGapKind.UNVERIFIED, "missing_namespace_listed_views")), result.gaps)
    }

    @Test
    fun `one table that fails to load is a named gap and the rest is still read`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"))
        server.backend.buildTable(TableIdentifier.of("sales", "good"), schema).withLocation("s3a://lake/wh/sales/good").create()
        server.backend.buildTable(TableIdentifier.of("sales", "bad"), schema).withLocation("s3a://lake/wh/sales/bad").create()
        server.fault = { path -> if (path.endsWith("/tables/bad")) 500 else null }

        val result = read(server)

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(listOf(ReadGap(listOf("sales", "bad"), ReadGapKind.FAILED, "load_table_failed", "ServiceFailureException")), result.gaps)
        assertTrue(result.locations.any { it.name == listOf("sales", "good") })
    }

    @Test
    fun `a child namespace listing that fails leaves that namespace as a gap`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("a"))
        server.backend.createNamespace(Namespace.of("a", "b"))
        server.fault = { path -> if (path == "namespaces" && server.requests.last().query["parent"] == "a") 503 else null }

        val result = read(server)

        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(listOf(listOf("a")), result.gaps.map { it.name })
        assertEquals("list_child_namespaces_failed", result.gaps.single().reason)
    }

    @Test
    fun `a namespace listing that fails fails the whole catalog`() {
        val server = server()
        server.fault = { path -> if (path == "namespaces") 500 else null }

        val result = read(server)

        assertEquals(CatalogReadStatus.FAILED, result.status)
        assertTrue(result.locations.isEmpty() && result.liveIceberg.isEmpty())
    }

    @Test
    fun `401 and 403 are unauthorized, at the catalog and at one table`() {
        val noConfig = server().apply { fault = { path -> if (path == "config") 401 else null } }
        assertEquals(CatalogReadStatus.UNAUTHORIZED, read(noConfig).status)

        val noList = server().apply { fault = { path -> if (path == "namespaces") 403 else null } }
        assertEquals(CatalogReadStatus.UNAUTHORIZED, read(noList).status)

        val oneTable = server()
        oneTable.backend.createNamespace(Namespace.of("hr"))
        oneTable.backend.buildTable(TableIdentifier.of("hr", "salaries"), schema).withLocation("s3a://lake/wh/hr/salaries").create()
        oneTable.fault = { path -> if (path.endsWith("/tables/salaries")) 403 else null }
        val result = read(oneTable)
        assertEquals(CatalogReadStatus.PARTIAL, result.status)
        assertEquals(ReadGapKind.UNAUTHORIZED, result.gaps.single().kind)
    }

    @Test
    fun `gaps keep only class names, never server text`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"))
        server.fault = { path -> if (path == "namespaces/sales/tables") 500 else null }

        val result = read(server)

        assertFalse(result.toString().contains("SECRET-MARKER"))
        assertEquals("ServiceFailureException", result.gaps.single().causeClass)
    }

    @Test
    fun `only uri and warehouse are passed on, so session credentials never reach the server`() {
        val server = server()
        val sessionProperties = managed(server) + mapOf(
            "token" to "token-value",
            "credential" to "client:secret",
            "header.Authorization" to "Bearer header-value",
            "jdbc.password" to "jdbc-value",
        )

        val result = RestCatalogReader.read(server.catalogName, sessionProperties, Configuration())

        assertEquals(CatalogReadStatus.COMPLETE, result.status, result.gaps.toString())
        assertTrue(server.requests.isNotEmpty())
        assertTrue(server.requests.all { it.authorization == null }, server.requests.toString())
        assertFalse(server.requests.any { it.path.contains("oauth") })
        assertEquals(
            setOf("uri", "warehouse", "rest-page-size", "view-endpoints-supported", "io-impl"),
            RestCatalogReader.clientProperties(server.catalogName, sessionProperties)!!.keys,
        )
    }

    @Test
    fun `an open client serves manifest reads of the tables it listed until it is closed`() {
        val server = server()
        server.backend.createNamespace(Namespace.of("sales"))
        val imported = server.backend.buildTable(TableIdentifier.of("sales", "imported"), schema)
            .withLocation("s3a://lake/wh/sales/imported")
            .withProperty("schema.name-mapping.default", "[]")
            .create()
        imported.newAppend().appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned()).withPath("s3a://lake/raw/part-00000.parquet")
                .withFormat(FileFormat.PARQUET).withFileSizeInBytes(10).withRecordCount(1).build(),
        ).commit()

        val opened = RestCatalogReader.open(server.catalogName, managed(server), Configuration()) as RestCatalogReader.Opened.Ready
        val reader = opened.reader
        val files = opened.use {
            val result = reader.read()
            val name = result.liveIceberg.single { it.nameMapped }.name
            NameMappedManifestReader().referencedFiles(name, reader.loadTable(name), listOf("s3a://lake/raw"))
        }

        assertEquals(NameMappedManifestReader.Outcome.Read(listOf("s3a://lake/raw/part-00000.parquet")), files)
        assertThrows(Exception::class.java) { reader.loadTable(listOf("sales", "imported")) }
    }

    @Test
    fun `catalogs that are not platform-managed REST catalogs are refused without a request`() {
        val server = server()
        val base = managed(server)
        val refused = listOf(
            base + ("type" to "hive"),
            base - "type",
            base + ("catalog-impl" to "org.apache.iceberg.jdbc.JdbcCatalog"),
            base + ("uri" to server.uri.replace("/internal/catalogs/", "/catalogs/")),
            base + ("uri" to server.uri.replace("spark_catalog", "other")),
            base + ("uri" to server.uri.replace("http://", "file://")),
            base - "uri",
        )

        refused.forEach { properties ->
            val result = RestCatalogReader.read(server.catalogName, properties, Configuration())
            assertEquals(CatalogReadStatus.FAILED, result.status)
            assertEquals(ReadGapKind.UNSUPPORTED, result.gaps.single().kind)
        }
        assertTrue(server.requests.isEmpty())
    }

    @Test
    fun `the session catalog is never read through SparkSessionCatalog`() {
        val iceberg = mockk<Catalog>()
        val sparkCatalog = mockk<SparkCatalog> { every { icebergCatalog() } returns iceberg }

        assertNull(IcebergCatalogReader.icebergCatalogOf(SparkSessionCatalog<Nothing>()))
        assertNull(IcebergCatalogReader.icebergCatalogOf(mockk<CatalogPlugin>()))
        assertSame(iceberg, IcebergCatalogReader.icebergCatalogOf(sparkCatalog))
    }

    private fun server(endpoints: List<Endpoint>? = null, lenientMissingNamespace: Boolean = false): TestRestCatalogServer {
        val backend = InMemoryCatalog().apply { initialize("backend", mapOf("warehouse" to "s3a://lake/wh")) }
        return TestRestCatalogServer(backend, endpoints = endpoints, lenientMissingNamespace = lenientMissingNamespace)
            .also { servers += it }
    }

    private fun managed(server: TestRestCatalogServer) =
        mapOf("type" to "rest", "uri" to server.uri, "warehouse" to "s3a://lake/wh")

    private fun read(server: TestRestCatalogServer) = RestCatalogReader.read(server.catalogName, managed(server), Configuration())

    private fun namespacesWithTablesListed(server: TestRestCatalogServer): Set<List<String>> =
        server.requests.filter { it.path.endsWith("/tables") }
            .map { RESTUtil.decodeNamespace(it.path.removePrefix("namespaces/").removeSuffix("/tables")).levels().toList() }
            .filterNot { it.first().startsWith("cleanup_probe_") }
            .toSet()
}
