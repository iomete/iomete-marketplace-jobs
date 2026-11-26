package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogReadPool
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Schema
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.apache.iceberg.types.Types
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class ParallelIcebergReadTest {
    private val closeables = mutableListOf<AutoCloseable>()
    private val schema = Schema(Types.NestedField.required(1, "id", Types.LongType.get()))
    private val pool = CatalogReadPool(8, "parallel-test").also { closeables += it }

    @AfterEach
    fun close() = closeables.reversed().forEach { it.close() }

    @Test
    fun `a parallel namespace read of a REST catalog equals a sequential one, failures and all`() {
        val server = server()
        tree(server)
        // The child listing of d and the namespace of e fail; both stay named gaps.
        server.fault = { path ->
            when {
                path == "namespaces" && server.requests.last().query["parent"] == "d" -> 503
                path == "namespaces/e" -> 500
                else -> null
            }
        }

        val sequential = reader(server, pool = null).readNamespaces()
        val parallel = reader(server, pool).readNamespaces()

        assertEquals(sequential, parallel)
        assertTrue(ReadGap(listOf("d"), ReadGapKind.FAILED, "list_child_namespaces_failed", "ServiceUnavailableException") in parallel.gaps, parallel.gaps.toString())
        assertTrue(ReadGap(listOf("e"), ReadGapKind.FAILED, "load_namespace_failed", "ServiceFailureException") in parallel.gaps, parallel.gaps.toString())
        assertEquals(listOf(listOf("a"), listOf("d"), listOf("e"), listOf("a", "b"), listOf("e", "f"), listOf("a", "b", "c")), parallel.namespaces)
    }

    @Test
    fun `a full parallel read equals a sequential one`() {
        val server = server()
        tree(server)

        assertEquals(reader(server, pool = null).read(), reader(server, pool).read())
    }

    @Test
    fun `namespace and depth limits give the same gaps in parallel`() {
        val server = server()
        tree(server)
        val limits = listOf(ReaderLimits(maxNamespaces = 4), ReaderLimits(maxNamespaceDepth = 2))

        limits.forEach { limit ->
            val sequential = reader(server, pool = null, limits = limit).readNamespaces()
            val parallel = reader(server, pool, limits = limit).readNamespaces()

            assertEquals(sequential, parallel)
            assertTrue(parallel.gaps.any { it.kind == ReadGapKind.LIMIT_EXCEEDED }, parallel.gaps.toString())
        }
    }

    @Test
    fun `a catalog other than REST is never read in parallel`() {
        val backend = InMemoryCatalog().apply { initialize("backend", mapOf("warehouse" to "s3a://lake/wh")) }
        (1..5).forEach { backend.createNamespace(Namespace.of("n$it"), mapOf("location" to "s3a://lake/wh/n$it")) }
        val callers = java.util.Collections.synchronizedSet(mutableSetOf<String>())
        val recording = object : InMemoryCatalog() {
            override fun loadNamespaceMetadata(namespace: Namespace): Map<String, String> {
                callers += Thread.currentThread().name
                return backend.loadNamespaceMetadata(namespace)
            }

            override fun listNamespaces(namespace: Namespace): List<Namespace> = backend.listNamespaces(namespace)
        }

        val result = IcebergCatalogReader("analytics", recording, pool = pool).readNamespaces()

        assertEquals(5, result.namespaces.size)
        assertEquals(setOf(Thread.currentThread().name), callers.toSet())
    }

    private fun tree(server: TestRestCatalogServer) {
        val backend = server.backend
        listOf(listOf("a"), listOf("a", "b"), listOf("a", "b", "c"), listOf("d"), listOf("e"), listOf("e", "f")).forEach { levels ->
            backend.createNamespace(Namespace.of(*levels.toTypedArray()), mapOf("location" to "s3a://lake/wh/${levels.joinToString("/")}"))
        }
        backend.buildTable(TableIdentifier.of(Namespace.of("a", "b"), "t1"), schema).withLocation("s3a://lake/wh/a/b/t1").create()
        backend.buildTable(TableIdentifier.of("d", "t2"), schema).withLocation("s3a://lake/wh/d/t2").create()
    }

    private fun server(): TestRestCatalogServer {
        val backend = InMemoryCatalog().apply { initialize("backend", mapOf("warehouse" to "s3a://lake/wh")) }
        return TestRestCatalogServer(backend).also { closeables += it }
    }

    private fun reader(
        server: TestRestCatalogServer,
        pool: CatalogReadPool?,
        limits: ReaderLimits = ReaderLimits(),
    ): IcebergCatalogReader {
        val opened =
            RestCatalogReader.open(
                server.catalogName,
                mapOf("type" to "rest", "uri" to server.uri, "warehouse" to "s3a://lake/wh"),
                Configuration(),
                limits = limits,
                pool = pool,
            ) as RestCatalogReader.Opened.Ready
        closeables += opened
        return opened.reader
    }
}
