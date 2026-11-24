package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.concurrent.CopyOnWriteArrayList
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.exceptions.NoSuchNamespaceException
import org.apache.iceberg.exceptions.NoSuchTableException
import org.apache.iceberg.exceptions.NoSuchViewException
import org.apache.iceberg.inmemory.InMemoryCatalog
import org.apache.iceberg.inmemory.InMemoryFileIO
import org.apache.iceberg.rest.CatalogHandlers
import org.apache.iceberg.rest.Endpoint
import org.apache.iceberg.rest.RESTUtil
import org.apache.iceberg.rest.responses.ConfigResponse
import org.apache.iceberg.rest.responses.ErrorResponse
import org.apache.iceberg.rest.responses.ListTablesResponse
import org.apache.iceberg.rest.responses.LoadTableResponse
import org.apache.iceberg.shaded.com.fasterxml.jackson.databind.ObjectMapper

data class RecordedRequest(val path: String, val query: Map<String, String>, val authorization: String?)

/** A REST catalog server over Iceberg's own request handlers, for the real RESTCatalog client to call. */
class TestRestCatalogServer(
    val backend: InMemoryCatalog,
    val catalogName: String = "spark_catalog",
    private val endpoints: List<Endpoint>? = null,
    private val lenientMissingNamespace: Boolean = false,
) : AutoCloseable {
    val requests = CopyOnWriteArrayList<RecordedRequest>()

    /** Returns a status to fail a request with, from its path below /v1. */
    @Volatile var fault: (String) -> Int? = { null }

    private val server = HttpServer.create(InetSocketAddress("127.0.0.1", 0), 0).apply {
        createContext("/") { exchange -> handle(exchange) }
        start()
    }

    val uri: String get() = "http://127.0.0.1:${server.address.port}/internal/catalogs/$catalogName"

    override fun close() = server.stop(0)

    private fun handle(exchange: HttpExchange) {
        val prefix = "/internal/catalogs/$catalogName/v1/"
        val rawPath = exchange.requestURI.rawPath
        val query = exchange.requestURI.rawQuery.orEmpty().split('&').filter { it.contains('=') }
            .associate { it.substringBefore('=') to URLDecoder.decode(it.substringAfter('='), StandardCharsets.UTF_8) }
        val path = rawPath.removePrefix(prefix)
        requests += RecordedRequest(path, query, exchange.requestHeaders.getFirst("Authorization"))
        try {
            if (!rawPath.startsWith(prefix) || exchange.requestMethod != "GET") {
                return error(exchange, 400, "BadRequestException", "unexpected request")
            }
            fault(path)?.let { status -> return error(exchange, status, "ServiceFailureException", "injected SECRET-MARKER") }
            ok(exchange, route(path.split('/').map { RESTUtil.decodeNamespace(it) to it }, query))
        } catch (e: NoSuchNamespaceException) {
            error(exchange, 404, "NoSuchNamespaceException", "missing")
        } catch (e: NoSuchTableException) {
            error(exchange, 404, "NoSuchTableException", "missing")
        } catch (e: NoSuchViewException) {
            error(exchange, 404, "NoSuchViewException", "missing")
        }
    }

    private fun route(segments: List<Pair<Namespace, String>>, query: Map<String, String>): Any {
        val raw = segments.map { it.second }
        val namespace = segments.getOrNull(1)?.first
        return when {
            raw == listOf("config") -> ConfigResponse.builder().apply { endpoints?.let { withEndpoints(it) } }.build()
            raw == listOf("namespaces") -> {
                val parent = query["parent"]?.let { Namespace.of(*RESTUtil.NAMESPACE_SPLITTER.splitToList(it).toTypedArray()) }
                CatalogHandlers.listNamespaces(backend, parent ?: Namespace.empty())
            }
            raw.size == 2 -> CatalogHandlers.loadNamespace(backend, namespace!!)
            raw.size == 3 && raw[2] == "tables" -> listOrLenient(namespace!!) { CatalogHandlers.listTables(backend, namespace) }
            raw.size == 3 && raw[2] == "views" -> listOrLenient(namespace!!) { CatalogHandlers.listViews(backend, namespace) }
            raw.size == 4 && raw[2] == "tables" -> {
                val loaded = CatalogHandlers.loadTable(backend, TableIdentifier.of(namespace!!, RESTUtil.decodeString(raw[3])))
                // The backend writes to InMemoryFileIO, so the client is told to read the table's files there.
                LoadTableResponse.builder().withTableMetadata(loaded.tableMetadata())
                    .addConfig("io-impl", InMemoryFileIO::class.java.name).build()
            }
            raw.size == 4 && raw[2] == "views" ->
                CatalogHandlers.loadView(backend, TableIdentifier.of(namespace!!, RESTUtil.decodeString(raw[3])))
            else -> throw IllegalArgumentException("no route")
        }
    }

    private fun listOrLenient(namespace: Namespace, list: () -> ListTablesResponse): ListTablesResponse =
        if (lenientMissingNamespace && !backend.namespaceExists(namespace)) ListTablesResponse.builder().build() else list()

    private fun ok(exchange: HttpExchange, body: Any) = send(exchange, 200, MAPPER.writeValueAsString(body))

    private fun error(exchange: HttpExchange, status: Int, type: String, message: String) =
        send(exchange, status, MAPPER.writeValueAsString(ErrorResponse.builder().responseCode(status).withType(type).withMessage(message).build()))

    private fun send(exchange: HttpExchange, status: Int, body: String) {
        val bytes = body.toByteArray(StandardCharsets.UTF_8)
        exchange.responseHeaders.add("Content-Type", "application/json")
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    private companion object {
        // The client's own mapper configuration, so requests and responses round-trip exactly.
        val MAPPER: ObjectMapper = Class.forName("org.apache.iceberg.rest.RESTObjectMapper")
            .getDeclaredMethod("mapper").apply { isAccessible = true }.invoke(null) as ObjectMapper
    }
}
