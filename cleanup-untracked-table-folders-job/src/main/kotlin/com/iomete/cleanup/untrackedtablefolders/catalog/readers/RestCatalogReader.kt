package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogReadPool
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CatalogProperties
import org.apache.iceberg.rest.RESTCatalog

/**
 * Reads a platform-managed REST catalog with the job's own uncached [RESTCatalog], not Spark's cached one.
 *
 * Only an allowlist of the session's catalog properties is passed on. Property values are never logged or
 * returned.
 */
object RestCatalogReader {
    internal val COPIED_PROPERTIES = listOf(CatalogProperties.URI, CatalogProperties.WAREHOUSE_LOCATION)
    private const val TIMEOUT_MS = "60000"

    sealed interface Opened {
        /** A live client. Close it after the last read, including manifest reads of tables it loaded. */
        class Ready internal constructor(private val client: RESTCatalog, val reader: IcebergCatalogReader) : Opened, AutoCloseable {
            override fun close() = client.close()
        }

        data class Refused(val result: CatalogReadResult) : Opened
    }

    /** [catalogProperties] are the session's spark.sql.catalog.<name>.* entries, without that prefix. */
    fun open(
        catalogName: String,
        catalogProperties: Map<String, String>,
        hadoopConf: Configuration,
        viewPathParser: ViewPathParser = ViewPathParser(),
        limits: ReaderLimits = ReaderLimits(),
        pool: CatalogReadPool? = null,
    ): Opened {
        val properties = clientProperties(catalogName, catalogProperties)
            ?: return Opened.Refused(CatalogReadResult.failed(catalogName, ReadGap(emptyList(), ReadGapKind.UNSUPPORTED, "not_a_managed_rest_catalog")))
        val client = RESTCatalog()
        try {
            client.setConf(hadoopConf)
            client.initialize(catalogName, properties)
        } catch (e: Exception) {
            runCatching { client.close() }
            return Opened.Refused(CatalogReadResult.failed(catalogName, gapFor(emptyList(), "rest_catalog_config_failed", e)))
        }
        return Opened.Ready(client, IcebergCatalogReader(catalogName, client, viewPathParser, limits, pool = pool))
    }

    /** Opens, reads and closes. */
    fun read(
        catalogName: String,
        catalogProperties: Map<String, String>,
        hadoopConf: Configuration,
        viewPathParser: ViewPathParser = ViewPathParser(),
        limits: ReaderLimits = ReaderLimits(),
    ): CatalogReadResult = when (val opened = open(catalogName, catalogProperties, hadoopConf, viewPathParser, limits)) {
        is Opened.Refused -> opened.result
        is Opened.Ready -> opened.use { it.reader.read() }
    }

    /** Null unless the catalog is type rest, has no catalog-impl, and points at /internal/catalogs/<name>. */
    internal fun clientProperties(catalogName: String, catalogProperties: Map<String, String>): Map<String, String>? {
        if (!catalogProperties["type"].equals("rest", ignoreCase = true)) return null
        if (catalogProperties.containsKey(CatalogProperties.CATALOG_IMPL)) return null
        val uri = catalogProperties[CatalogProperties.URI]?.let { runCatching { URI(it.trim()) }.getOrNull() } ?: return null
        if (uri.scheme?.lowercase() !in setOf("http", "https") || uri.host.isNullOrBlank()) return null
        if (uri.path?.trimEnd('/') != "/internal/catalogs/$catalogName") return null

        return COPIED_PROPERTIES.mapNotNull { key -> catalogProperties[key]?.let { key to it } }.toMap() + mapOf(
            // Server defaults can page. This asks for one page; the client still follows next-page tokens.
            "rest-page-size" to Int.MAX_VALUE.toString(),
            // Asks for views even when the server's config lists no endpoints. The missing-namespace probe
            // catches a server that ignores it.
            "view-endpoints-supported" to "true",
            CatalogProperties.FILE_IO_IMPL to "org.apache.iceberg.hadoop.HadoopFileIO",
            // A stalled server must fail the read, which then blocks deletion, not hang the run.
            "rest.client.connection-timeout-ms" to TIMEOUT_MS,
            "rest.client.socket-timeout-ms" to TIMEOUT_MS,
        )
    }
}
