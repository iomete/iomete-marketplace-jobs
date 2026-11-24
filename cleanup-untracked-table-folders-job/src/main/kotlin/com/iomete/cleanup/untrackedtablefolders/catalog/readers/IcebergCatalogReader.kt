package com.iomete.cleanup.untrackedtablefolders.catalog.readers

import java.util.UUID
import org.apache.iceberg.HasTableOperations
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.Namespace
import org.apache.iceberg.catalog.SupportsNamespaces
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.catalog.ViewCatalog
import org.apache.iceberg.exceptions.NoSuchNamespaceException
import org.apache.iceberg.spark.SparkCatalog
import org.apache.iceberg.view.BaseView
import org.apache.iceberg.view.SQLViewRepresentation
import org.apache.spark.sql.connector.catalog.CatalogPlugin

/**
 * Reads every namespace, table and view of an Iceberg catalog through the Iceberg API.
 *
 * Missing-namespace errors are never treated as "no tables" here: a namespace that was listed and then can't
 * be read is a gap.
 */
class IcebergCatalogReader(
    private val catalogName: String,
    private val catalog: Catalog,
    private val viewPathParser: ViewPathParser = ViewPathParser(),
    private val limits: ReaderLimits = ReaderLimits(),
    private val probeNamespace: Namespace = Namespace.of("cleanup_probe_" + UUID.randomUUID().toString().replace("-", "")),
) {
    fun read(): CatalogReadResult {
        val namespaces = catalog as? SupportsNamespaces
            ?: return CatalogReadResult.failed(catalogName, ReadGap(emptyList(), ReadGapKind.UNSUPPORTED, "catalog_lists_no_namespaces"))
        val result = ReadAccumulator(catalogName)
        val all = try {
            listAllNamespaces(namespaces, result)
        } catch (e: Exception) {
            return CatalogReadResult.failed(catalogName, gapFor(emptyList(), "list_namespaces_failed", e))
        }
        probeMissingNamespace(result)
        all.forEach { readNamespace(namespaces, it, result) }
        return result.result()
    }

    /** Every namespace and its location, without tables. */
    fun readNamespaces(): CatalogReadResult {
        val namespaces = catalog as? SupportsNamespaces
            ?: return CatalogReadResult.failed(catalogName, ReadGap(emptyList(), ReadGapKind.UNSUPPORTED, "catalog_lists_no_namespaces"))
        val result = ReadAccumulator(catalogName)
        val all = try {
            listAllNamespaces(namespaces, result)
        } catch (e: Exception) {
            return CatalogReadResult.failed(catalogName, gapFor(emptyList(), "list_namespaces_failed", e))
        }
        all.forEach { readNamespaceLocation(namespaces, it, result) }
        return result.result()
    }

    /** One namespace with its tables and views, or null when it does not exist. */
    fun readNamespace(name: List<String>): CatalogReadResult? {
        val namespaces = catalog as? SupportsNamespaces
            ?: return CatalogReadResult.failed(catalogName, ReadGap(emptyList(), ReadGapKind.UNSUPPORTED, "catalog_lists_no_namespaces"))
        val result = ReadAccumulator(catalogName)
        val namespace = Namespace.of(*name.toTypedArray())
        try {
            if (!namespaces.namespaceExists(namespace)) return null
        } catch (e: Exception) {
            result.gap(name, "load_namespace_failed", e)
            return result.result()
        }
        probeMissingNamespace(result)
        readNamespace(namespaces, namespace, result)
        return result.result()
    }

    /** One table or view, or null when neither exists. [name] is the namespace levels and the object name. */
    fun readObject(name: List<String>): CatalogReadResult? {
        val result = ReadAccumulator(catalogName)
        val identifier = TableIdentifier.of(*name.toTypedArray())
        val views = catalog as? ViewCatalog
        try {
            val namespace = identifier.namespace()
            if ((catalog as? SupportsNamespaces)?.namespaceExists(namespace) == false) return null
            when {
                catalog.tableExists(identifier) -> readTable(identifier, result)
                views != null && views.viewExists(identifier) -> readView(views, identifier, result)
                else -> return null
            }
        } catch (e: Exception) {
            result.gap(name, "load_table_failed", e)
        }
        return result.result()
    }

    /** For a manifest read of one table found by [read]. */
    fun loadTable(name: List<String>): Table {
        val identifier = TableIdentifier.of(*name.toTypedArray())
        catalog.invalidateTable(identifier)
        return catalog.loadTable(identifier)
    }

    // Children are listed with parent=, so nested and dotted namespaces are read too.
    private fun listAllNamespaces(catalog: SupportsNamespaces, result: ReadAccumulator): List<Namespace> {
        val found = linkedSetOf<Namespace>()
        val pending = ArrayDeque(catalog.listNamespaces(Namespace.empty()).map { it to 1 })
        while (pending.isNotEmpty()) {
            val (namespace, depth) = pending.removeFirst()
            val name = namespace.levels().toList()
            if (namespace.length() != depth || found.contains(namespace)) {
                result.gaps += ReadGap(name, ReadGapKind.UNVERIFIED, "unexpected_namespace_listing")
                continue
            }
            if (found.size >= limits.maxNamespaces) {
                result.gaps += ReadGap(emptyList(), ReadGapKind.LIMIT_EXCEEDED, "too_many_namespaces")
                break
            }
            found += namespace
            if (depth >= limits.maxNamespaceDepth) {
                result.gaps += ReadGap(name, ReadGapKind.LIMIT_EXCEEDED, "namespace_too_deep")
                continue
            }
            try {
                catalog.listNamespaces(namespace).forEach { child ->
                    if (child.levels().take(depth) == name) {
                        pending.addLast(child to depth + 1)
                    } else {
                        result.gaps += ReadGap(child.levels().toList(), ReadGapKind.UNVERIFIED, "unexpected_namespace_listing")
                    }
                }
            } catch (e: Exception) {
                result.gap(name, "list_child_namespaces_failed", e)
            }
        }
        return found.toList()
    }

    // A server that answers an unknown namespace with an empty list could also answer a real one that way.
    // The REST client also returns no views, without asking, when the server doesn't advertise view endpoints.
    // The server's answer does not change within a run, so a reader probes once and every later read gets the gaps.
    private val probeGaps: List<ReadGap> by lazy {
        val probeResult = ReadAccumulator(catalogName)
        probe(probeResult, "tables") { catalog.listTables(probeNamespace) }
        (catalog as? ViewCatalog)?.let { views -> probe(probeResult, "views") { views.listViews(probeNamespace) } }
        probeResult.gaps.toList()
    }

    private fun probeMissingNamespace(result: ReadAccumulator) {
        result.gaps += probeGaps
    }

    private fun probe(result: ReadAccumulator, kind: String, list: () -> List<TableIdentifier>) {
        try {
            list()
            result.gaps += ReadGap(emptyList(), ReadGapKind.UNVERIFIED, "missing_namespace_listed_$kind")
        } catch (_: NoSuchNamespaceException) {
            // Expected.
        } catch (e: Exception) {
            result.gap(emptyList(), "missing_namespace_probe_failed", e)
        }
    }

    private fun readNamespaceLocation(catalog: SupportsNamespaces, namespace: Namespace, result: ReadAccumulator) {
        val name = namespace.levels().toList()
        result.namespaces += name
        try {
            result.location(name, LocationRole.NAMESPACE, catalog.loadNamespaceMetadata(namespace)["location"])
        } catch (e: Exception) {
            result.gap(name, "load_namespace_failed", e)
        }
    }

    private fun readNamespace(catalog: SupportsNamespaces, namespace: Namespace, result: ReadAccumulator) {
        val name = namespace.levels().toList()
        readNamespaceLocation(catalog, namespace, result)
        try {
            this.catalog.listTables(namespace).forEach { readTable(it, result) }
        } catch (e: Exception) {
            result.gap(name, "list_tables_failed", e)
        }
        val views = this.catalog as? ViewCatalog ?: return
        try {
            views.listViews(namespace).forEach { readView(views, it, result) }
        } catch (e: Exception) {
            result.gap(name, "list_views_failed", e)
        }
    }

    private fun readTable(identifier: TableIdentifier, result: ReadAccumulator) {
        val name = identifier.namespace().levels().toList() + identifier.name()
        try {
            val table = loadTable(name)
            val metadata = (table as? HasTableOperations)?.operations()?.current()
            if (metadata == null) {
                result.gaps += ReadGap(name, ReadGapKind.UNSUPPORTED, "table_metadata_not_available")
                return
            }
            result.icebergTable(name, metadata)
        } catch (e: Exception) {
            result.gap(name, "load_table_failed", e)
        }
    }

    private fun readView(views: ViewCatalog, identifier: TableIdentifier, result: ReadAccumulator) {
        val name = identifier.namespace().levels().toList() + identifier.name()
        try {
            val view = views.loadView(identifier)
            result.location(name, LocationRole.VIEW, view.location())
            result.liveIceberg += LiveIcebergObject(
                name = name,
                uuid = view.uuid()?.toString(),
                isView = true,
                location = view.location(),
                metadataFileLocation = (view as? BaseView)?.operations()?.current()?.metadataFileLocation(),
                referencedFiles = emptyList(),
                nameMapped = false,
            )
            val sql = view.currentVersion().representations().filterIsInstance<SQLViewRepresentation>()
                .firstOrNull { it.dialect().equals("spark", ignoreCase = true) }
            if (sql == null) {
                result.externalPathUse += ExternalPathUse(name, emptyList(), "view_has_no_spark_sql")
            } else {
                viewPathParser.pathUse(name, sql.sql())?.let { result.externalPathUse += it }
            }
        } catch (e: Exception) {
            result.gap(name, "load_view_failed", e)
        }
    }

    companion object {
        /**
         * The Iceberg catalog behind a non-session Spark catalog, or null. spark_catalog's SparkSessionCatalog is
         * not a SparkCatalog: it merges two stores and must never answer an ownership read.
         */
        fun icebergCatalogOf(plugin: CatalogPlugin): Catalog? = (plugin as? SparkCatalog)?.icebergCatalog()
    }
}
