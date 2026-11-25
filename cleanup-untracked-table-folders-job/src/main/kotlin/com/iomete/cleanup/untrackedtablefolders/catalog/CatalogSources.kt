package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.catalog.readers.CatalogReadResult
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.HmsCatalogReader
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.IcebergCatalogReader

/** One store behind a Spark catalog. spark_catalog has two: the Hive metastore and, when it is REST, the REST catalog. */
interface CatalogSource {
    /** Namespaces and their locations, without tables. */
    fun readDatabases(): CatalogReadResult

    /** One namespace with its tables and views, or null when this store does not have it. */
    fun readDatabase(levels: List<String>): CatalogReadResult?

    /** One table or view, or null when this store does not have it. [name] is the namespace levels and the object name. */
    fun readObject(name: List<String>): CatalogReadResult?
}

internal class HmsSource(
    private val reader: HmsCatalogReader,
) : CatalogSource {
    override fun readDatabases() = reader.readDatabases()

    // The metastore has one level of databases, so a nested namespace is never one of its databases.
    override fun readDatabase(levels: List<String>) = levels.singleOrNull()?.let { reader.readDatabase(it) }

    override fun readObject(name: List<String>) = name.takeIf { it.size == 2 }?.let { (database, table) -> reader.readTable(database, table) }
}

internal class IcebergSource(
    private val reader: IcebergCatalogReader,
) : CatalogSource {
    override fun readDatabases() = reader.readNamespaces()

    override fun readDatabase(levels: List<String>) = reader.readNamespace(levels)

    override fun readObject(name: List<String>) = reader.readObject(name)
}

/** A catalog the readers can't read. Its storage is unknown, so it never counts as read. */
class UnsupportedCatalogException(
    catalog: String,
    reason: String,
) : IllegalStateException("Catalog=$catalog cannot be read for ownership checks: $reason")
