package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.catalog.readers.CatalogReadResult
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.HmsCatalogReader
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.IcebergCatalogReader
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.LiveIcebergObject
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.LocationRole
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.ReadGap
import com.iomete.cleanup.untrackedtablefolders.catalog.readers.RestCatalogReader
import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogFileSystemProvider
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogStorageProperties
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.annotation.PreDestroy
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import java.util.concurrent.ConcurrentHashMap
import org.apache.iceberg.hadoop.HadoopFileIO
import scala.collection.JavaConverters

data class DiscoveredDatabase(
    val catalog: String,
    val database: String,
    val location: String?,
    val tables: List<DiscoveredTable>,
) {
    val unresolvedTables: List<DiscoveredTable> get() = tables.filter { it.isUnresolved }

    // Only these pick and anchor cleanup roots and count toward coverage. Every table location stays protected.
    val fileTableLocations: List<String> get() = tables.filter { it.storesFiles }.mapNotNull { it.location }
}

/** What a lookup of one table by name found. */
sealed interface NamedTableStorage {
    data object NotFound : NamedTableStorage

    /** A null location is a view. */
    data class Found(
        val location: String?,
        val writeLocations: List<String> = emptyList(),
    ) : NamedTableStorage

    /** The table exists, but the storage it may use cannot be established, as for an unresolved table. */
    data class Unresolved(
        val reason: String,
    ) : NamedTableStorage
}

// status is derived, unrecognized_uri or no_warehouse; location is set only when derived.
data class RestNamespaceRoot(
    val location: String?,
    val status: String,
)

data class DiscoveredTable(
    val catalog: String,
    val database: String,
    // A label such as table-3, never the catalog's name for it: the readers also see tables the job's user may not.
    val table: String,
    val isTemporary: Boolean,
    val location: String?,
    val unresolvedReason: String? = null,
    // Every other path the object stores files in or reads: Iceberg write paths, partitions with their own location,
    // a view's folder and the paths its text reads.
    val writeLocations: List<String> = emptyList(),
    // The location read before an unresolved table failed a later check. It is not verified, so it only keeps that
    // folder out of the unmatched list and must never select a scan root or anchor ownership.
    val knownLocation: String? = null,
    val provider: String? = null,
    // Paths without scheme and bucket. Each stands for that path in any bucket, so it protects that path in the bucket
    // of every cleanup root, and never selects or anchors one.
    val unqualifiedPaths: List<String> = emptyList(),
    val isView: Boolean = false,
    // The current Iceberg metadata of a live Iceberg table or view, for the storage-side checks.
    val iceberg: LiveIcebergObject? = null,
) {
    init {
        require(knownLocation == null || unresolvedReason != null) { "knownLocation is only for unresolved tables" }
    }

    // A null location means the catalog object owns no verified folder, such as a view. Unresolved means
    // the table owns storage we could not locate, so its folder cannot be identified.
    val isUnresolved: Boolean get() = unresolvedReason != null

    // The metastore gives a JDBC table a default location, but its rows live in another database, so no folder holds them.
    val storesFiles: Boolean get() = provider?.lowercase() !in HmsCatalogReader.JDBC_PROVIDERS

    val qualifiedName: String get() = "$catalog.$database.$table"

    // Every path this object protects under [cleanupRoot]. A path that can't be compared throws, so the caller fails closed.
    fun pathsUnder(cleanupRoot: String): List<String> =
        (listOfNotNull(location, knownLocation) + writeLocations + unqualifiedPathsIn(cleanupRoot)).filter { StoragePathUtils.overlaps(it, cleanupRoot) }

    // The unqualified paths placed in the bucket of [location].
    fun unqualifiedPathsIn(location: String): List<String> =
        unqualifiedPaths.map { path ->
            requireNotNull(StoragePathUtils.inBucketOf(path, location)) { "A path without scheme and bucket has no absolute path to compare" }
        }
}

class DatabaseNotFoundException(
    val catalog: String,
    val database: String,
    cause: Throwable,
) : RuntimeException(
    "Database not found: catalog=$catalog, database=$database",
    cause,
)

private const val WAREHOUSE_PROPERTY = "warehouse"
private const val SESSION_CATALOG_NAME = "spark_catalog"
private const val SESSION_WAREHOUSE_KEY = "spark.sql.warehouse.dir"
private const val CATALOG_TYPE_PROPERTY = "type"
private const val CATALOG_URI_PROPERTY = "uri"
private const val SPARK_SESSION_CATALOG_CLASS = "org.apache.iceberg.spark.SparkSessionCatalog"
private const val SPARK_CATALOG_CLASS = "org.apache.iceberg.spark.SparkCatalog"
private val ICEBERG_CATALOG_CLASSES = setOf(SPARK_SESSION_CATALOG_CLASS, SPARK_CATALOG_CLASS)
private const val REST_CATALOG_SERVICE_HOST = "iom-rest-catalog"

// The class IOMETE sets for a JDBC catalog. It is the only catalog type the platform creates without table storage.
private const val JDBC_CATALOG_CLASS = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"

// iom-core gives computes <base>/internal/catalogs/<catalog>. iom-rest-catalog also serves the same catalogs at
// /catalogs/<catalog>, which is accepted only on its own in-cluster service host.
internal fun isManagedRestCatalogUri(
    uri: String,
    catalog: String,
): Boolean {
    val parsed = runCatching { java.net.URI(uri.trim()) }.getOrNull() ?: return false
    val path = parsed.path?.trimEnd('/') ?: return false
    if (path.endsWith("/internal/catalogs/$catalog")) {
        return true
    }

    val host = parsed.host.orEmpty().lowercase()
    val restCatalogServiceHost = host == REST_CATALOG_SERVICE_HOST || host.startsWith("$REST_CATALOG_SERVICE_HOST.")
    return restCatalogServiceHost && path.endsWith("/catalogs/$catalog")
}

/**
 * Catalog contents for ownership checks, read from each store with the catalog readers: never through Spark SQL, and
 * never through SparkSessionCatalog, which hides a Hive table behind a REST table of the same name. Every call reads
 * again; only the readers' clients are kept for the run. A store that can't be read, or a catalog type the readers
 * don't support, throws or leaves tables unresolved. It never makes storage look unowned.
 */
@ApplicationScoped
class CatalogDiscoveryService {
    private val logger = SafeLogger.getLogger(CatalogDiscoveryService::class.java)

    @Inject
    lateinit var sparkSessionProvider: SparkSessionProvider

    @Inject
    lateinit var catalogFileSystemProvider: CatalogFileSystemProvider

    // Tests set this to read in-memory catalogs.
    internal var catalogSources: ((String) -> List<CatalogSource>)? = null

    private val openSources = ConcurrentHashMap<String, List<CatalogSource>>()
    private val openRestClients = ConcurrentHashMap<String, AutoCloseable>()

    // The namespace each database name read from a catalog's listing stands for, so a nested REST namespace is read
    // again by its levels. A name that two namespaces share maps to null.
    private val namespaceLevels = ConcurrentHashMap<Pair<String, String>, NamespaceLevels>()

    private data class NamespaceLevels(val levels: List<String>?)

    // The namespaces each store of a catalog listed, by catalog and store position.
    private val listedNamespaces = ConcurrentHashMap<Pair<String, Int>, Set<List<String>>>()

    fun discoverDatabase(
        catalog: String,
        database: String,
    ): DiscoveredDatabase {
        logger.info("Discovering database metadata for catalog=$catalog database=$database")
        val levels = levelsOf(catalog, database)
        val sources = sources(catalog)
        val reads = sources.map { it.readDatabase(levels) }
        if (reads.all { it == null }) {
            throw DatabaseNotFoundException(catalog, database, IllegalStateException("no store of the catalog has this database"))
        }
        // A store that lists the database but can't read it may still hold tables, so it is not taken as absent.
        sources.withIndex().filter { reads[it.index] == null }.forEach { (index, source) ->
            val listed = listedNamespaces[catalog to index] ?: source.readDatabases().namespaces.toSet()
            check(levels !in listed) {
                "catalog=$catalog, database=$database is listed by a store of the catalog that then reports it missing"
            }
        }
        val found = reads.filterNotNull()
        requireDatabaseRead(catalog, database, levels, found)
        requireEvidenceWithin(catalog, database, levels, found) { it == levels || (it.size == levels.size + 1 && it.take(levels.size) == levels) }

        return DiscoveredDatabase(
            catalog = catalog,
            database = database,
            location = found.firstNotNullOfOrNull { namespaceLocation(it, levels) },
            tables = discoveredTables(catalog, database, levels, found),
        )
    }

    fun discoverTables(
        catalog: String,
        database: String,
    ): List<DiscoveredTable> = discoverDatabase(catalog, database).tables

    // A database whose location cannot be read maps to null. One without a scheme and bucket keeps its path.
    fun visibleDatabaseLocations(catalog: String): Map<String, String?> {
        val startedAt = System.nanoTime()
        val reads = sources(catalog).map { it.readDatabases() }
        reads.forEachIndexed { index, read -> listedNamespaces[catalog to index] = read.namespaces.toSet() }
        // A gap for a namespace the listing did not return, such as an unexpected child, hides that namespace.
        reads.flatMap { read -> read.gaps.filter { it.name.isEmpty() || it.name !in read.namespaces } }.takeIf { it.isNotEmpty() }?.let { gaps ->
            throw IllegalStateException("The databases of catalog=$catalog could not be listed: ${describe(gaps)}")
        }

        val levelsByName = linkedMapOf<String, MutableSet<List<String>>>()
        reads.flatMap { it.namespaces }.forEach { levels -> levelsByName.getOrPut(levels.joinToString(".")) { linkedSetOf() } += levels }
        logger.info("Reading the locations of ${levelsByName.size} database(s) in catalog=$catalog to protect their storage")

        val locations =
            levelsByName.entries.associate { (name, variants) ->
                val levels = variants.singleOrNull()
                namespaceLevels[catalog to name] = NamespaceLevels(levels)
                name to levels?.let { databaseLocation(catalog, name, it, sources(catalog).zip(reads)) }
            }
        logger.info(
            "Read the locations of ${locations.size} database(s) in catalog=$catalog in " +
                "${(System.nanoTime() - startedAt) / 1_000_000} ms; ${locations.count { it.value == null }} had no readable location"
        )
        return locations
    }

    // Null when the table does not exist or is a view. Any other failure is thrown so the caller can fail closed.
    fun tableLocationIfExists(
        catalog: String,
        database: String,
        table: String,
    ): String? {
        val tables = namedTables(catalog, database, table) ?: return null
        tables.firstOrNull { it.isUnresolved }?.let { throw IllegalStateException("The storage of the table could not be established: ${it.unresolvedReason}") }
        val owned = tables.filterNot { it.isView }
        if (owned.any { it.location == null && it.unqualifiedPaths.isNotEmpty() }) {
            throw IllegalStateException("The table has a location with no scheme and bucket")
        }
        return owned.mapNotNull { it.location }.distinct().let { locations ->
            check(locations.size <= 1) { "Tables of this name in more than one store have different locations" }
            locations.singleOrNull()
        }
    }

    // Reads the table as the full read of a database does, so a write path into a folder, or a setting that lets the
    // table use files outside its declared paths, counts the same. Any failure other than a missing table is thrown.
    fun tableStorageIfExists(
        catalog: String,
        database: String,
        table: String,
    ): NamedTableStorage {
        val tables = namedTables(catalog, database, table) ?: return NamedTableStorage.NotFound
        tables.firstOrNull { it.isUnresolved }?.let { return NamedTableStorage.Unresolved(it.unresolvedReason.orEmpty()) }
        val location = tables.firstNotNullOfOrNull { it.location }
        val otherPaths = tables.flatMap { listOfNotNull(it.location) + it.writeLocations + it.unqualifiedPaths }.distinct() - setOfNotNull(location)
        return NamedTableStorage.Found(location, otherPaths)
    }

    fun sharedWarehouseRoots(catalog: String): List<String> {
        val spark = sparkSessionProvider.getOrCreate()
        val catalogWarehouse = catalogProperties()["$catalog.$WAREHOUSE_PROPERTY"].orEmpty()

        // The session warehouse alone does not cover REST namespaces.
        if (catalogWarehouse.isEmpty()) {
            return emptyList()
        }

        val sessionWarehouse =
            if (catalog == SESSION_CATALOG_NAME) {
                spark.conf().get(SESSION_WAREHOUSE_KEY, "").trim()
            } else {
                ""
            }

        return listOf(catalogWarehouse, sessionWarehouse).filter { it.isNotEmpty() }.distinct()
    }

    fun knownWarehouseRoots(): List<String> {
        val spark = sparkSessionProvider.getOrCreate()
        val catalogWarehouses = catalogProperties().filterKeys { it.endsWith(".$WAREHOUSE_PROPERTY") }.values

        return (catalogWarehouses + spark.conf().get(SESSION_WAREHOUSE_KEY, "").trim())
            .filter { it.isNotEmpty() }
            .distinct()
            .sorted()
    }

    // Catalogs registered in this Spark session, by catalog name, with the warehouses they name. A catalog with no
    // warehouse setting, or a blank one, is listed with none: a REST catalog can get its warehouse from the server.
    // Only JDBC catalogs are left out, because they keep no table folders. The built-in spark_catalog always exists
    // and puts its Hive tables in the session warehouse, so it is listed with that warehouse too.
    fun sessionCatalogWarehouses(): Map<String, List<String>> {
        val properties = catalogProperties()
        val registered =
            properties
                .filter { (key, catalogClass) -> '.' !in key && catalogClass.isNotEmpty() && catalogClass != JDBC_CATALOG_CLASS }
                .keys
                .associateWith { catalog -> listOfNotNull(properties["$catalog.$WAREHOUSE_PROPERTY"]?.takeIf { it.isNotEmpty() }) }
        val sessionCatalogWarehouses =
            (registered[SESSION_CATALOG_NAME].orEmpty() + sparkSessionProvider.getOrCreate().conf().get(SESSION_WAREHOUSE_KEY, "").trim())
                .filter { it.isNotEmpty() }
                .distinct()

        return registered + (SESSION_CATALOG_NAME to sessionCatalogWarehouses)
    }

    // Settings under spark.sql.catalog., keyed by the rest of the name. Spark loads catalogs from the session config,
    // which starts as the startup SparkConf and can gain or change catalogs later, for example through IOMETE's
    // catalog sync. The startup SparkConf alone can miss a catalog this session can query.
    private fun catalogProperties(): Map<String, String> {
        val prefix = CatalogStorageProperties.CATALOG_CONF_PREFIX + "."
        return JavaConverters.mapAsJavaMapConverter(sparkSessionProvider.getOrCreate().conf().getAll()).asJava()
            .filterKeys { it.startsWith(prefix) }
            .entries
            .associate { (key, value) -> key.removePrefix(prefix) to value.trim() }
    }

    // The managed REST catalog places every table at <warehouse>/<namespace>/<table> and ignores any
    // namespace location property, so this root holds its tables even when none of them is visible.
    // Null when the catalog is not an Iceberg REST catalog at all.
    fun restNamespaceRoot(
        catalog: String,
        database: String,
    ): RestNamespaceRoot? {
        val properties = catalogProperties()

        val catalogClass = properties[catalog].orEmpty()
        val catalogType = properties["$catalog.$CATALOG_TYPE_PROPERTY"].orEmpty()
        val catalogUri = properties["$catalog.$CATALOG_URI_PROPERTY"].orEmpty()
        val warehouse = properties["$catalog.$WAREHOUSE_PROPERTY"].orEmpty().trimEnd('/')

        if (catalogClass !in ICEBERG_CATALOG_CLASSES || !catalogType.equals("rest", ignoreCase = true)) {
            return null
        }

        return when {
            !isManagedRestCatalogUri(catalogUri, catalog) -> RestNamespaceRoot(location = null, status = "unrecognized_uri")
            warehouse.isEmpty() -> RestNamespaceRoot(location = null, status = "no_warehouse")
            else -> RestNamespaceRoot(location = "$warehouse/$database", status = "derived")
        }
    }

    private fun namedTables(
        catalog: String,
        database: String,
        table: String,
    ): List<DiscoveredTable>? {
        val levels = levelsOf(catalog, database)
        val reads = sources(catalog).mapNotNull { it.readObject(levels + table) }
        if (reads.isEmpty()) {
            return null
        }
        requireDatabaseRead(catalog, database, levels, reads)
        requireEvidenceWithin(catalog, database, levels, reads) { it == levels + table }
        return discoveredTables(catalog, database, levels, reads)
    }

    private fun levelsOf(
        catalog: String,
        database: String,
    ): List<String> {
        val known = namespaceLevels[catalog to database] ?: return listOf(database)
        return known.levels ?: throw IllegalStateException("More than one namespace of catalog=$catalog is named $database")
    }

    // A failure to read the catalog or the database itself leaves every table of it unknown.
    private fun requireDatabaseRead(
        catalog: String,
        database: String,
        levels: List<String>,
        reads: List<CatalogReadResult>,
    ) {
        val failures = reads.flatMap { it.gaps }.filter { (it.name.isEmpty() || it.name == levels) && it.location == null }
        if (failures.isNotEmpty()) {
            throw IllegalStateException("catalog=$catalog, database=$database could not be read: ${describe(failures)}")
        }
    }

    // Evidence the reader filed under another name, for example a table a server lists in a namespace other than
    // the one asked for, can't be matched to this database. Dropping it would hide that table and its failures.
    private fun requireEvidenceWithin(
        catalog: String,
        database: String,
        levels: List<String>,
        reads: List<CatalogReadResult>,
        expected: (List<String>) -> Boolean,
    ) {
        val names =
            reads.flatMap { read ->
                read.locations.map { it.name } + read.liveIceberg.map { it.name } + read.externalPathUse.map { it.name } +
                    read.gaps.filter { it.name.isNotEmpty() }.map { it.name }
            }
        check(names.all(expected)) { "catalog=$catalog, database=$database returned objects outside the database that was read" }
    }

    private fun namespaceLocation(
        read: CatalogReadResult,
        levels: List<String>,
    ): String? =
        read.locations.firstOrNull { it.role == LocationRole.NAMESPACE && it.name == levels }?.location
            ?: read.gaps.firstOrNull { it.name == levels && it.location != null }?.location

    // The metastore's location when it has the database. A REST namespace without one is stored in the folder the
    // managed REST catalog derives for it. Null when a store could not read it.
    private fun databaseLocation(
        catalog: String,
        database: String,
        levels: List<String>,
        reads: List<Pair<CatalogSource, CatalogReadResult>>,
    ): String? {
        val holding = reads.filter { levels in it.second.namespaces }
        if (holding.any { (_, read) -> read.gaps.any { it.name == levels && it.location == null } }) {
            return null
        }
        holding.firstNotNullOfOrNull { namespaceLocation(it.second, levels) }?.let { return it }
        // Only the REST catalog places a namespace without a location in a derived folder. A metastore database
        // without one has no known folder.
        if (holding.any { it.first is HmsSource }) {
            return null
        }
        return restNamespaceRoot(catalog, database)?.takeIf { it.status == "derived" }?.location
    }

    private fun discoveredTables(
        catalog: String,
        database: String,
        levels: List<String>,
        reads: List<CatalogReadResult>,
    ): List<DiscoveredTable> {
        val found =
            reads.flatMap { read ->
                val names =
                    (read.locations.map { it.name } + read.liveIceberg.map { it.name } + read.externalPathUse.map { it.name } + read.gaps.map { it.name })
                        .filter { it.size == levels.size + 1 && it.take(levels.size) == levels }
                        .distinct()
                names.map { name -> name.last() to tableOf(catalog, database, name, read) }
            }
        return found.sortedBy { it.first }.mapIndexed { index, (_, table) ->
            table.copy(table = "${if (table.isView) "view" else "table"}-${index + 1}")
        }
    }

    private fun tableOf(
        catalog: String,
        database: String,
        name: List<String>,
        read: CatalogReadResult,
    ): DiscoveredTable {
        val owned = read.locations.filter { it.name == name }
        val live = read.liveIceberg.firstOrNull { it.name == name }
        val uses = read.externalPathUse.filter { it.name == name }
        val gaps = read.gaps.filter { it.name == name }
        val tableLocations = owned.filter { it.role == LocationRole.TABLE }
        val usedPaths = uses.flatMap { it.paths }
        val (qualifiedUses, unqualifiedUses) = usedPaths.partition { runCatching { StoragePathUtils.isQualifiedLocation(it) }.getOrDefault(false) }

        val reasons =
            gaps.filter { it.location == null }.map { describe(listOf(it)) } +
                uses.mapNotNull { it.unknownReason }.map { "unknown_external_storage_use:$it" } +
                unqualifiedUses.filter { StoragePathUtils.unqualifiedPath(it) == null }.map { "read_path_not_comparable" }
        val unresolved = reasons.isNotEmpty()
        val location = tableLocations.firstOrNull()?.location

        return DiscoveredTable(
            catalog = catalog,
            database = database,
            table = name.last(),
            isTemporary = false,
            location = location.takeUnless { unresolved },
            unresolvedReason = reasons.distinct().joinToString("; ").ifEmpty { null },
            writeLocations = (tableLocations.drop(1) + owned.filter { it.role != LocationRole.TABLE }).map { it.location }.plus(qualifiedUses).distinct(),
            knownLocation = location.takeIf { unresolved },
            provider = "jdbc".takeIf { tableLocations.any { !it.storesFiles } },
            unqualifiedPaths = (gaps.mapNotNull { it.location } + unqualifiedUses.filter { StoragePathUtils.unqualifiedPath(it) != null }).distinct(),
            isView = live?.isView == true || owned.any { it.role == LocationRole.VIEW },
            iceberg = live,
        )
    }

    // Gap reasons and error class names only. Messages can carry server text, paths or credentials.
    private fun describe(gaps: List<ReadGap>): String =
        gaps.joinToString(", ") { gap -> "${gap.reason} (${gap.kind.name.lowercase()}${gap.causeClass?.let { ", $it" }.orEmpty()})" }

    private fun sources(catalog: String): List<CatalogSource> =
        catalogSources?.invoke(catalog) ?: openSources.computeIfAbsent(catalog) { sparkSources(it) }

    private fun sparkSources(catalog: String): List<CatalogSource> {
        val properties = catalogProperties()
        val catalogClass = properties[catalog].orEmpty()
        val type = properties["$catalog.$CATALOG_TYPE_PROPERTY"].orEmpty().lowercase()
        val hasCatalogImpl = properties.containsKey("$catalog.catalog-impl")

        return when {
            catalogClass == JDBC_CATALOG_CLASS -> emptyList()
            catalog == SESSION_CATALOG_NAME -> {
                val spark = sparkSessionProvider.getOrCreate()
                val hms =
                    HmsSource(
                        HmsCatalogReader(
                            catalog,
                            spark.sharedState().externalCatalog(),
                            HadoopFileIO(catalogFileSystemProvider.catalogReadConfiguration(catalog)),
                        ),
                    )
                when {
                    catalogClass.isEmpty() -> listOf(hms)
                    catalogClass == SPARK_SESSION_CATALOG_CLASS && type == "rest" -> listOf(hms, restSource(catalog, properties))
                    // A Hive-type session catalog keeps its Iceberg tables in the metastore, which the reader reads.
                    catalogClass == SPARK_SESSION_CATALOG_CLASS && type == "hive" && !hasCatalogImpl && sameMetastore(catalog, properties, spark) -> listOf(hms)
                    else -> throw UnsupportedCatalogException(catalog, "session catalog class or type is not supported")
                }
            }
            catalogClass == SPARK_CATALOG_CLASS && type == "rest" -> listOf(restSource(catalog, properties))
            catalogClass == SPARK_CATALOG_CLASS -> {
                val plugin = sparkSessionProvider.getOrCreate().sessionState().catalogManager().catalog(catalog)
                val icebergCatalog = IcebergCatalogReader.icebergCatalogOf(plugin) ?: throw UnsupportedCatalogException(catalog, "no Iceberg catalog behind it")
                listOf(IcebergSource(IcebergCatalogReader(catalog, icebergCatalog)))
            }
            else -> throw UnsupportedCatalogException(catalog, "catalog class is not supported")
        }
    }

    // The Iceberg session catalog can name its own metastore. Only Spark's own metastore is read.
    private fun sameMetastore(
        catalog: String,
        properties: Map<String, String>,
        spark: org.apache.spark.sql.SparkSession,
    ): Boolean {
        val uri = properties["$catalog.$CATALOG_URI_PROPERTY"]?.trim().orEmpty()
        return uri.isEmpty() || uri == spark.sparkContext().hadoopConfiguration().get("hive.metastore.uris")?.trim()
    }

    private fun restSource(
        catalog: String,
        properties: Map<String, String>,
    ): CatalogSource {
        val catalogProperties = properties.filterKeys { it.startsWith("$catalog.") }.mapKeys { it.key.removePrefix("$catalog.") }
        return when (
            val opened = RestCatalogReader.open(catalog, catalogProperties, catalogFileSystemProvider.catalogReadConfiguration(catalog))
        ) {
            is RestCatalogReader.Opened.Refused -> throw UnsupportedCatalogException(catalog, describe(opened.result.gaps))
            is RestCatalogReader.Opened.Ready -> {
                openRestClients.put(catalog, opened)?.let { runCatching { it.close() } }
                IcebergSource(opened.reader)
            }
        }
    }

    @PreDestroy
    fun close() {
        openRestClients.values.forEach { runCatching { it.close() } }
        openRestClients.clear()
        openSources.clear()
    }
}
