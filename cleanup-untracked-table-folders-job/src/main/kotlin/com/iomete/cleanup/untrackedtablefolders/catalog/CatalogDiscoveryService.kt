package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogStorageProperties
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
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
    val table: String,
    val isTemporary: Boolean,
    val location: String?,
    val unresolvedReason: String? = null,
    // Iceberg write paths can place this table's files outside its location.
    val writeLocations: List<String> = emptyList(),
    // The location read before an unresolved table failed a later check, or the folder named by its missing metadata
    // file. It is not verified, so it only keeps that folder out of the unmatched list and must never select a scan
    // root or anchor ownership.
    val knownLocation: String? = null,
    val provider: String? = null,
) {
    init {
        require(knownLocation == null || unresolvedReason != null) { "knownLocation is only for unresolved tables" }
    }

    // A null location means the catalog object owns no storage, such as a view. Unresolved means
    // the table owns storage we could not locate, so its folder cannot be identified.
    val isUnresolved: Boolean get() = unresolvedReason != null

    // The metastore gives a JDBC table a default location, but its rows live in another database, so no folder holds them.
    val storesFiles: Boolean get() = provider?.lowercase() !in JDBC_PROVIDERS

    val qualifiedName: String get() = "$catalog.$database.$table"
}

private class TableLocationUnresolvedException(
    val reason: String,
    cause: Throwable,
    val knownLocation: String? = null,
) : RuntimeException(reason, cause)

// A name part that holds a backtick is never put in SQL text. Doubling it would be wrong for SHOW NAMESPACES output,
// which already quotes some names, and a raw one moves where Spark splits the name, so it could name another object.
private class UnsupportedIdentifierException(
    message: String,
) : IllegalArgumentException(message)

// The backtick-quoted multipart name for catalog SQL text.
private fun sqlName(vararg parts: Pair<String, String>): String {
    parts.firstOrNull { (_, name) -> '`' in name }?.let { (kind, _) ->
        throw UnsupportedIdentifierException(
            "The $kind name contains a backtick, so the job does not put it in SQL text: " +
                parts.joinToString(", ") { (part, name) -> "$part=$name" },
        )
    }
    return parts.joinToString(".") { (_, name) -> "`$name`" }
}

class DatabaseNotFoundException(
    val catalog: String,
    val database: String,
    cause: Throwable,
) : RuntimeException(
    "Database not found: catalog=$catalog, database=$database",
    cause,
)

// REST catalog errors preserve this Iceberg missing-location message.
private const val MISSING_METADATA_MESSAGE = "Location does not exist:"

private const val WAREHOUSE_PROPERTY = "warehouse"
private const val SESSION_CATALOG_NAME = "spark_catalog"
private const val SESSION_WAREHOUSE_KEY = "spark.sql.warehouse.dir"
private const val CATALOG_TYPE_PROPERTY = "type"
private const val CATALOG_URI_PROPERTY = "uri"
private val ICEBERG_CATALOG_CLASSES =
    setOf("org.apache.iceberg.spark.SparkSessionCatalog", "org.apache.iceberg.spark.SparkCatalog")
private const val REST_CATALOG_SERVICE_HOST = "iom-rest-catalog"

// The class IOMETE sets for a JDBC catalog. It is the only catalog type the platform creates without table storage.
private const val JDBC_CATALOG_CLASS = "org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog"

// Spark's names for its JDBC data source, plus the SQL Server connector the IOMETE docs use, lowercased.
private val JDBC_PROVIDERS =
    setOf(
        "jdbc",
        "org.apache.spark.sql.jdbc",
        "org.apache.spark.sql.jdbc.defaultsource",
        "org.apache.spark.sql.execution.datasources.jdbc",
        "org.apache.spark.sql.execution.datasources.jdbc.defaultsource",
        "org.apache.spark.sql.execution.datasources.jdbc.jdbcrelationprovider",
        "com.microsoft.sqlserver.jdbc.spark",
    )

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

internal fun isMetadataNotFoundError(error: Throwable): Boolean {
    var current: Throwable? = error

    while (current != null) {
        if (current is java.io.FileNotFoundException ||
            current.message.orEmpty().contains(MISSING_METADATA_MESSAGE)
        ) {
            return true
        }

        current = current.cause
    }

    return false
}

// For a missing <folder>/metadata/<file>.metadata.json, <folder> is where the table normally lives. It is evidence
// only: write.metadata.path can place metadata elsewhere, so it may protect a folder but never proves one unowned.
internal fun missingMetadataTableFolder(error: Throwable): String? {
    val missingPath =
        generateSequence(error) { it.cause }
            .mapNotNull { it.message?.substringAfter(MISSING_METADATA_MESSAGE, missingDelimiterValue = "")?.trim() }
            .firstOrNull { it.isNotEmpty() }
            ?.split(Regex("\\s+"))
            ?.first()
            ?: return null
    val folder = missingPath.substringBeforeLast("/$METADATA_FOLDER/", missingDelimiterValue = "")
    val fileName = missingPath.substringAfterLast("/$METADATA_FOLDER/")

    if (folder.isEmpty() || '/' in fileName || !fileName.endsWith(METADATA_FILE_SUFFIX)) {
        return null
    }

    return folder.takeIf {
        runCatching { StoragePathUtils.isQualifiedLocation(it) && !StoragePathUtils.isStorageRoot(it) }.getOrDefault(false)
    }
}

private const val METADATA_FOLDER = "metadata"
private const val METADATA_FILE_SUFFIX = ".metadata.json"

private val ICEBERG_WRITE_PATH_PROPERTIES =
    listOf("write.data.path", "write.metadata.path", "write.object-storage.path", "write.folder-storage.path")

private data class TableStorage(
    val location: String?,
    val writeLocations: List<String>,
    val provider: String? = null,
)

// Typed errors only: a denied lookup's message can name a table that looks like this error class.
private fun isTableNotFoundError(error: Throwable): Boolean =
    generateSequence(error) { it.cause }.any { current ->
        current is NoSuchTableException || (current is SparkThrowable && current.errorClass == "TABLE_OR_VIEW_NOT_FOUND")
    }

private fun isDatabaseNotFoundError(error: Throwable): Boolean {
    var current: Throwable? = error

    while (current != null) {
        val className = current.javaClass.name
        val message = current.message.orEmpty()

        if (className.endsWith("NoSuchNamespaceException") || message.contains("SCHEMA_NOT_FOUND", ignoreCase = true)) {
            return true
        }

        current = current.cause
    }

    return false
}

@ApplicationScoped
class CatalogDiscoveryService {
    private val logger = SafeLogger.getLogger(CatalogDiscoveryService::class.java)

    @Inject
    lateinit var sparkSessionProvider: SparkSessionProvider

    fun discoverDatabase(
        catalog: String,
        database: String,
    ): DiscoveredDatabase {
        logger.info("Discovering database metadata for catalog=$catalog database=$database")

        return DiscoveredDatabase(
            catalog = catalog,
            database = database,
            location = discoverDatabaseLocation(
                catalog = catalog,
                database = database,
            ),
            tables = discoverTables(
                catalog = catalog,
                database = database,
            ),
        )
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

    fun discoverTables(
        catalog: String,
        database: String,
    ): List<DiscoveredTable> {
        logger.info("Discovering active tables for catalog=$catalog database=$database")

        val name = sqlName("catalog" to catalog, "database" to database)
        val spark = sparkSessionProvider.getOrCreate()

        val rows =
            try {
                spark.sql("SHOW TABLES FROM $name").collectAsList()
            } catch (th: Throwable) {
                if (isDatabaseNotFoundError(th)) {
                    throw DatabaseNotFoundException(
                        catalog = catalog,
                        database = database,
                        cause = th,
                    )
                }

                throw IllegalStateException(
                    "Failed to discover tables for catalog=$catalog database=$database",
                    th,
                )
            }

        return rows.map { row ->
            val tableName = row.getString(1)
            val isTemporary = row.getBoolean(2)

            try {
                val storage = checkNotNull(discoverTableStorage(catalog = catalog, database = database, table = tableName))
                DiscoveredTable(
                    catalog = catalog,
                    database = database,
                    table = tableName,
                    isTemporary = isTemporary,
                    location = storage.location,
                    writeLocations = storage.writeLocations,
                    provider = storage.provider,
                )
            } catch (th: TableLocationUnresolvedException) {
                logger.warn(
                    "Catalog lists a table whose Iceberg metadata or storage location could not be resolved: " +
                        "catalog=$catalog, database=$database, table=$tableName, reason=${th.reason}"
                )

                DiscoveredTable(
                    catalog = catalog,
                    database = database,
                    table = tableName,
                    isTemporary = isTemporary,
                    location = null,
                    unresolvedReason = th.reason,
                    knownLocation = th.knownLocation,
                )
            }
        }
    }

    // A database whose location cannot be read maps to null.
    fun visibleDatabaseLocations(catalog: String): Map<String, String?> {
        val startedAt = System.nanoTime()
        val name = sqlName("catalog" to catalog)
        val spark = sparkSessionProvider.getOrCreate()
        val databases = spark.sql("SHOW NAMESPACES IN $name").collectAsList().map { it.getString(0) }
        logger.info("Reading the locations of ${databases.size} visible database(s) in catalog=$catalog to protect their storage")

        val locations =
            databases.associateWith { database ->
                try {
                    discoverDatabaseLocation(catalog, database)
                } catch (error: Exception) {
                    logger.warn("Could not read the location of database=$database in catalog=$catalog", error)
                    null
                }
            }

        logger.info(
            "Read the locations of ${locations.size} database(s) in catalog=$catalog in " +
                "${(System.nanoTime() - startedAt) / 1_000_000} ms; ${locations.count { it.value == null }} had no readable location"
        )
        return locations
    }

    // Null when the table does not exist; any other failure is thrown so the caller can fail closed.
    fun tableLocationIfExists(
        catalog: String,
        database: String,
        table: String,
    ): String? {
        val name = sqlName("catalog" to catalog, "database" to database, "table" to table)
        val spark = sparkSessionProvider.getOrCreate()
        dropCachedTable(spark, catalog, database, table)
        val rows =
            try {
                spark.sql("DESCRIBE EXTENDED $name").collectAsList()
            } catch (th: Throwable) {
                if (isTableNotFoundError(th)) {
                    return null
                }
                throw IllegalStateException("Failed to look up table=$catalog.$database.$table", th)
            }
        val details = tableDetails(rows) ?: throw IllegalStateException("No single table details section for table=$catalog.$database.$table")
        val location = describeValue(details, "Location")
        check(location != null || isView(details)) { "No location for table=$catalog.$database.$table" }

        return location
    }

    // Reads the table as the full read of a database does, so a write path into a folder, or a setting that lets the
    // table use files outside its declared paths, counts the same. Any failure other than a missing table is thrown.
    fun tableStorageIfExists(
        catalog: String,
        database: String,
        table: String,
    ): NamedTableStorage =
        try {
            discoverTableStorage(catalog = catalog, database = database, table = table, missingAsNull = true)
                ?.let { NamedTableStorage.Found(it.location, it.writeLocations) }
                ?: NamedTableStorage.NotFound
        } catch (th: TableLocationUnresolvedException) {
            NamedTableStorage.Unresolved(th.reason)
        }

    private fun discoverDatabaseLocation(
        catalog: String,
        database: String,
    ): String? {
        val qualifiedDatabaseName = sqlName("catalog" to catalog, "database" to database)
        val spark = sparkSessionProvider.getOrCreate()

        val rows =
            try {
                spark.sql("DESCRIBE DATABASE EXTENDED $qualifiedDatabaseName").collectAsList()
            } catch (th: Throwable) {
                if (isDatabaseNotFoundError(th)) {
                    throw DatabaseNotFoundException(
                        catalog = catalog,
                        database = database,
                        cause = th,
                    )
                }

                throw IllegalStateException(
                    "Failed to discover location for database=$qualifiedDatabaseName",
                    th,
                )
            }

        return rows
            .firstOrNull { row -> row.getString(0).trim().equals("Location", ignoreCase = true) }
            ?.getString(1)
            ?.trim()
            ?.takeIf { it.isNotBlank() }
    }

    // A path without a scheme and bucket resolves against the writer's default filesystem, which the job cannot know.
    // With missingAsNull, a table that does not exist gives null instead of an error.
    private fun discoverTableStorage(
        catalog: String,
        database: String,
        table: String,
        missingAsNull: Boolean = false,
    ): TableStorage? {
        if ('`' in table) {
            throw TableLocationUnresolvedException(
                "The table name contains a backtick, so the job does not put it in SQL text and cannot read its location",
                UnsupportedIdentifierException("backtick in table name"),
            )
        }
        val qualifiedTableName = sqlName("catalog" to catalog, "database" to database, "table" to table)
        val spark = sparkSessionProvider.getOrCreate()
        dropCachedTable(spark, catalog, database, table)

        val rows =
            try {
                spark.sql("DESCRIBE EXTENDED $qualifiedTableName").collectAsList()
            } catch (th: Throwable) {
                if (missingAsNull && isTableNotFoundError(th)) {
                    return null
                }
                if (isMetadataNotFoundError(th)) {
                    throw TableLocationUnresolvedException(th.message ?: th::class.java.name, th, missingMetadataTableFolder(th))
                }

                throw IllegalStateException(
                    "Failed to discover location for table=$qualifiedTableName",
                    th,
                )
            }

        val details =
            tableDetails(rows) ?: throw TableLocationUnresolvedException(
                "DESCRIBE EXTENDED returned no single table details section",
                IllegalStateException("no single table details section"),
            )
        val location =
            describeValue(details, "Location") ?: if (isView(details)) {
                return TableStorage(null, emptyList())
            } else {
                throw TableLocationUnresolvedException(
                    "The table has no location, so its storage cannot be found",
                    IllegalStateException("missing table location"),
                )
            }
        if (runCatching { !StoragePathUtils.isQualifiedLocation(location) }.getOrDefault(true)) {
            throw TableLocationUnresolvedException(
                "The table location has no scheme or bucket, so it cannot be matched to storage",
                IllegalStateException("unqualified table location"),
            )
        }
        val provider = describeValue(details, "Provider")
        if (provider != null && !provider.equals("iceberg", ignoreCase = true)) {
            return TableStorage(location, emptyList(), provider)
        }

        val properties =
            try {
                spark.sql("SHOW TBLPROPERTIES $qualifiedTableName").collectAsList().associate { it.getString(0) to it.getString(1) }
            } catch (th: Throwable) {
                throw TableLocationUnresolvedException("Table properties could not be read: ${th.message ?: th::class.java.name}", th, location)
            }

        // Both settings let the table use files outside its location and declared write paths. Iceberg reads any
        // gc.enabled value other than true as false.
        if (properties["gc.enabled"]?.equals("true", ignoreCase = true) == false) {
            throw TableLocationUnresolvedException(
                "gc.enabled is not true, so the table may use files outside its location",
                IllegalStateException("gc.enabled is not true"),
                location,
            )
        }
        if (properties.containsKey("write.location-provider.impl")) {
            throw TableLocationUnresolvedException(
                "write.location-provider.impl is set, so the table may write outside its declared paths",
                IllegalStateException("write.location-provider.impl"),
                location,
            )
        }

        val writeLocations = ICEBERG_WRITE_PATH_PROPERTIES.mapNotNull { properties[it]?.trim() }
        if (writeLocations.any { runCatching { !StoragePathUtils.isQualifiedLocation(it) }.getOrDefault(true) }) {
            throw TableLocationUnresolvedException(
                "A declared write path has no scheme or bucket, so it cannot be matched to storage",
                IllegalStateException("unqualified write path"),
                location,
            )
        }

        return TableStorage(location = location, writeLocations = writeLocations, provider = provider)
    }

    // Iceberg catalogs cache each table they load (cache-enabled is on by default, and the copy expires only after it
    // goes unused), so a table read earlier in the run, for example as another database's table, would be described
    // from that copy. Only this table's copy is dropped, and a failure is thrown so the caller fails closed.
    private fun dropCachedTable(
        spark: SparkSession,
        catalog: String,
        database: String,
        table: String,
    ) {
        try {
            val tableCatalog = spark.sessionState().catalogManager().catalog(catalog) as? TableCatalog ?: error("not a table catalog")
            tableCatalog.invalidateTable(Identifier.of(arrayOf(database), table))
        } catch (th: Throwable) {
            throw IllegalStateException("Failed to drop the cached copy of table=`$catalog`.`$database`.`$table`", th)
        }
    }

    // Columns are listed before the table details, and on v1 tables columns with defaults again after them, so a
    // column named like a detail field must not be read as one. The details start at a header row with no data type,
    // which no column has, and end at the next blank row. Null when that section is missing, repeated, or repeats a field.
    private fun tableDetails(rows: List<Row>): Map<String, String>? {
        val header =
            rows.indices.singleOrNull { index ->
                rows[index].getString(0)?.trim() == "# Detailed Table Information" && rows[index].getString(1).isNullOrBlank()
            } ?: return null
        val fields = rows.drop(header + 1).takeWhile { !it.getString(0).isNullOrBlank() }
        val details = fields.associate { it.getString(0).trim().lowercase() to it.getString(1)?.trim().orEmpty() }

        return details.takeIf { it.size == fields.size }
    }

    // A view owns no storage, so it is the only catalog object that may have no location.
    private fun isView(details: Map<String, String>): Boolean = describeValue(details, "Type").equals("VIEW", ignoreCase = true)

    private fun describeValue(
        details: Map<String, String>,
        name: String,
    ): String? = details[name.lowercase()]?.takeIf { it.isNotBlank() }
}
