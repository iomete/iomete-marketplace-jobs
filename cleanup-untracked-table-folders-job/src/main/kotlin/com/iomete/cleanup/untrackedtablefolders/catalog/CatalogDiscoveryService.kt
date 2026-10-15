package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogStorageProperties
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.apache.spark.SparkThrowable
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.jboss.logging.Logger

data class DiscoveredDatabase(
    val catalog: String,
    val database: String,
    val location: String?,
    val tables: List<DiscoveredTable>,
) {
    val unresolvedTables: List<DiscoveredTable> get() = tables.filter { it.isUnresolved }
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
) {
    // A null location means the catalog object owns no storage, such as a view. Unresolved means
    // the table owns storage we could not locate, so its folder cannot be identified.
    val isUnresolved: Boolean get() = unresolvedReason != null

    val qualifiedName: String get() = "$catalog.$database.$table"
}

private class TableLocationUnresolvedException(
    val reason: String,
    cause: Throwable,
) : RuntimeException(reason, cause)

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

private val ICEBERG_WRITE_PATH_PROPERTIES =
    listOf("write.data.path", "write.metadata.path", "write.object-storage.path", "write.folder-storage.path")

private data class TableStorage(
    val location: String?,
    val writeLocations: List<String>,
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
    private val logger = Logger.getLogger(CatalogDiscoveryService::class.java)

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
        val catalogWarehouse =
            spark.sparkContext().getConf()
                .get(CatalogStorageProperties.catalogPropertyPrefix(catalog) + WAREHOUSE_PROPERTY, "")
                .trim()

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
        val catalogWarehouses =
            spark.sparkContext().getConf()
                .getAllWithPrefix("spark.sql.catalog.")
                .filter { it._1().endsWith(".$WAREHOUSE_PROPERTY") }
                .map { it._2().trim() }

        return (catalogWarehouses + spark.conf().get(SESSION_WAREHOUSE_KEY, "").trim())
            .filter { it.isNotEmpty() }
            .distinct()
            .sorted()
    }

    // The managed REST catalog places every table at <warehouse>/<namespace>/<table> and ignores any
    // namespace location property, so this root holds its tables even when none of them is visible.
    // Null when the catalog is not an Iceberg REST catalog at all.
    fun restNamespaceRoot(
        catalog: String,
        database: String,
    ): RestNamespaceRoot? {
        val sparkConf = sparkSessionProvider.getOrCreate().sparkContext().getConf()
        val prefix = CatalogStorageProperties.catalogPropertyPrefix(catalog)

        val catalogClass = sparkConf.get(prefix.removeSuffix("."), "").trim()
        val catalogType = sparkConf.get(prefix + CATALOG_TYPE_PROPERTY, "").trim()
        val catalogUri = sparkConf.get(prefix + CATALOG_URI_PROPERTY, "").trim()
        val warehouse = sparkConf.get(prefix + WAREHOUSE_PROPERTY, "").trim().trimEnd('/')

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

        val spark = sparkSessionProvider.getOrCreate()

        val rows =
            try {
                spark.sql("SHOW TABLES FROM `$catalog`.`$database`").collectAsList()
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
                val storage = discoverTableStorage(catalog = catalog, database = database, table = tableName)
                DiscoveredTable(
                    catalog = catalog,
                    database = database,
                    table = tableName,
                    isTemporary = isTemporary,
                    location = storage.location,
                    writeLocations = storage.writeLocations,
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
                )
            }
        }
    }

    // A database whose location cannot be read maps to null.
    fun visibleDatabaseLocations(catalog: String): Map<String, String?> {
        val spark = sparkSessionProvider.getOrCreate()
        val databases = spark.sql("SHOW NAMESPACES IN `$catalog`").collectAsList().map { it.getString(0) }

        return databases.associateWith { database ->
            try {
                discoverDatabaseLocation(catalog, database)
            } catch (error: Exception) {
                logger.warn("Could not read the location of database=$database in catalog=$catalog", error)
                null
            }
        }
    }

    // Null when the table does not exist; any other failure is thrown so the caller can fail closed.
    fun tableLocationIfExists(
        catalog: String,
        database: String,
        table: String,
    ): String? {
        val rows =
            try {
                sparkSessionProvider.getOrCreate().sql("DESCRIBE EXTENDED `$catalog`.`$database`.`$table`").collectAsList()
            } catch (th: Throwable) {
                if (isTableNotFoundError(th)) {
                    return null
                }
                throw IllegalStateException("Failed to look up table=$catalog.$database.$table", th)
            }
        val details = tableDetails(rows) ?: throw IllegalStateException("No single table details section for table=$catalog.$database.$table")

        return describeValue(details, "Location")
    }

    private fun discoverDatabaseLocation(
        catalog: String,
        database: String,
    ): String? {
        val spark = sparkSessionProvider.getOrCreate()
        val qualifiedDatabaseName = "`$catalog`.`$database`"

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
    private fun discoverTableStorage(
        catalog: String,
        database: String,
        table: String,
    ): TableStorage {
        val spark = sparkSessionProvider.getOrCreate()
        val qualifiedTableName = "`$catalog`.`$database`.`$table`"

        val rows =
            try {
                spark.sql("DESCRIBE EXTENDED $qualifiedTableName").collectAsList()
            } catch (th: Throwable) {
                if (isMetadataNotFoundError(th)) {
                    throw TableLocationUnresolvedException(th.message ?: th::class.java.name, th)
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
        val location = describeValue(details, "Location")
        if (location != null && runCatching { !StoragePathUtils.isQualifiedLocation(location) }.getOrDefault(true)) {
            throw TableLocationUnresolvedException(
                "The table location has no scheme or bucket, so it cannot be matched to storage",
                IllegalStateException("unqualified table location"),
            )
        }
        val provider = describeValue(details, "Provider")
        if (location == null || (provider != null && !provider.equals("iceberg", ignoreCase = true))) {
            return TableStorage(location, emptyList())
        }

        val properties =
            try {
                spark.sql("SHOW TBLPROPERTIES $qualifiedTableName").collectAsList().associate { it.getString(0) to it.getString(1) }
            } catch (th: Throwable) {
                throw TableLocationUnresolvedException("Table properties could not be read: ${th.message ?: th::class.java.name}", th)
            }

        // Both settings let the table use files outside its location and declared write paths.
        if (properties["gc.enabled"]?.trim().equals("false", ignoreCase = true)) {
            throw TableLocationUnresolvedException(
                "gc.enabled=false, so the table may use files outside its location",
                IllegalStateException("gc.enabled=false"),
            )
        }
        if (!properties["write.location-provider.impl"].isNullOrBlank()) {
            throw TableLocationUnresolvedException(
                "write.location-provider.impl is set, so the table may write outside its declared paths",
                IllegalStateException("write.location-provider.impl"),
            )
        }

        val writeLocations = ICEBERG_WRITE_PATH_PROPERTIES.mapNotNull { properties[it]?.trim()?.takeIf(String::isNotBlank) }
        if (writeLocations.any { runCatching { !StoragePathUtils.isQualifiedLocation(it) }.getOrDefault(true) }) {
            throw TableLocationUnresolvedException(
                "A declared write path has no scheme or bucket, so it cannot be matched to storage",
                IllegalStateException("unqualified write path"),
            )
        }

        return TableStorage(location = location, writeLocations = writeLocations)
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

    private fun describeValue(
        details: Map<String, String>,
        name: String,
    ): String? = details[name.lowercase()]?.takeIf { it.isNotBlank() }
}
