package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.storage.CatalogStorageProperties
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
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
                DiscoveredTable(
                    catalog = catalog,
                    database = database,
                    table = tableName,
                    isTemporary = isTemporary,
                    location = discoverTableLocation(
                        catalog = catalog,
                        database = database,
                        table = tableName,
                    ),
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

    fun discoverDatabaseLocation(
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

    private fun discoverTableLocation(
        catalog: String,
        database: String,
        table: String,
    ): String? {
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

        return rows
            .firstOrNull { row -> row.getString(0).trim().equals("Location", ignoreCase = true) }
            ?.getString(1)
            ?.trim()
            ?.takeIf { it.isNotBlank() }
    }
}
