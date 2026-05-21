package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

data class DiscoveredDatabase(
    val catalog: String,
    val database: String,
    val location: String?,
    val tables: List<DiscoveredTable>,
)

data class DiscoveredTable(
    val catalog: String,
    val database: String,
    val table: String,
    val isTemporary: Boolean,
    val location: String?,
)

class DatabaseNotFoundException(
    val catalog: String,
    val database: String,
    cause: Throwable,
) : RuntimeException(
    "Database not found: catalog=$catalog, database=$database",
    cause,
)

private fun isDatabaseNotFoundError(error: Throwable): Boolean {
    var current: Throwable? = error

    while (current != null) {
        val className = current::class.qualifiedName.orEmpty()
        val message = current.message.orEmpty()

        if (className.endsWith("NoSuchNamespaceException") || message.contains("SCHEMA_NOT_FOUND")) {
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

            DiscoveredTable(
                catalog = catalog,
                database = database,
                table = tableName,
                isTemporary = row.getBoolean(2),
                location = discoverTableLocation(
                    catalog = catalog,
                    database = database,
                    table = tableName,
                ),
            )
        }
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
