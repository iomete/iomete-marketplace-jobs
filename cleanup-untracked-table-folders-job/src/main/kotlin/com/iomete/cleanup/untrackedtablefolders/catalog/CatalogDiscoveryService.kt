package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import jakarta.enterprise.context.ApplicationScoped
import jakarta.inject.Inject
import org.jboss.logging.Logger

data class DiscoveredTable(
    val catalog: String,
    val database: String,
    val table: String,
    val isTemporary: Boolean,
    val location: String?,
)

@ApplicationScoped
class CatalogDiscoveryService {
    private val logger = Logger.getLogger(CatalogDiscoveryService::class.java)

    @Inject
    lateinit var sparkSessionProvider: SparkSessionProvider

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
                logger.warn("Failed to discover tables for catalog=$catalog database=$database", th)
                return emptyList()
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
                logger.warn("Failed to discover location for table=$qualifiedTableName", th)
                return null
            }

        return rows
            .firstOrNull { row -> row.getString(0).trim().equals("Location", ignoreCase = true) }
            ?.getString(1)
            ?.trim()
            ?.takeIf { it.isNotBlank() }
    }
}
