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
            DiscoveredTable(
                catalog = catalog,
                database = database,
                table = row.getString(1),
                isTemporary = row.getBoolean(2),
            )
        }
    }
}
