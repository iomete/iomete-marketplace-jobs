package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.spark.PINNED_PARSER_SETTINGS
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSqlText
import com.iomete.cleanup.untrackedtablefolders.spark.VARIABLE_SUBSTITUTE
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.internal.SessionState
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

private const val DATABASE_LOCATION = "s3a://b/lake/sales.db"

// Captures the SQL text the catalog reads send and reads it as Spark would, so the names Spark looks up can be compared
// with the names the job meant.
class CatalogSqlTextTest {

    private val sent = mutableListOf<String>()
    private var listedTables = emptyList<String>()
    private var listedNamespaces = emptyList<String>()

    // The session's SQL settings. Nothing is set at first, so Spark's defaults apply, as in a new session.
    private val sessionSettings = mutableMapOf<String, String>()

    private val tableCatalog = mockk<TableCatalog>(relaxed = true)
    private val spark =
        mockk<SparkSession>().also { spark ->
            val catalogManager = mockk<CatalogManager>()
            every { catalogManager.catalog(any()) } returns tableCatalog
            every { spark.sessionState() } returns mockk<SessionState>().also { every { it.catalogManager() } returns catalogManager }
            every { spark.sql(any()) } answers { respond(firstArg<String>().also { sent += it }) }
            val conf = mockk<RuntimeConfig>()
            every { conf.set(any<String>(), any<String>()) } answers { sessionSettings[firstArg()] = secondArg() }
            every { conf.get(any<String>()) } answers { sessionSettings.getValue(firstArg()) }
            every { spark.conf() } returns conf
        }

    // The provider's own pins run on every session it hands out; only the session itself is a stand-in.
    private val discovery =
        CatalogDiscoveryService().apply {
            val provider = SparkSessionProvider()
            sparkSessionProvider = mockk<SparkSessionProvider>().also { every { it.getOrCreate() } answers { provider.pinParserSettings(spark) } }
        }

    @Test
    fun `names that hold dollar-brace text reach Spark as written`() {
        val catalog = "c_\${x}"
        val database = "db_\${spark.app.name}"
        val table = "t_\${system:user.home}"
        val probed = "t_\${env:HOME}"
        listedTables = listOf(table)
        listedNamespaces = listOf(database)

        discovery.discoverDatabase(catalog, database)
        discovery.tableLocationIfExists(catalog, database, probed)
        discovery.visibleDatabaseLocations(catalog)

        val expected =
            listOf(
                listOf(catalog, database),
                listOf(catalog, database),
                listOf(catalog, database, table),
                listOf(catalog, database, table),
                listOf(catalog, database, probed),
                listOf(catalog),
                listOf(catalog, database),
            )
        assertEquals(expected, sent.map { identifier(it, sessionSettings) })
        assertEquals(PINNED_PARSER_SETTINGS, sessionSettings)

        // Lift L1: with substitution on, as it is by default, Spark would have looked up other names.
        sent.zip(expected).forEach { (sql, names) ->
            assertNotEquals(names, identifier(sql, PINNED_PARSER_SETTINGS + (VARIABLE_SUBSTITUTE to "true")), sql)
        }
    }

    @Test
    fun `a listed table whose name has a backtick is unresolved and never described`() {
        listedTables = listOf("events", "bad`name")

        val tables = discovery.discoverTables("c", "sales")

        assertEquals(listOf("events" to false, "bad`name" to true), tables.map { it.table to it.isUnresolved })
        assertTrue(tables[1].unresolvedReason.orEmpty().contains("backtick"), tables[1].unresolvedReason)
        assertEquals(
            listOf("SHOW TABLES FROM `c`.`sales`", "DESCRIBE EXTENDED `c`.`sales`.`events`", "SHOW TBLPROPERTIES `c`.`sales`.`events`"),
            sent,
        )
        verify(exactly = 0) { tableCatalog.invalidateTable(Identifier.of(arrayOf("sales"), "bad`name")) }
    }

    @Test
    fun `a catalog, database or table name with a backtick is refused before any SQL is built`() {
        listOf("c`x" to "sales", "c" to "sa`les", "c" to "`sales`").forEach { (catalog, database) ->
            val error = assertThrows(IllegalArgumentException::class.java) { discovery.discoverDatabase(catalog, database) }
            assertTrue(error.message.orEmpty().contains("backtick"), error.message)
            assertThrows(IllegalArgumentException::class.java) { discovery.discoverTables(catalog, database) }
            // Never null: null means the table does not exist, which would leave its folder unprotected.
            assertThrows(IllegalArgumentException::class.java) { discovery.tableLocationIfExists(catalog, database, "events") }
        }
        assertThrows(IllegalArgumentException::class.java) { discovery.tableLocationIfExists("c", "sales", "x`y") }
        assertThrows(IllegalArgumentException::class.java) { discovery.visibleDatabaseLocations("c`x") }

        assertEquals(emptyList<String>(), sent)
    }

    @Test
    fun `a listed database whose name has a backtick has no readable location and is never described`() {
        // SHOW NAMESPACES quotes names that need it, so these come back with backticks around them.
        listedNamespaces = listOf("sales", "`sales-eu`", "`a``b`")

        val locations = discovery.visibleDatabaseLocations("c")

        assertEquals(mapOf("sales" to DATABASE_LOCATION, "`sales-eu`" to null, "`a``b`" to null), locations)
        assertEquals(listOf("SHOW NAMESPACES IN `c`", "DESCRIBE DATABASE EXTENDED `c`.`sales`"), sent)
    }

    private fun respond(sql: String): Dataset<Row> =
        dataset(
            when {
                sql.startsWith("SHOW NAMESPACES IN ") -> listedNamespaces.map { RowFactory.create(it) }
                sql.startsWith("SHOW TABLES FROM ") -> listedTables.map { RowFactory.create("sales", it, false) }
                sql.startsWith("DESCRIBE DATABASE EXTENDED ") -> listOf(RowFactory.create("Location", DATABASE_LOCATION))
                sql.startsWith("DESCRIBE EXTENDED ") ->
                    listOf(
                        RowFactory.create("id", "bigint", null),
                        RowFactory.create("", "", ""),
                        RowFactory.create("# Detailed Table Information", "", ""),
                        RowFactory.create("Location", "$DATABASE_LOCATION/events", ""),
                        RowFactory.create("Provider", "iceberg", ""),
                    )
                sql.startsWith("SHOW TBLPROPERTIES ") -> emptyList()
                else -> error("unexpected SQL: $sql")
            },
        )

    private fun dataset(rows: List<Row>): Dataset<Row> = mockk<Dataset<Row>>().also { every { it.collectAsList() } returns rows }
}

// The multipart name a catalog statement refers to, as Spark reads it.
private fun identifier(
    sql: String,
    settings: Map<String, String>,
): List<String> = SparkSqlText(sql, settings).catalogStatementName()
