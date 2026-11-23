package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.CleanupFlowHarness.Companion.putFolder
import com.iomete.cleanup.untrackedtablefolders.catalog.CatalogDiscoveryService
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredDatabase
import com.iomete.cleanup.untrackedtablefolders.catalog.DiscoveredTable
import com.iomete.cleanup.untrackedtablefolders.config.ApplicationConfig
import com.iomete.cleanup.untrackedtablefolders.spark.SparkSessionProvider
import com.iomete.cleanup.untrackedtablefolders.storage.InMemoryS3FileSystem
import io.mockk.every
import io.mockk.mockk
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.internal.SessionState
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

private const val CATALOG = "example_catalog"
private const val WAREHOUSE = "s3a://bucket/lake"
private const val ORDERS = "s3a://bucket/lake/orders.db"
private const val SALES = "s3a://bucket/lake/sales.db"

class IdentifierGuardFlowTest {

    private val sent = mutableListOf<String>()

    // Answers only the statements it knows. Anything else fails, as Spark fails on a name it cannot parse or find.
    private val answers =
        mapOf(
            "DESCRIBE DATABASE EXTENDED `$CATALOG`.`orders`" to listOf(RowFactory.create("Location", ORDERS)),
            "SHOW TABLES FROM `$CATALOG`.`orders`" to
                listOf(RowFactory.create("orders", "t1", false), RowFactory.create("orders", "odd`name", false)),
            "DESCRIBE EXTENDED `$CATALOG`.`orders`.`t1`" to
                listOf(
                    RowFactory.create("", "", ""),
                    RowFactory.create("# Detailed Table Information", "", ""),
                    RowFactory.create("Location", "$ORDERS/t1", ""),
                    RowFactory.create("Provider", "hive", ""),
                ),
        )

    private val spark =
        mockk<SparkSession>().also { spark ->
            val catalogManager = mockk<CatalogManager>()
            every { catalogManager.catalog(any()) } returns mockk<TableCatalog>(relaxed = true)
            every { spark.sessionState() } returns mockk<SessionState>().also { every { it.catalogManager() } returns catalogManager }
            every { spark.sql(any()) } answers {
                val sql = firstArg<String>().also { sent += it }
                dataset(answers[sql] ?: throw IllegalStateException("cannot run $sql"))
            }
        }

    private val catalogReads =
        CatalogDiscoveryService().apply {
            sparkSessionProvider = mockk<SparkSessionProvider>().also { every { it.getOrCreate() } returns spark }
        }

    @BeforeEach
    fun setUp() {
        InMemoryS3FileSystem.reset()
    }

    @AfterEach
    fun tearDown() {
        InMemoryS3FileSystem.reset()
        FileSystem.closeAll()
    }

    @Test
    fun `a backtick in a database or table name stops only that database`() {
        listOf(ORDERS, SALES).forEach { root ->
            putFolder(root)
            putFolder("$root/t1", "part-0.parquet")
            putFolder("$root/orphan", "part-0.parquet")
        }
        val flow = flow(CATALOG, listOf("bad`db", "orders", "sales"))
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "bad`db") } answers { catalogReads.discoverDatabase(CATALOG, "bad`db") }
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "orders") } answers { catalogReads.discoverDatabase(CATALOG, "orders") }
        every { flow.catalogDiscoveryService.discoverDatabase(CATALOG, "sales") } returns
            DiscoveredDatabase(CATALOG, "sales", SALES, listOf(DiscoveredTable(CATALOG, "sales", "t1", isTemporary = false, location = "$SALES/t1")))

        flow.run()

        val records = flow.auditRecords.associateBy { it.databaseName }
        assertEquals(
            mapOf(
                "bad`db" to ("FAILED" to "unexpected_error"),
                "orders" to ("BLOCKED" to "unresolved_catalog_ownership"),
                "sales" to ("SUCCESS" to null),
            ),
            records.mapValues { (_, record) -> record.status to record.statusReason },
        )
        val error = records.getValue("bad`db").errorMessage.orEmpty()
        assertTrue(error.contains("backtick") && error.contains("database=bad`db"), error)
        assertEquals(1, records.getValue("orders").unresolvedTableCount)
        assertTrue(records.getValue("orders").diagnosticDetails.getValue("unresolved_tables_sample").contains("$CATALOG.orders.odd`name"))
        assertTrue(flow.objectExists("$ORDERS/orphan/part-0.parquet"))
        assertEquals(listOf("$SALES/orphan"), records.getValue("sales").deletedFolders)
        assertFalse(flow.objectExists("$SALES/orphan/part-0.parquet"))
        assertTrue(sent.none { "bad`db" in it || "odd`name" in it }, sent.toString())
    }

    @Test
    fun `a backtick in the catalog name fails each database with its own row and the run still ends`() {
        putFolder(SALES)
        putFolder("$SALES/orphan", "part-0.parquet")
        val catalog = "lake`x"
        val flow = flow(catalog, listOf("sales", "orders"))
        every { flow.catalogDiscoveryService.discoverDatabase(catalog, any()) } answers { catalogReads.discoverDatabase(catalog, secondArg()) }

        flow.run()

        assertEquals(listOf("sales", "orders"), flow.auditRecords.map { it.databaseName })
        flow.auditRecords.forEach { record ->
            assertEquals("FAILED" to "unexpected_error", record.status to record.statusReason)
            assertTrue(record.errorMessage.orEmpty().contains("backtick"), record.errorMessage)
        }
        assertEquals(emptyList<String>(), sent)
        assertTrue(flow.objectExists("$SALES/orphan/part-0.parquet"))
    }

    private fun flow(
        catalog: String,
        databases: List<String>,
    ): CleanupFlowHarness =
        CleanupFlowHarness(ApplicationConfig(catalog = catalog, databases = databases, dryRun = false, deleteEnabled = true)).also { flow ->
            every { flow.catalogDiscoveryService.sharedWarehouseRoots(any()) } returns listOf(WAREHOUSE)
        }

    private fun dataset(rows: List<Row>): Dataset<Row> = mockk<Dataset<Row>>().also { every { it.collectAsList() } returns rows }
}
