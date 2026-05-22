package com.iomete.catalogsync

import com.iomete.catalogsync.extract.TableExtractor
import com.iomete.catalogsync.extract.TableExtractorFactory
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import io.mockk.*
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.eclipse.microprofile.config.Config
import org.eclipse.microprofile.config.ConfigProvider
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.*
import java.util.concurrent.Callable

/**
 * Tests for LakehouseMetadataExtractor.scrape() orchestration behavior.
 * Verifies silent-loss prevention, timeout handling, and failure tracking.
 */
class ScrapeOrchestratorTest {

    private lateinit var mockSparkSession: SparkSession
    private lateinit var mockTableExtractorFactory: TableExtractorFactory
    private lateinit var mockDataSync: DataSync
    private lateinit var mockSparkSessionProvider: SparkSessionProvider
    private lateinit var mockApplicationConfig: ApplicationConfig
    private lateinit var mockMeterRegistry: MeterRegistry
    private lateinit var mockCoreServiceClient: CoreServiceClient
    private lateinit var mockTimer: Timer
    private lateinit var mockCounter: Counter

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk()
        mockTableExtractorFactory = mockk()
        mockDataSync = mockk()
        mockSparkSessionProvider = mockk()
        mockApplicationConfig = mockk()
        mockMeterRegistry = mockk()
        mockCoreServiceClient = mockk()
        mockTimer = mockk(relaxed = true)
        mockCounter = mockk(relaxed = true)

        every { mockSparkSessionProvider.sparkSession } returns mockSparkSession
        every { mockApplicationConfig.excludeSchemas() } returns Optional.of(setOf())

        // Timer mock: invoke the callable/runnable/supplier so side effects happen
        every { mockMeterRegistry.timer(any(), *anyVararg()) } returns mockTimer
        every { mockTimer.recordCallable<Any>(any()) } answers {
            firstArg<Callable<Any>>().call()
        }
        every { mockTimer.record(any<Runnable>()) } answers {
            firstArg<Runnable>().run()
        }
        every { mockTimer.record<Any>(any<java.util.function.Supplier<Any>>()) } answers {
            firstArg<java.util.function.Supplier<Any>>().get()
        }
        every { mockMeterRegistry.counter(any(), *anyVararg()) } returns mockCounter
        // printMetrics needs registry.meters
        every { mockMeterRegistry.meters } returns emptyList()

        // Mock ConfigProvider for parallelism and timeout
        mockkStatic(ConfigProvider::class)
        val mockConfig = mockk<Config>()
        every { ConfigProvider.getConfig() } returns mockConfig
        every { mockConfig.getOptionalValue("HTTP_PARALLELISM", Int::class.java) } returns Optional.of(2)
        every { mockConfig.getOptionalValue("TABLE_PROCESS_TIMEOUT_SECONDS", Long::class.java) } returns Optional.of(60L)
        every { mockConfig.getOptionalValue("SYNC_TIMEOUT_SECONDS", Long::class.java) } returns Optional.of(60L)

        // Default data sync stubs
        every { mockDataSync.syncTableData(any()) } returns true
        every { mockDataSync.syncSchemaData(any()) } returns true
        every { mockDataSync.syncCatalogData(any()) } returns true
    }

    @AfterEach
    fun tearDown() {
        unmockkStatic(ConfigProvider::class)
    }

    private fun buildExtractor(): LakehouseMetadataExtractor {
        return LakehouseMetadataExtractor(
            mockTableExtractorFactory,
            mockDataSync,
            mockSparkSessionProvider,
            mockApplicationConfig,
            mockMeterRegistry,
            mockCoreServiceClient
        )
    }

    private fun mockSchemaRow(name: String): Row {
        return mockk { every { getString(0) } returns name }
    }

    private fun mockTableRow(name: String, isTemp: Boolean = false): Row {
        return mockk {
            every { getString(1) } returns name
            every { getBoolean(2) } returns isTemp
        }
    }

    private fun mockDescribeRow(colName: String, dataType: String, comment: String?): Row {
        return mockk {
            every { getString(0) } returns colName
            every { getString(1) } returns dataType
            every { getString(2) } returns comment
        }
    }

    private fun stubSchemas(catalog: String, vararg schemas: String) {
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show databases in `$catalog`") } returns dataset
        every { dataset.collectAsList() } returns schemas.map { mockSchemaRow(it) }
    }

    private fun stubTablesFor(catalog: String, schema: String, vararg tableNames: String) {
        val tableRows = tableNames.map { mockTableRow(it) }
        val tablesDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show tables from `$catalog`.`$schema`") } returns tablesDataset
        every { tablesDataset.collectAsList() } returns tableRows

        // No views for simplicity
        val viewsDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("show views from `$catalog`.`$schema`") } returns viewsDataset
        every { viewsDataset.collectAsList() } returns emptyList()
    }

    private fun stubDescribeTable(catalog: String, schema: String, table: String) {
        val describeDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("describe extended `$catalog`.`$schema`.`$table`") } returns describeDataset
        every { describeDataset.collectAsList() } returns listOf(
            mockDescribeRow("id", "int", null),
            mockDescribeRow("# Detailed Table Information", "", ""),
            mockDescribeRow("Type", "MANAGED", null),
            mockDescribeRow("Provider", "iceberg", null),
            mockDescribeRow("Owner", "test", null)
        )

        val mockExtractor = mockk<TableExtractor>()
        every { mockExtractor.getTableType } returns "MANAGED"
        every {
            mockTableExtractorFactory.extractorFor(
                provider = "iceberg",
                isView = false,
                catalog = catalog,
                schema = schema,
                table = table,
                currentSnapshotId = any()
            )
        } returns mockExtractor
    }

    // ─── Behavior 1: Phase 1 discovery failure doesn't drop sibling schemas ───

    @Test
    fun `scrape should process other schemas when getTables throws for one schema`() {
        val catalog = CoreServiceClient.CatalogDetails("cat1", listOf("iceberg"), null, null, listOf())
        every { mockCoreServiceClient.catalogs() } returns listOf(catalog)

        stubSchemas("cat1", "schema1", "schema2")

        // Use spyk so we can override getTables
        val extractor = spyk(buildExtractor())
        every { extractor.getTables("cat1", "schema1", any()) } throws RuntimeException("S3 access denied")
        every { extractor.getTables("cat1", "schema2", any()) } returns listOf(mockTableRow("table_a"))

        stubDescribeTable("cat1", "schema2", "table_a")

        extractor.scrape(AppConfig())

        // The key assertion: schema2's table was synced despite schema1's failure
        verify { mockDataSync.syncTableData(match { it.name == "table_a" && it.schema == "schema2" }) }
    }

    // ─── Helper: run scrape with a spyk extractor ───

    private fun scrapeWith(
        catalogs: List<CoreServiceClient.CatalogDetails>,
        configure: (LakehouseMetadataExtractor) -> Unit = {}
    ) {
        every { mockCoreServiceClient.catalogs() } returns catalogs
        val extractor = spyk(buildExtractor())
        configure(extractor)
        extractor.scrape(AppConfig())
    }

    // ─── Behavior 2: Discovery failure marks schema as discoveryFailed ───

    @Test
    fun `scrape should sync schema metadata with discoveryFailed when getTables throws`() {
        val catalog = CoreServiceClient.CatalogDetails("cat1", listOf("iceberg"), null, null, listOf())
        every { mockCoreServiceClient.catalogs() } returns listOf(catalog)

        stubSchemas("cat1", "schema1")

        val extractor = spyk(buildExtractor())
        every { extractor.getTables("cat1", "schema1", any()) } throws RuntimeException("S3 access denied")

        extractor.scrape(AppConfig())

        // Schema metadata should be synced with discoveryFailed = true
        verify {
            mockDataSync.syncSchemaData(match {
                it.catalog == "cat1" && it.schema == "schema1" && it.discoveryFailed
            })
        }
    }

    // ─── Behavior 4: Phase 2 Row access failure doesn't drop sibling tables ───

    @Test
    fun `scrape should process other tables when Row getString throws for one table`() {
        val catalog = CoreServiceClient.CatalogDetails("cat1", listOf("iceberg"), null, null, listOf())
        every { mockCoreServiceClient.catalogs() } returns listOf(catalog)

        stubSchemas("cat1", "schema1")

        // Two table rows: first one has corrupted Row, second is fine
        val badRow = mockk<Row>()
        every { badRow.getString(1) } throws RuntimeException("Corrupted row data")
        val goodRow = mockTableRow("good_table")

        val extractor = spyk(buildExtractor())
        every { extractor.getTables("cat1", "schema1", any()) } returns listOf(badRow, goodRow)

        stubDescribeTable("cat1", "schema1", "good_table")

        extractor.scrape(AppConfig())

        // good_table should still be synced despite the bad row
        verify { mockDataSync.syncTableData(match { it.name == "good_table" }) }
    }

    // ─── Behavior 8: Phase 3 sync timeout ───

    @Test
    fun `scrape should timeout slow sync calls and track as sync failure`() {
        val catalog = CoreServiceClient.CatalogDetails("cat1", listOf("iceberg"), null, null, listOf())
        every { mockCoreServiceClient.catalogs() } returns listOf(catalog)

        stubSchemas("cat1", "schema1")

        val extractor = spyk(buildExtractor())
        every { extractor.getTables("cat1", "schema1", any()) } returns listOf(mockTableRow("table_a"))

        stubDescribeTable("cat1", "schema1", "table_a")

        // Override SYNC_TIMEOUT_SECONDS to 1 second
        val mockConfig = mockk<Config>()
        every { ConfigProvider.getConfig() } returns mockConfig
        every { mockConfig.getOptionalValue("HTTP_PARALLELISM", Int::class.java) } returns Optional.of(2)
        every { mockConfig.getOptionalValue("TABLE_PROCESS_TIMEOUT_SECONDS", Long::class.java) } returns Optional.of(60L)
        every { mockConfig.getOptionalValue("SYNC_TIMEOUT_SECONDS", Long::class.java) } returns Optional.of(1L)

        // syncTableData blocks for 5 seconds
        every { mockDataSync.syncTableData(any()) } answers {
            Thread.sleep(5000)
            true
        }

        extractor.scrape(AppConfig())

        // Should track as sync failure
        verify {
            mockDataSync.syncSchemaData(match {
                it.catalog == "cat1" && it.schema == "schema1" && it.syncFailedCount == 1
            })
        }
    }

    // ─── Behavior 7: Per-table timeout ───

    @Test
    fun `scrape should timeout slow tables and still process fast tables`() {
        val catalog = CoreServiceClient.CatalogDetails("cat1", listOf("iceberg"), null, null, listOf())
        every { mockCoreServiceClient.catalogs() } returns listOf(catalog)

        stubSchemas("cat1", "schema1")

        val extractor = spyk(buildExtractor())
        every { extractor.getTables("cat1", "schema1", any()) } returns listOf(
            mockTableRow("slow_table"),
            mockTableRow("fast_table")
        )

        // Override TABLE_PROCESS_TIMEOUT_SECONDS to 1 second for test
        val mockConfig = mockk<Config>()
        every { ConfigProvider.getConfig() } returns mockConfig
        every { mockConfig.getOptionalValue("HTTP_PARALLELISM", Int::class.java) } returns Optional.of(2)
        every { mockConfig.getOptionalValue("TABLE_PROCESS_TIMEOUT_SECONDS", Long::class.java) } returns Optional.of(1L)
        every { mockConfig.getOptionalValue("SYNC_TIMEOUT_SECONDS", Long::class.java) } returns Optional.of(60L)

        stubDescribeTable("cat1", "schema1", "fast_table")

        // slow_table blocks for 5 seconds in describeTable
        val slowDescribeDataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql("describe extended `cat1`.`schema1`.`slow_table`") } returns slowDescribeDataset
        every { slowDescribeDataset.collectAsList() } answers {
            Thread.sleep(5000)
            emptyList()
        }
        val slowExtractor = mockk<TableExtractor>()
        every { slowExtractor.getTableType } returns "MANAGED"
        every {
            mockTableExtractorFactory.extractorFor(
                provider = any(), isView = any(), catalog = "cat1",
                schema = "schema1", table = "slow_table", currentSnapshotId = any()
            )
        } returns slowExtractor

        extractor.scrape(AppConfig())

        // fast_table should be synced
        verify { mockDataSync.syncTableData(match { it.name == "fast_table" }) }
        // slow_table should NOT be synced (timed out)
        verify(exactly = 0) { mockDataSync.syncTableData(match { it.name == "slow_table" }) }
    }

    // ─── Behavior 6: Phase 3 sync failure tracked separately ───

    @Test
    fun `scrape should track sync failures separately from process failures`() {
        val catalog = CoreServiceClient.CatalogDetails("cat1", listOf("iceberg"), null, null, listOf())
        every { mockCoreServiceClient.catalogs() } returns listOf(catalog)

        stubSchemas("cat1", "schema1")

        val extractor = spyk(buildExtractor())
        every { extractor.getTables("cat1", "schema1", any()) } returns listOf(mockTableRow("table_a"))

        stubDescribeTable("cat1", "schema1", "table_a")

        // syncTableData throws — table processed OK but sync failed
        every { mockDataSync.syncTableData(any()) } throws RuntimeException("HTTP 503")

        extractor.scrape(AppConfig())

        // Schema metadata should show 0 process failures but sync failures tracked
        verify {
            mockDataSync.syncSchemaData(match {
                it.catalog == "cat1" && it.schema == "schema1" &&
                    it.failedTableCount == 0 && it.syncFailedCount > 0
            })
        }
    }

    // ─── Behavior 5: Row access failure produces error with tableName=unknown ───

    @Test
    fun `scrape should record error with unknown tableName when Row getString throws`() {
        val catalog = CoreServiceClient.CatalogDetails("cat1", listOf("iceberg"), null, null, listOf())
        every { mockCoreServiceClient.catalogs() } returns listOf(catalog)

        stubSchemas("cat1", "schema1")

        val badRow = mockk<Row>()
        every { badRow.getString(1) } throws RuntimeException("Corrupted row data")

        val extractor = spyk(buildExtractor())
        every { extractor.getTables("cat1", "schema1", any()) } returns listOf(badRow)

        extractor.scrape(AppConfig())

        // Schema metadata should report 1 failed table
        verify {
            mockDataSync.syncSchemaData(match {
                it.catalog == "cat1" && it.schema == "schema1" && it.failedTableCount == 1
            })
        }
    }

    // ─── Behavior 3: Catalog-level discovery failure ───

    @Test
    fun `scrape should sync catalog metadata with discoveryFailed when getSchemas throws`() {
        val catalog = CoreServiceClient.CatalogDetails("cat1", listOf("iceberg"), null, null, listOf())
        every { mockCoreServiceClient.catalogs() } returns listOf(catalog)

        // getSchemas throws, caught by scrape's try-catch which marks catalog as discoveryFailed
        every { mockSparkSession.sql("show databases in `cat1`") } throws RuntimeException("Connection refused")

        val extractor = spyk(buildExtractor())
        extractor.scrape(AppConfig())

        // Catalog metadata should be synced with discoveryFailed = true
        verify {
            mockDataSync.syncCatalogData(match {
                it.catalog == "cat1" && it.discoveryFailed
            })
        }
    }
}
