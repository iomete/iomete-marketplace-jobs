package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CatalogClient
import com.iomete.catalogsync.CoreClient
import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.CatalogExclusionRule
import com.iomete.catalogsync.config.ExclusionRules
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.ws.rs.core.Response
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class MetadataScraperTest {
    private lateinit var mockSparkSession: SparkSession
    private lateinit var mockSparkContext: SparkContext
    private lateinit var mockCatalogServiceClient: CatalogClient
    private lateinit var mockSparkSessionProvider: SparkSessionProvider
    private lateinit var mockApplicationConfig: ApplicationConfig
    private lateinit var mockCoreServiceClient: CoreClient
    private lateinit var mockSparkMetadataReader: SparkMetadataReader
    private lateinit var mockTableMetadataExtractor: TableMetadataExtractor
    private lateinit var scraper: MetadataScraper

    private val testCatalog = CatalogDetails(
        name = "test_catalog",
        type = listOf("INTERNAL", "ICEBERG"),
        location = "s3://bucket/path",
        storageEndpoint = null,
        sparkProperties = emptyMap(),
    )

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk()
        mockSparkContext = mockk()
        mockCatalogServiceClient = mockk()
        mockSparkSessionProvider = mockk()
        mockApplicationConfig = mockk()
        mockCoreServiceClient = mockk()
        mockSparkMetadataReader = mockk()
        mockTableMetadataExtractor = mockk()

        every { mockSparkSessionProvider.getSession(any()) } returns mockSparkSession
        every { mockApplicationConfig.exclusionRules } returns ExclusionRules()
        every { mockSparkSession.sparkContext() } returns mockSparkContext
        every { mockSparkContext.applicationId() } returns "app-test-123"

        scraper = MetadataScraper(
            mockSparkSessionProvider,
            mockApplicationConfig,
            mockSparkMetadataReader,
            mockTableMetadataExtractor,
            mockCoreServiceClient,
            mockCatalogServiceClient,
        )
    }

    @AfterEach
    fun teardown() {
        scraper.shutdown()
    }

    @Test
    fun `run processes all schemas and indexes tables and schemas`() {
        val table1 = makeTableMetadata("test_catalog", "schema1", "table1")
        val table2 = makeTableMetadata("test_catalog", "schema2", "table2")

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns listOf("schema1", "schema2")
        stubSchema("test_catalog", "schema1", listOf("table1"), listOf(table1))
        stubSchema("test_catalog", "schema2", listOf("table2"), listOf(table2))

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexTable(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexSchema(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        verify(exactly = 1) { mockCatalogServiceClient.indexTable(match { it.name == "table1" }) }
        verify(exactly = 1) { mockCatalogServiceClient.indexTable(match { it.name == "table2" }) }
        verify(exactly = 2) { mockCatalogServiceClient.indexSchema(any()) }
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "test_catalog" }) }
    }

    @Test
    fun `run skips excluded catalogs`() {
        val excludedCatalog = CatalogDetails(
            name = "excluded_catalog",
            type = listOf("INTERNAL"),
        )
        every { mockApplicationConfig.exclusionRules } returns ExclusionRules(
            catalogs = CatalogExclusionRule(names = listOf("excluded_catalog")),
        )
        // Recreate scraper with the updated config
        scraper.shutdown()
        scraper = MetadataScraper(
            mockSparkSessionProvider, mockApplicationConfig, mockSparkMetadataReader,
            mockTableMetadataExtractor, mockCoreServiceClient, mockCatalogServiceClient,
        )

        every { mockCoreServiceClient.catalogs() } returns listOf(excludedCatalog, testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns emptyList()

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        verify(exactly = 0) { mockSparkMetadataReader.getSchemas(mockSparkSession, "excluded_catalog") }
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "test_catalog" }) }
    }

    @Test
    fun `run filters out excluded schemas but processes remaining ones`() {
        val table1 = makeTableMetadata("test_catalog", "public", "users")

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns listOf("public", "excluded_schema")

        // "public" schema passes exclusion rules
        stubSchema("test_catalog", "public", listOf("users"), listOf(table1))
        // "excluded_schema" has properties that trigger exclusion
        every {
            mockSparkMetadataReader.getSchemaProperties(mockSparkSession, "test_catalog", "excluded_schema")
        } returns mapOf("iomete.governance.index" to "false")

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexTable(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexSchema(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        verify(exactly = 1) { mockCatalogServiceClient.indexTable(match { it.name == "users" }) }
        // Only one schema should be indexed (the non-excluded one)
        verify(exactly = 1) { mockCatalogServiceClient.indexSchema(match { it.schema == "public" }) }
        verify(exactly = 0) { mockCatalogServiceClient.indexSchema(match { it.schema == "excluded_schema" }) }
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `run continues processing other schemas when one schema throws non-excluded exception`() {
        val table1 = makeTableMetadata("test_catalog", "good_schema", "table1")

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns listOf("good_schema", "bad_schema")

        stubSchema("test_catalog", "good_schema", listOf("table1"), listOf(table1))
        // bad_schema throws a RuntimeException (not ExcludedItemException) during processSchema
        // The error is caught per-schema so it doesn't crash the entire catalog
        every {
            mockSparkMetadataReader.getSchemaProperties(mockSparkSession, "test_catalog", "bad_schema")
        } throws RuntimeException("Schema read failure")

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexTable(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexSchema(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        // good_schema should still be indexed despite bad_schema failing
        verify(exactly = 1) { mockCatalogServiceClient.indexTable(match { it.name == "table1" }) }
        verify(exactly = 1) { mockCatalogServiceClient.indexSchema(match { it.schema == "good_schema" }) }
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.totalSchemaCount == 1 }) }
    }

    @Test
    fun `run completes even when table indexing HTTP call fails`() {
        val table1 = makeTableMetadata("test_catalog", "schema1", "good_table")
        val table2 = makeTableMetadata("test_catalog", "schema1", "bad_table")

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns listOf("schema1")
        stubSchema("test_catalog", "schema1", listOf("good_table", "bad_table"), listOf(table1, table2))

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexTable(match { it.name == "good_table" }) } returns mockResponse
        every { mockCatalogServiceClient.indexTable(match { it.name == "bad_table" }) } throws RuntimeException("HTTP 500")
        every { mockCatalogServiceClient.indexSchema(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        // Both indexTable calls were attempted
        verify(exactly = 1) { mockCatalogServiceClient.indexTable(match { it.name == "good_table" }) }
        verify(exactly = 1) { mockCatalogServiceClient.indexTable(match { it.name == "bad_table" }) }
        // Catalog indexing still happens despite the table indexing failure
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `run completes even when schema indexing HTTP call fails`() {
        val table1 = makeTableMetadata("test_catalog", "schema1", "table1")

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns listOf("schema1")
        stubSchema("test_catalog", "schema1", listOf("table1"), listOf(table1))

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexTable(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexSchema(any()) } throws RuntimeException("HTTP 500")
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        verify(exactly = 1) { mockCatalogServiceClient.indexSchema(any()) }
        // Catalog indexing still happens
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `run completes even when catalog indexing HTTP call fails`() {
        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns emptyList()

        every { mockCatalogServiceClient.indexCatalog(any()) } throws RuntimeException("HTTP 500")

        // Should not throw
        scraper.run()

        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `run processes multiple catalogs independently`() {
        val catalog2 = CatalogDetails(name = "catalog2", type = listOf("EXTERNAL"))

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog, catalog2)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns emptyList()
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "catalog2") } returns emptyList()

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "test_catalog" }) }
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "catalog2" }) }
    }

    @Test
    fun `run continues processing second catalog when first catalog fails entirely`() {
        val catalog2 = CatalogDetails(name = "catalog2", type = listOf("EXTERNAL"))

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog, catalog2)
        // First catalog fails at getSchemas
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } throws RuntimeException("Spark failure")
        // Second catalog succeeds
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "catalog2") } returns emptyList()

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        // First catalog should not have been indexed
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "test_catalog" }) }
        // Second catalog should still be indexed
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "catalog2" }) }
    }

    // --- helpers ---

    private fun stubSchema(
        catalogName: String,
        schemaName: String,
        tableNames: List<String>,
        tableMetadataList: List<TableMetadata>,
    ) {
        every {
            mockSparkMetadataReader.getSchemaProperties(mockSparkSession, catalogName, schemaName)
        } returns emptyMap()

        val showTablesRows = tableNames.map { ShowTablesRow(name = it, isTemp = false) }
        every {
            mockSparkMetadataReader.getTables(mockSparkSession, match { it.name == catalogName }, schemaName)
        } returns showTablesRows

        tableMetadataList.forEach { tm ->
            every {
                mockTableMetadataExtractor.scrapeTable(mockSparkSession, catalogName, schemaName, tm.name, false)
            } returns tm
        }
    }

    private fun makeTableMetadata(catalog: String, schema: String, name: String) = TableMetadata(
        catalog = catalog,
        schema = schema,
        name = name,
        description = null,
        tableType = "MANAGED",
        isView = false,
        isTemporary = false,
        owner = "test_owner",
        provider = "iceberg",
        viewText = null,
        sizeInBytes = 1024L,
        numFiles = 10L,
        totalTableSizeInBytes = 1024L,
        totalTableNumFiles = 10L,
        totalRecords = 100L,
        columns = emptyList(),
        tags = emptyList(),
        syncTime = System.currentTimeMillis(),
        sparkApplicationId = "app-test-123",
    )
}
