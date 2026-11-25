package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.CatalogClient
import com.iomete.catalogsync.CoreClient
import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.CatalogExclusionRule
import com.iomete.catalogsync.config.CatalogInclusionRule
import com.iomete.catalogsync.config.ExclusionRules
import com.iomete.catalogsync.config.InclusionRules
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import jakarta.ws.rs.core.Response
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

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
        every { mockApplicationConfig.inclusionRules } returns InclusionRules()
        every { mockApplicationConfig.exclusionRules } returns ExclusionRules()
        every { mockSparkSession.sparkContext() } returns mockSparkContext
        every { mockSparkContext.applicationId() } returns "app-test-123"
        every { mockCoreServiceClient.domain("default") } returns CoreClient.Domain(isPrivate = false)

        scraper = MetadataScraper(
            mockSparkSessionProvider,
            mockApplicationConfig,
            mockSparkMetadataReader,
            mockTableMetadataExtractor,
            mockCoreServiceClient,
            mockCatalogServiceClient,
            "default",
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
        verify(exactly = 0) { mockCoreServiceClient.ownedCatalogs(any()) }
    }

    @Test
    fun `run enables iceberg fast path only for non-view tables in iceberg catalogs`() {
        val table = makeTableMetadata("test_catalog", "schema1", "table1")
        val view = makeTableMetadata("test_catalog", "schema1", "view1").copy(tableType = "view", isView = true)

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns listOf("schema1")
        every { mockSparkMetadataReader.getSchemaProperties(mockSparkSession, "test_catalog", "schema1") } returns emptyMap()
        every {
            mockSparkMetadataReader.getTables(mockSparkSession, match { it.name == "test_catalog" }, "schema1")
        } returns listOf(
            ShowTablesRow(name = "table1", isTemp = false, isView = false),
            ShowTablesRow(name = "view1", isTemp = false, isView = true),
        )
        every {
            mockTableMetadataExtractor.scrapeTable(
                spark = mockSparkSession,
                catalog = "test_catalog",
                schema = "schema1",
                tableName = "table1",
                isTemp = false,
                useIcebergFastPath = true,
            )
        } returns table
        every {
            mockTableMetadataExtractor.scrapeTable(
                spark = mockSparkSession,
                catalog = "test_catalog",
                schema = "schema1",
                tableName = "view1",
                isTemp = false,
                useIcebergFastPath = false,
            )
        } returns view

        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexTable(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexSchema(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        verify(exactly = 1) {
            mockTableMetadataExtractor.scrapeTable(
                spark = mockSparkSession,
                catalog = "test_catalog",
                schema = "schema1",
                tableName = "table1",
                isTemp = false,
                useIcebergFastPath = true,
            )
        }
        verify(exactly = 1) {
            mockTableMetadataExtractor.scrapeTable(
                spark = mockSparkSession,
                catalog = "test_catalog",
                schema = "schema1",
                tableName = "view1",
                isTemp = false,
                useIcebergFastPath = false,
            )
        }
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
            mockTableMetadataExtractor, mockCoreServiceClient, mockCatalogServiceClient, "default",
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
    fun `run syncs only the included catalogs`() {
        val otherCatalog = CatalogDetails(name = "other_catalog", type = listOf("INTERNAL"))
        every { mockApplicationConfig.inclusionRules } returns InclusionRules(
            catalogs = CatalogInclusionRule(names = listOf("test_catalog", "missing_catalog")),
        )
        recreateScraper()

        every { mockCoreServiceClient.catalogs() } returns listOf(otherCatalog, testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns emptyList()
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockk<Response>()

        scraper.run()

        verify(exactly = 0) { mockSparkMetadataReader.getSchemas(mockSparkSession, "other_catalog") }
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "test_catalog" }) }
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `run fails when none of the included catalogs exist`() {
        every { mockApplicationConfig.inclusionRules } returns InclusionRules(
            catalogs = CatalogInclusionRule(names = listOf("missing_catalog")),
        )
        recreateScraper()

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)

        val error = assertThrows(IllegalStateException::class.java) { scraper.run() }

        assertEquals(
            "None of the included catalogs can be synced by this job: [missing_catalog]. Catalogs this job can sync: [test_catalog]",
            error.message,
        )

        verify(exactly = 0) { mockSparkMetadataReader.getSchemas(any(), any()) }
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `run applies exclusion rules to included catalogs`() {
        every { mockApplicationConfig.inclusionRules } returns InclusionRules(
            catalogs = CatalogInclusionRule(names = listOf("test_catalog")),
        )
        every { mockApplicationConfig.exclusionRules } returns ExclusionRules(
            catalogs = CatalogExclusionRule(names = listOf("test_catalog")),
        )
        recreateScraper()

        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)

        scraper.run()

        verify(exactly = 0) { mockSparkMetadataReader.getSchemas(any(), any()) }
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `run does not sync an included catalog that an exclusion property matches`() {
        val hotCatalog = CatalogDetails(
            name = "hot_catalog",
            type = listOf("INTERNAL"),
            sparkProperties = mapOf("iomete.governance.index" to "false"),
        )
        every { mockApplicationConfig.inclusionRules } returns InclusionRules(
            catalogs = CatalogInclusionRule(names = listOf("hot_catalog")),
        )
        recreateScraper()

        every { mockCoreServiceClient.catalogs() } returns listOf(hotCatalog, testCatalog)

        scraper.run()

        verify(exactly = 0) { mockSparkMetadataReader.getSchemas(any(), any()) }
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    private fun recreateScraper() {
        scraper.shutdown()
        scraper = MetadataScraper(
            mockSparkSessionProvider, mockApplicationConfig, mockSparkMetadataReader,
            mockTableMetadataExtractor, mockCoreServiceClient, mockCatalogServiceClient, "default",
        )
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

    @Test
    fun `a private domain syncs only its own catalogs that the job was launched with`() {
        val owned = CatalogDetails(name = "owned", type = listOf("iceberg"), storageEndpoint = "http://minio:9000")
        val notLaunched = CatalogDetails(name = "not_launched", type = listOf("iceberg"))
        every { mockCoreServiceClient.domain("private-a") } returns CoreClient.Domain(isPrivate = true)
        every { mockCoreServiceClient.ownedCatalogs("private-a") } returns CoreClient.OwnedCatalogs(listOf(owned, notLaunched))
        every { mockSparkSessionProvider.isLaunchCatalog("owned") } returns true
        every { mockSparkSessionProvider.isLaunchCatalog("not_launched") } returns false
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "owned") } returns emptyList()
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockk()

        scraperIn("private-a").run()

        verify(exactly = 0) { mockCoreServiceClient.catalogs() }
        verify(exactly = 1) {
            mockCatalogServiceClient.indexCatalog(match { it.catalog == "owned" && it.storageEndpoint == "http://minio:9000" })
        }
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "not_launched" }) }
    }

    @Test
    fun `a private domain fails when its included catalog exists only outside the domain`() {
        val owned = CatalogDetails(name = "owned", type = listOf("iceberg"))
        every { mockApplicationConfig.inclusionRules } returns InclusionRules(
            catalogs = CatalogInclusionRule(names = listOf("other_domain_catalog")),
        )
        every { mockCoreServiceClient.domain("private-a") } returns CoreClient.Domain(isPrivate = true)
        every { mockCoreServiceClient.ownedCatalogs("private-a") } returns CoreClient.OwnedCatalogs(listOf(owned))
        every { mockCoreServiceClient.catalogs() } returns listOf(owned, CatalogDetails(name = "other_domain_catalog", type = listOf("iceberg")))
        every { mockSparkSessionProvider.isLaunchCatalog("owned") } returns true

        val error = assertThrows<IllegalStateException> { scraperIn("private-a").run() }

        assertTrue(
            error.message!!.endsWith(
                "Catalogs this job can sync: [owned] (a private domain syncs only the catalogs it owns that the job was launched with)",
            ),
            error.message,
        )
        verify(exactly = 0) { mockCoreServiceClient.catalogs() }
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `a private domain fails when its only included catalog is owned but not launched`() {
        val notLaunched = CatalogDetails(name = "not_launched", type = listOf("iceberg"))
        every { mockApplicationConfig.inclusionRules } returns InclusionRules(
            catalogs = CatalogInclusionRule(names = listOf("not_launched")),
        )
        every { mockCoreServiceClient.domain("private-a") } returns CoreClient.Domain(isPrivate = true)
        every { mockCoreServiceClient.ownedCatalogs("private-a") } returns CoreClient.OwnedCatalogs(listOf(notLaunched))
        every { mockSparkSessionProvider.isLaunchCatalog("not_launched") } returns false

        val error = assertThrows<IllegalStateException> { scraperIn("private-a").run() }

        assertTrue(error.message!!.startsWith("None of the included catalogs can be synced by this job: [not_launched]"), error.message)
        assertTrue(error.message!!.contains("a private domain syncs only"), error.message)
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    // Pins a known gap: iom-core's owned-catalogs response carries no catalog properties, so catalog-level
    // property rules (including the default `iomete.governance.index=false`) cannot match in a private domain.
    // Flip this test when owned-catalogs starts returning the properties.
    @Test
    fun `a private domain cannot exclude a catalog by its properties because owned-catalogs returns none`() {
        val owned = CatalogDetails(name = "owned", type = listOf("iceberg"))
        every { mockApplicationConfig.inclusionRules } returns InclusionRules(
            catalogs = CatalogInclusionRule(names = listOf("owned")),
        )
        every { mockCoreServiceClient.domain("private-a") } returns CoreClient.Domain(isPrivate = true)
        every { mockCoreServiceClient.ownedCatalogs("private-a") } returns CoreClient.OwnedCatalogs(listOf(owned))
        every { mockSparkSessionProvider.isLaunchCatalog("owned") } returns true
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "owned") } returns emptyList()
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockk()

        scraperIn("private-a").run()

        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.catalog == "owned" }) }
    }

    @Test
    fun `a failed private-domain lookup stops the run before anything is indexed`() {
        every { mockCoreServiceClient.domain("private-a") } throws RuntimeException("iom-core unavailable")

        assertThrows<RuntimeException> { scraperIn("private-a").run() }

        verify(exactly = 0) { mockCoreServiceClient.catalogs() }
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `a lookup without the private flag stops the run before anything is indexed`() {
        every { mockCoreServiceClient.domain("private-a") } returns CoreClient.Domain(isPrivate = null)

        assertThrows<IllegalStateException> { scraperIn("private-a").run() }

        verify(exactly = 0) { mockCoreServiceClient.catalogs() }
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `a catalog whose schemas can't be listed is not reported`() {
        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } throws RuntimeException("access denied")

        scraper.run()

        verify(exactly = 0) { mockCatalogServiceClient.indexSchema(any()) }
        verify(exactly = 0) { mockCatalogServiceClient.indexCatalog(any()) }
    }

    @Test
    fun `a schema whose tables can't be listed is not reported, so its indexed tables stay`() {
        val table1 = makeTableMetadata("test_catalog", "schema1", "table1")
        every { mockCoreServiceClient.catalogs() } returns listOf(testCatalog)
        every { mockSparkMetadataReader.getSchemas(mockSparkSession, "test_catalog") } returns listOf("schema1", "denied")
        stubSchema("test_catalog", "schema1", listOf("table1"), listOf(table1))
        every { mockSparkMetadataReader.getSchemaProperties(mockSparkSession, "test_catalog", "denied") } returns emptyMap()
        every {
            mockSparkMetadataReader.getTables(mockSparkSession, match { it.name == "test_catalog" }, "denied")
        } throws RuntimeException("access denied")
        val mockResponse = mockk<Response>()
        every { mockCatalogServiceClient.indexTable(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexSchema(any()) } returns mockResponse
        every { mockCatalogServiceClient.indexCatalog(any()) } returns mockResponse

        scraper.run()

        verify(exactly = 1) { mockCatalogServiceClient.indexSchema(match { it.schema == "schema1" }) }
        verify(exactly = 0) { mockCatalogServiceClient.indexSchema(match { it.schema == "denied" }) }
        verify(exactly = 1) { mockCatalogServiceClient.indexCatalog(match { it.totalSchemaCount == 1 }) }
    }

    // --- helpers ---

    private fun scraperIn(domain: String): MetadataScraper {
        scraper.shutdown()
        scraper = MetadataScraper(
            mockSparkSessionProvider, mockApplicationConfig, mockSparkMetadataReader,
            mockTableMetadataExtractor, mockCoreServiceClient, mockCatalogServiceClient, domain,
        )
        return scraper
    }

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
                mockTableMetadataExtractor.scrapeTable(
                    spark = mockSparkSession,
                    catalog = catalogName,
                    schema = schemaName,
                    tableName = tm.name,
                    isTemp = false,
                    useIcebergFastPath = any(),
                )
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
