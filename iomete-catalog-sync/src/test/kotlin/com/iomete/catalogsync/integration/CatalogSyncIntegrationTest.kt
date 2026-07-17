package com.iomete.catalogsync.integration

import com.iomete.catalogsync.CatalogClient
import com.iomete.catalogsync.CoreClient
import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.extract.TableExtractorFactory
import com.iomete.catalogsync.extract.datasets.IcebergTableExtractor
import com.iomete.catalogsync.metadata.CatalogMetadata
import com.iomete.catalogsync.metadata.IcebergMetadataReader
import com.iomete.catalogsync.metadata.MetadataScraper
import com.iomete.catalogsync.metadata.SchemaMetadata
import com.iomete.catalogsync.metadata.SparkMetadataReader
import com.iomete.catalogsync.metadata.TableMetadata
import com.iomete.catalogsync.metadata.TableMetadataExtractor
import com.iomete.catalogsync.presidio.EntityType
import com.iomete.catalogsync.presidio.PIIDetectionService
import com.iomete.catalogsync.presidio.PresidioClient
import com.iomete.catalogsync.presidio.PresidioResponse
import io.mockk.every
import io.mockk.mockk
import io.mockk.slot
import io.mockk.verify
import jakarta.ws.rs.core.Response
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@Tag("integration")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CatalogSyncIntegrationTest {

    private lateinit var spark: SparkSession
    private lateinit var sparkMetadataReader: SparkMetadataReader
    private lateinit var tableExtractorFactory: TableExtractorFactory
    private lateinit var tableMetadataExtractor: TableMetadataExtractor
    private lateinit var piiDetectionService: PIIDetectionService
    private lateinit var applicationConfig: ApplicationConfig

    private val presidioClient: PresidioClient = mockk(relaxed = true)

    @BeforeAll
    fun setup() {
        spark = SparkTestSupport.getOrCreateSpark()
        TestDataSetup.createTestData(spark)

        sparkMetadataReader = SparkMetadataReader()
        tableExtractorFactory = TableExtractorFactory()
        applicationConfig = ApplicationConfig()
        piiDetectionService = PIIDetectionService(presidioClient)
        tableMetadataExtractor = TableMetadataExtractor(
            tableExtractorFactory = tableExtractorFactory,
            piiDetectionService = piiDetectionService,
            applicationConfig = applicationConfig,
            sparkMetadataReader = sparkMetadataReader,
            icebergMetadataReader = IcebergMetadataReader(),
        )
    }

    @AfterAll
    fun teardown() {
        SparkTestSupport.cleanup()
    }

    @Test
    fun `getSchemas discovers all namespaces`() {
        val schemas = sparkMetadataReader.getSchemas(spark, "test_catalog")
        assertTrue(schemas.contains("test_db"), "Should contain test_db")
        assertTrue(schemas.contains("another_db"), "Should contain another_db")
        assertEquals(2, schemas.size)
    }

    @Test
    fun `getTables discovers all tables in a schema`() {
        val catalog = CatalogDetails(name = "test_catalog", type = listOf("iceberg"))
        val tables = sparkMetadataReader.getTables(spark, catalog, "test_db")
        val tableNames = tables.map { it.name }.toSet()

        assertTrue(tableNames.contains("users"), "Should contain users table")
        assertTrue(tableNames.contains("events"), "Should contain events table")
        assertTrue(tableNames.contains("empty_table"), "Should contain empty_table")
        assertEquals(3, tables.size)
    }

    @Test
    fun `describeTable returns correct columns`() {
        val description = sparkMetadataReader.describeTable(spark, "test_catalog", "test_db", "users")

        val columnNames = description.columns.map { it.name }
        assertEquals(listOf("id", "name", "age", "salary", "is_active", "created_at", "email"), columnNames)

        val columnTypes = description.columns.associate { it.name to it.dataType }
        assertEquals("bigint", columnTypes["id"])
        assertEquals("string", columnTypes["name"])
        assertEquals("int", columnTypes["age"])
        assertEquals("double", columnTypes["salary"])
        assertEquals("boolean", columnTypes["is_active"])
        assertEquals("timestamp", columnTypes["created_at"])
        assertEquals("string", columnTypes["email"])

        // Verify sort order is sequential
        description.columns.forEachIndexed { index, col ->
            assertEquals(index, col.sortOrder, "Column ${col.name} should have sortOrder=$index")
        }
    }

    @Test
    fun `describeTable detects partition columns`() {
        val description = sparkMetadataReader.describeTable(spark, "test_catalog", "test_db", "events")

        val eventDateCol = description.columns.find { it.name == "event_date" }
        assertNotNull(eventDateCol, "event_date column should exist")
        assertTrue(eventDateCol!!.isPartitionKey, "event_date should be marked as partition key")

        val eventIdCol = description.columns.find { it.name == "event_id" }
        assertFalse(eventIdCol!!.isPartitionKey, "event_id should not be a partition key")
    }

    @Test
    fun `IcebergTableExtractor extracts real snapshot stats`() {
        val extractor = IcebergTableExtractor(
            spark = spark,
            catalog = "test_catalog",
            schema = "test_db",
            table = "users",
        )

        val stats = extractor.extractTableStatistics()

        assertNotNull(stats, "Stats should not be null for a table with data")
        assertEquals(5L, stats!!.totalRecords, "users table should have 5 total records")
        assertTrue(stats.sizeInBytes!! > 0, "sizeInBytes should be positive")
        assertTrue(stats.numFiles!! > 0, "numFiles should be positive")
        assertTrue(stats.lastModified!! > 0, "lastModified should be positive")
        assertTrue(stats.totalTableSizeInBytes!! > 0, "totalTableSizeInBytes should be positive")
        assertTrue(stats.totalTableNumFiles!! > 0, "totalTableNumFiles should be positive")
    }

    @Test
    fun `IcebergTableExtractor handles empty table`() {
        val extractor = IcebergTableExtractor(
            spark = spark,
            catalog = "test_catalog",
            schema = "test_db",
            table = "empty_table",
        )

        val stats = extractor.extractTableStatistics()
        assertNull(stats, "Stats should be null for an empty table with no snapshots")
    }

    @Test
    fun `scrapeTable returns complete TableMetadata`() {
        val metadata = tableMetadataExtractor.scrapeTable(
            spark = spark,
            catalog = "test_catalog",
            schema = "test_db",
            tableName = "users",
            isTemp = false,
            useIcebergFastPath = true,
        )

        assertEquals("test_catalog", metadata.catalog)
        assertEquals("test_db", metadata.schema)
        assertEquals("users", metadata.name)
        assertEquals("iceberg", metadata.provider)
        assertFalse(metadata.isView)
        assertFalse(metadata.isTemporary)
        assertEquals("Users table comment", metadata.description)
        assertEquals(7, metadata.columns.size)
        assertEquals("user id", metadata.columns.single { it.name == "id" }.description)
        assertEquals("email address", metadata.columns.single { it.name == "email" }.description)

        // Stats should be present from real Iceberg snapshots
        assertNotNull(metadata.totalRecords, "totalRecords should be present")
        assertEquals(5L, metadata.totalRecords)
        assertNotNull(metadata.sizeInBytes, "sizeInBytes should be present")
        assertNotNull(metadata.numFiles, "numFiles should be present")
        assertNotNull(metadata.lastModified, "lastModified should be present")
    }

    @Test
    fun `scrapeTable handles partitioned table`() {
        val metadata = tableMetadataExtractor.scrapeTable(
            spark = spark,
            catalog = "test_catalog",
            schema = "test_db",
            tableName = "events",
            isTemp = false,
            useIcebergFastPath = true,
        )

        val partitionCol = metadata.columns.find { it.name == "event_date" }
        assertNotNull(partitionCol, "event_date column should exist")
        assertTrue(partitionCol!!.isPartitionKey, "event_date should be partition key")

        assertEquals(2L, metadata.totalRecords, "events table should have 2 records")
    }

    @Test
    fun `scrapeTable handles empty table gracefully`() {
        val metadata = tableMetadataExtractor.scrapeTable(
            spark = spark,
            catalog = "test_catalog",
            schema = "test_db",
            tableName = "empty_table",
            isTemp = false,
            useIcebergFastPath = true,
        )

        assertEquals(2, metadata.columns.size, "empty_table should have 2 columns")
        assertNull(metadata.totalRecords, "empty_table should have null totalRecords")
        assertNull(metadata.sizeInBytes, "empty_table should have null sizeInBytes")
        assertNull(metadata.numFiles, "empty_table should have null numFiles")
    }

    @Test
    fun `iceberg fast path preserves Spark path table metadata shape`() {
        val sparkPath = tableMetadataExtractor.scrapeTable(
            spark = spark,
            catalog = "test_catalog",
            schema = "test_db",
            tableName = "users",
            isTemp = false,
            useIcebergFastPath = false,
        )
        val fastPath = tableMetadataExtractor.scrapeTable(
            spark = spark,
            catalog = "test_catalog",
            schema = "test_db",
            tableName = "users",
            isTemp = false,
            useIcebergFastPath = true,
        )

        assertEquals(sparkPath.catalog, fastPath.catalog)
        assertEquals(sparkPath.schema, fastPath.schema)
        assertEquals(sparkPath.name, fastPath.name)
        assertEquals(sparkPath.tableType, fastPath.tableType)
        assertEquals(sparkPath.provider, fastPath.provider)
        assertEquals(sparkPath.isView, fastPath.isView)
        assertEquals(sparkPath.columns.map { it.name }, fastPath.columns.map { it.name })
        assertEquals(sparkPath.columns.map { it.dataType }, fastPath.columns.map { it.dataType })
        assertEquals(sparkPath.columns.map { it.description }, fastPath.columns.map { it.description })
        assertEquals(sparkPath.columns.map { it.sortOrder }, fastPath.columns.map { it.sortOrder })
        assertEquals(sparkPath.columns.map { it.isPartitionKey }, fastPath.columns.map { it.isPartitionKey })
        assertEquals(sparkPath.totalRecords, fastPath.totalRecords)
        assertEquals(sparkPath.numFiles, fastPath.numFiles)
        assertEquals(sparkPath.sizeInBytes, fastPath.sizeInBytes)
        assertEquals(sparkPath.totalTableNumFiles, fastPath.totalTableNumFiles)
        assertEquals(sparkPath.totalTableSizeInBytes, fastPath.totalTableSizeInBytes)
    }

    @Test
    fun `iceberg fast path preserves PII tags`() {
        System.setProperty("piiDetectionEnabled", "true")
        try {
            every { presidioClient.analyze(any()) } answers {
                val text = firstArg<com.iomete.catalogsync.presidio.PresidioRequest>().text
                if (text.contains("@")) {
                    listOf(PresidioResponse(entityType = EntityType.EMAIL_ADDRESS, score = 0.95f))
                } else {
                    emptyList()
                }
            }
            val piiEnabledExtractor = TableMetadataExtractor(
                tableExtractorFactory = tableExtractorFactory,
                piiDetectionService = PIIDetectionService(presidioClient),
                applicationConfig = applicationConfig,
                sparkMetadataReader = sparkMetadataReader,
                icebergMetadataReader = IcebergMetadataReader(),
            )

            val metadata = piiEnabledExtractor.scrapeTable(
                spark = spark,
                catalog = "test_catalog",
                schema = "test_db",
                tableName = "users",
                isTemp = false,
                useIcebergFastPath = true,
            )

            val emailColumn = metadata.columns.single { it.name == "email" }
            assertTrue(emailColumn.tags.contains("DETECTED:EMAIL_ADDRESS"))
            assertTrue(emailColumn.tags.contains("DETECTED:PII"))
            assertTrue(metadata.tags.contains("DETECTED:PII"))
        } finally {
            System.clearProperty("piiDetectionEnabled")
        }
    }

    @Test
    fun `full pipeline scrapes catalog and invokes indexing`() {
        val coreClient: CoreClient = mockk()
        val catalogClient: CatalogClient = mockk()
        val sparkSessionProvider: SparkSessionProvider = mockk()
        val mockResponse: Response = mockk(relaxed = true)

        val testCatalog = CatalogDetails(
            name = "test_catalog",
            type = listOf("iceberg"),
        )

        every { coreClient.catalogs() } returns listOf(testCatalog)
        every { sparkSessionProvider.getSession(any()) } returns spark

        val capturedTables = mutableListOf<TableMetadata>()
        val capturedSchemas = mutableListOf<SchemaMetadata>()
        val capturedCatalogs = mutableListOf<CatalogMetadata>()

        every { catalogClient.indexTable(capture(capturedTables)) } returns mockResponse
        every { catalogClient.indexSchema(capture(capturedSchemas)) } returns mockResponse
        every { catalogClient.indexCatalog(capture(capturedCatalogs)) } returns mockResponse

        val scraper = MetadataScraper(
            sparkSessionProvider = sparkSessionProvider,
            applicationConfig = applicationConfig,
            sparkMetadataReader = sparkMetadataReader,
            tableMetadataExtractor = tableMetadataExtractor,
            coreServiceClient = coreClient,
            catalogServiceClient = catalogClient,
        )

        try {
            scraper.run()

            // Verify all 4 tables were indexed
            assertEquals(4, capturedTables.size, "Should index 4 tables total")
            val indexedTableNames = capturedTables.map { "${it.schema}.${it.name}" }.toSet()
            assertTrue(indexedTableNames.contains("test_db.users"))
            assertTrue(indexedTableNames.contains("test_db.events"))
            assertTrue(indexedTableNames.contains("test_db.empty_table"))
            assertTrue(indexedTableNames.contains("another_db.products"))

            val indexedUsers = capturedTables.single { it.schema == "test_db" && it.name == "users" }
            assertEquals("Users table comment", indexedUsers.description)
            assertEquals(5L, indexedUsers.totalRecords)
            assertTrue(indexedUsers.numFiles!! > 0)
            assertTrue(indexedUsers.sizeInBytes!! > 0)
            assertEquals("email address", indexedUsers.columns.single { it.name == "email" }.description)

            val indexedEvents = capturedTables.single { it.schema == "test_db" && it.name == "events" }
            assertTrue(indexedEvents.columns.single { it.name == "event_date" }.isPartitionKey)

            val indexedEmptyTable = capturedTables.single { it.schema == "test_db" && it.name == "empty_table" }
            assertNull(indexedEmptyTable.totalRecords)
            assertNull(indexedEmptyTable.numFiles)
            assertNull(indexedEmptyTable.sizeInBytes)

            // Verify 2 schemas were indexed
            assertEquals(2, capturedSchemas.size, "Should index 2 schemas")
            val indexedSchemaNames = capturedSchemas.map { it.schema }.toSet()
            assertTrue(indexedSchemaNames.contains("test_db"))
            assertTrue(indexedSchemaNames.contains("another_db"))

            // Verify 1 catalog was indexed
            assertEquals(1, capturedCatalogs.size, "Should index 1 catalog")
            val catalogMeta = capturedCatalogs[0]
            assertEquals("test_catalog", catalogMeta.catalog)
            assertEquals(2, catalogMeta.totalSchemaCount)
            assertEquals(4, catalogMeta.totalTableCount)

            // Verify catalog aggregated stats
            assertTrue(catalogMeta.totalSizeInBytes > 0, "Catalog totalSizeInBytes should be positive")
            assertTrue(catalogMeta.totalFiles > 0, "Catalog totalFiles should be positive")

            verify(exactly = 4) { catalogClient.indexTable(any()) }
            verify(exactly = 2) { catalogClient.indexSchema(any()) }
            verify(exactly = 1) { catalogClient.indexCatalog(any()) }
        } finally {
            scraper.shutdown()
        }
    }
}
