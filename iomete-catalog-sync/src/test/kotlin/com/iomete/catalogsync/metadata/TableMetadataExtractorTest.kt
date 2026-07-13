package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.DefaultRule
import com.iomete.catalogsync.config.ExcludedItemException
import com.iomete.catalogsync.config.ExclusionRules
import com.iomete.catalogsync.config.GeneralFilter
import com.iomete.catalogsync.extract.ColumnStat
import com.iomete.catalogsync.extract.SupportColumnStatistics
import com.iomete.catalogsync.extract.SupportColumnTags
import com.iomete.catalogsync.extract.SupportTableStatistics
import com.iomete.catalogsync.extract.TableExtractor
import com.iomete.catalogsync.extract.TableExtractorFactory
import com.iomete.catalogsync.extract.TableStatistics
import com.iomete.catalogsync.presidio.PIIDetectionService
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TableMetadataExtractorTest {
    private lateinit var tableExtractorFactory: TableExtractorFactory
    private lateinit var piiDetectionService: PIIDetectionService
    private lateinit var applicationConfig: ApplicationConfig
    private lateinit var sparkMetadataReader: SparkMetadataReader
    private lateinit var icebergMetadataReader: IcebergMetadataReader
    private lateinit var tableMetadataExtractor: TableMetadataExtractor
    private lateinit var sparkSession: SparkSession

    @BeforeEach
    fun setup() {
        tableExtractorFactory = mockk()
        piiDetectionService = mockk()
        applicationConfig = mockk()
        sparkMetadataReader = mockk()
        icebergMetadataReader = mockk()
        sparkSession = mockk(relaxed = true)

        every { applicationConfig.exclusionRules } returns ExclusionRules(
            defaultRule = DefaultRule(filterByProperties = emptyMap())
        )

        val mockSparkContext = mockk<SparkContext>()
        every { sparkSession.sparkContext() } returns mockSparkContext
        every { mockSparkContext.applicationId() } returns "app-test-123"

        tableMetadataExtractor =
            TableMetadataExtractor(
                tableExtractorFactory,
                piiDetectionService,
                applicationConfig,
                sparkMetadataReader,
                icebergMetadataReader,
            )
    }

    private fun setupBasicTableExtractor(
        catalog: String = "test_catalog",
        schema: String = "test_schema",
        tableName: String = "test_table",
        metadata: Map<String, String> = mapOf("Type" to "MANAGED", "Provider" to "iceberg"),
        columns: List<ColumnMetadata> = listOf(
            ColumnMetadata("id", "int", "User ID", 0, false),
            ColumnMetadata("email", "string", "User email", 1, false),
        ),
        extractor: TableExtractor = mockk<TableExtractor>(),
    ): TableExtractor {
        val tableDescription = TableDescription(columns, metadata)

        every { sparkMetadataReader.describeTable(sparkSession, catalog, schema, tableName) } returns tableDescription
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession,
                provider = metadata.getOrDefault("Provider", "UNKNOWN"),
                isView = metadata.getOrDefault("Type", "UNKNOWN").equals("view", ignoreCase = true),
                catalog = catalog,
                schema = schema,
                table = tableName,
            )
        } returns extractor

        return extractor
    }

    private fun extractorWithExclusionRules(exclusionRules: ExclusionRules): TableMetadataExtractor {
        every { applicationConfig.exclusionRules } returns exclusionRules
        return TableMetadataExtractor(tableExtractorFactory, piiDetectionService, applicationConfig, sparkMetadataReader, icebergMetadataReader)
    }

    @Test
    fun `scrapeTable should extract table metadata correctly`() {
        setupBasicTableExtractor()

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertEquals("test_catalog", result.catalog)
        assertEquals("test_schema", result.schema)
        assertEquals("test_table", result.name)
        assertEquals("MANAGED", result.tableType)
        assertEquals(2, result.columns.size)
        assertEquals("email", result.columns[1].name)
    }

    @Test
    fun `scrapeTable with EXTERNAL table type`() {
        setupBasicTableExtractor(metadata = mapOf("Type" to "EXTERNAL", "Provider" to "parquet"))

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertEquals("EXTERNAL", result.tableType)
        assertFalse(result.isView)
    }

    @Test
    fun `scrapeTable with VIEW type sets isView true`() {
        val viewWithTags = object : TableExtractor, SupportColumnTags {
            override val getTableType = "VIEW"
        }
        setupBasicTableExtractor(
            metadata = mapOf("Type" to "view", "Provider" to "UNKNOWN"),
            extractor = viewWithTags,
        )
        every { piiDetectionService.extract(any(), any(), any(), any()) } returns emptyMap()

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertTrue(result.isView)
        assertEquals("view", result.tableType)
    }

    @Test
    fun `scrapeTable throws ExcludedItemException for excluded table`() {
        val extractor = extractorWithExclusionRules(ExclusionRules(
            tables = GeneralFilter(filterByProperties = mapOf("hidden" to "true")),
            defaultRule = DefaultRule(filterByProperties = emptyMap())
        ))

        val tableDescription = TableDescription(
            columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Table Properties" to "[hidden=true]")
        )
        every { sparkMetadataReader.describeTable(sparkSession, "cat", "sch", "tbl") } returns tableDescription

        assertThrows(ExcludedItemException::class.java) {
            extractor.scrapeTable(sparkSession, "cat", "sch", "tbl", false)
        }
    }

    @Test
    fun `scrapeTable populates statistics for SupportTableStatistics extractor`() {
        val statsExtractor = object : TableExtractor, SupportTableStatistics, SupportColumnTags {
            override val getTableType = "MANAGED"
            override fun extractTableStatistics() = TableStatistics(
                lastModified = 1000L,
                numFiles = 5L,
                totalTableNumFiles = 10L,
                sizeInBytes = 1024L,
                totalTableSizeInBytes = 2048L,
                totalRecords = 100L,
            )
        }
        setupBasicTableExtractor(extractor = statsExtractor)
        every { piiDetectionService.extract(any(), any(), any(), any()) } returns emptyMap()

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertEquals(1000L, result.lastModified)
        assertEquals(5L, result.numFiles)
        assertEquals(10L, result.totalTableNumFiles)
        assertEquals(1024L, result.sizeInBytes)
        assertEquals(2048L, result.totalTableSizeInBytes)
        assertEquals(100L, result.totalRecords)
    }

    @Test
    fun `scrapeTable with generic extractor has null statistics`() {
        val genericExtractor = mockk<TableExtractor>()
        setupBasicTableExtractor(
            metadata = mapOf("Type" to "UNKNOWN", "Provider" to "csv"),
            extractor = genericExtractor,
        )

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertNull(result.lastModified)
        assertNull(result.numFiles)
        assertNull(result.sizeInBytes)
        assertNull(result.totalRecords)
    }

    @Test
    fun `scrapeTable with column statistics extractor populates column stats`() {
        val colStatsExtractor = object : TableExtractor, SupportColumnStatistics, SupportColumnTags {
            override val getTableType = "MANAGED"
            override fun extractColumnStatistics(columns: List<String>): Map<String, List<ColumnStat>> {
                return mapOf("id" to listOf(ColumnStat("distinctCount", "100")))
            }
        }
        setupBasicTableExtractor(extractor = colStatsExtractor)
        every { piiDetectionService.extract(any(), any(), any(), any()) } returns emptyMap()

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertEquals(1, result.columns[0].stats.size)
        assertEquals("distinctCount", result.columns[0].stats[0].name)
        assertEquals("100", result.columns[0].stats[0].statValue)
    }

    @Test
    fun `scrapeTable assigns PII tags to columns`() {
        val tagExtractor = object : TableExtractor, SupportColumnTags {
            override val getTableType = "MANAGED"
        }
        setupBasicTableExtractor(extractor = tagExtractor)
        every { piiDetectionService.extract(any(), any(), any(), any()) } returns mapOf(
            "email" to listOf("DETECTED:EMAIL_ADDRESS", "DETECTED:PII")
        )

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        val emailCol = result.columns.find { it.name == "email" }!!
        assertTrue(emailCol.tags.contains("DETECTED:EMAIL_ADDRESS"))
        assertTrue(emailCol.tags.contains("DETECTED:PII"))
        // Table-level tags should aggregate PII/PCI tags
        assertTrue(result.tags.contains("DETECTED:PII"))
    }

    @Test
    fun `scrapeTable skips PII detection when extractor does not support tags`() {
        val noTagExtractor = mockk<TableExtractor>()
        setupBasicTableExtractor(extractor = noTagExtractor)

        tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        verify(exactly = 0) { piiDetectionService.extract(any(), any(), any(), any()) }
    }

    @Test
    fun `scrapeTable with isTemp true sets isTemporary`() {
        setupBasicTableExtractor()

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", true)

        assertTrue(result.isTemporary)
    }

    @Test
    fun `scrapeTable populates syncTime and sparkApplicationId`() {
        setupBasicTableExtractor()

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertTrue(result.syncTime > 0)
        assertEquals("app-test-123", result.sparkApplicationId)
    }


    @Test
    fun `scrapeTable with valid Table Properties triggers exclusion when rule matches`() {
        val extractor = extractorWithExclusionRules(ExclusionRules(
            tables = GeneralFilter(filterByProperties = mapOf("hidden" to "true")),
            defaultRule = DefaultRule(filterByProperties = emptyMap())
        ))

        val tableDescription = TableDescription(
            columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Table Properties" to "[hidden=true,key2=value2]")
        )
        every { sparkMetadataReader.describeTable(sparkSession, "cat", "sch", "tbl") } returns tableDescription

        assertThrows(ExcludedItemException::class.java) {
            extractor.scrapeTable(sparkSession, "cat", "sch", "tbl", false)
        }
    }

    @Test
    fun `scrapeTable with null Table Properties does not trigger exclusion`() {
        val extractor = extractorWithExclusionRules(ExclusionRules(
            tables = GeneralFilter(filterByProperties = mapOf("hidden" to "true")),
            defaultRule = DefaultRule(filterByProperties = emptyMap())
        ))

        val tableDescription = TableDescription(
            columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg")
        )
        every { sparkMetadataReader.describeTable(sparkSession, "cat", "sch", "tbl") } returns tableDescription
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession, provider = "iceberg", isView = false,
                catalog = "cat", schema = "sch", table = "tbl"
            )
        } returns mockk<TableExtractor>()

        assertDoesNotThrow {
            extractor.scrapeTable(sparkSession, "cat", "sch", "tbl", false)
        }
    }

    @Test
    fun `scrapeTable with empty Table Properties does not trigger exclusion`() {
        val extractor = extractorWithExclusionRules(ExclusionRules(
            tables = GeneralFilter(filterByProperties = mapOf("hidden" to "true")),
            defaultRule = DefaultRule(filterByProperties = emptyMap())
        ))

        val tableDescription = TableDescription(
            columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Table Properties" to "")
        )
        every { sparkMetadataReader.describeTable(sparkSession, "cat", "sch", "tbl") } returns tableDescription
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession, provider = "iceberg", isView = false,
                catalog = "cat", schema = "sch", table = "tbl"
            )
        } returns mockk<TableExtractor>()

        assertDoesNotThrow {
            extractor.scrapeTable(sparkSession, "cat", "sch", "tbl", false)
        }
    }

    @Test
    fun `scrapeTable with malformed Table Properties does not trigger exclusion`() {
        val extractor = extractorWithExclusionRules(ExclusionRules(
            tables = GeneralFilter(filterByProperties = mapOf("hidden" to "true")),
            defaultRule = DefaultRule(filterByProperties = emptyMap())
        ))

        val tableDescription = TableDescription(
            columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Table Properties" to "not_valid_format")
        )
        every { sparkMetadataReader.describeTable(sparkSession, "cat", "sch", "tbl") } returns tableDescription
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession, provider = "iceberg", isView = false,
                catalog = "cat", schema = "sch", table = "tbl"
            )
        } returns mockk<TableExtractor>()

        assertDoesNotThrow {
            extractor.scrapeTable(sparkSession, "cat", "sch", "tbl", false)
        }
    }

    @Test
    fun `scrapeTable with Table Properties containing values with equals sign`() {
        val extractor = extractorWithExclusionRules(ExclusionRules(
            tables = GeneralFilter(filterByProperties = mapOf("key" to "val=ue")),
            defaultRule = DefaultRule(filterByProperties = emptyMap())
        ))

        val tableDescription = TableDescription(
            columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Table Properties" to "[key=val=ue]")
        )
        every { sparkMetadataReader.describeTable(sparkSession, "cat", "sch", "tbl") } returns tableDescription

        assertThrows(ExcludedItemException::class.java) {
            extractor.scrapeTable(sparkSession, "cat", "sch", "tbl", false)
        }
    }


    @Test
    fun `scrapeTable parses valid Created Time to epoch seconds`() {
        setupBasicTableExtractor(
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Created Time" to "Thu Jan 02 10:30:00 UTC 2025")
        )

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertNotNull(result.createdAt)
        assertTrue(result.createdAt!! > 0)
    }

    @Test
    fun `scrapeTable with missing Created Time returns null createdAt`() {
        setupBasicTableExtractor(
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg")
        )

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertNull(result.createdAt)
    }

    @Test
    fun `scrapeTable with unparseable Created Time returns null createdAt`() {
        setupBasicTableExtractor(
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Created Time" to "not-a-valid-date")
        )

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertNull(result.createdAt)
    }

    @Test
    fun `scrapeTable uses iceberg fast path without Spark describe or snapshot statistics query`() {
        val fastPathStats = TableStatistics(
            lastModified = 1000L,
            numFiles = 5L,
            totalTableNumFiles = 10L,
            sizeInBytes = 1024L,
            totalTableSizeInBytes = 2048L,
            totalRecords = 100L,
        )
        val fastPathMetadata = IcebergLoadedTableMetadata(
            tableDescription = TableDescription(
                columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
                metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Comment" to "from iceberg"),
            ),
            tableProperties = mapOf("hidden" to "false"),
            statistics = fastPathStats,
        )
        val statsExtractor = object : TableExtractor, SupportTableStatistics {
            override val getTableType = "MANAGED"
            override fun extractTableStatistics(): TableStatistics = error("should not query Iceberg snapshots on fast path")
        }

        every {
            icebergMetadataReader.loadTableMetadata(sparkSession, "cat", "sch", "tbl")
        } returns fastPathMetadata
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession,
                provider = "iceberg",
                isView = false,
                catalog = "cat",
                schema = "sch",
                table = "tbl",
                tableProperties = mapOf("hidden" to "false"),
            )
        } returns statsExtractor

        val result = tableMetadataExtractor.scrapeTable(
            spark = sparkSession,
            catalog = "cat",
            schema = "sch",
            tableName = "tbl",
            isTemp = false,
            useIcebergFastPath = true,
        )

        assertEquals("from iceberg", result.description)
        assertEquals(fastPathStats.lastModified, result.lastModified)
        assertEquals(fastPathStats.numFiles, result.numFiles)
        assertEquals(fastPathStats.totalTableNumFiles, result.totalTableNumFiles)
        verify(exactly = 0) { sparkMetadataReader.describeTable(any(), any(), any(), any()) }
        verify(exactly = 0) { sparkSession.sql(match { it.contains("snapshots", ignoreCase = true) }) }
    }

    @Test
    fun `scrapeTable fast path promotes iceberg provider to managed when type metadata is absent`() {
        val fastPathMetadata = IcebergLoadedTableMetadata(
            tableDescription = TableDescription(
                columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
                metadata = mapOf("Provider" to "iceberg"),
            ),
            tableProperties = emptyMap(),
            statistics = null,
        )
        val extractor = mockk<TableExtractor>()

        every {
            icebergMetadataReader.loadTableMetadata(sparkSession, "cat", "sch", "tbl")
        } returns fastPathMetadata
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession,
                provider = "iceberg",
                isView = false,
                catalog = "cat",
                schema = "sch",
                table = "tbl",
                tableProperties = emptyMap(),
            )
        } returns extractor

        val result = tableMetadataExtractor.scrapeTable(
            spark = sparkSession,
            catalog = "cat",
            schema = "sch",
            tableName = "tbl",
            isTemp = false,
            useIcebergFastPath = true,
        )

        assertEquals("MANAGED", result.tableType)
        assertFalse(result.isView)
        verify(exactly = 0) { sparkMetadataReader.describeTable(any(), any(), any(), any()) }
    }

    @Test
    fun `scrapeTable fast path preserves complete output shape in regular test gate`() {
        val columns = listOf(
            ColumnMetadata("id", "bigint", "user id", 0, false),
            ColumnMetadata("email", "string", "email address", 1, false),
            ColumnMetadata("event_date", "string", "event partition date", 2, true),
        )
        val fastPathStats = TableStatistics(
            lastModified = 1234L,
            numFiles = 2L,
            totalTableNumFiles = 3L,
            sizeInBytes = 512L,
            totalTableSizeInBytes = 768L,
            totalRecords = 5L,
        )
        val fastPathMetadata = IcebergLoadedTableMetadata(
            tableDescription = TableDescription(
                columns = columns,
                metadata = mapOf(
                    "Type" to "MANAGED",
                    "Provider" to "iceberg",
                    "Comment" to "users table",
                    "Owner" to "analytics",
                    "Partition Spec" to "event_date",
                ),
            ),
            tableProperties = mapOf("comment" to "users table", "owner" to "analytics", "hidden" to "false"),
            statistics = fastPathStats,
        )
        val tagsExtractor = object : TableExtractor, SupportColumnTags {
            override val getTableType = "MANAGED"
        }

        every {
            icebergMetadataReader.loadTableMetadata(sparkSession, "cat", "sch", "tbl")
        } returns fastPathMetadata
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession,
                provider = "iceberg",
                isView = false,
                catalog = "cat",
                schema = "sch",
                table = "tbl",
                tableProperties = mapOf("comment" to "users table", "owner" to "analytics", "hidden" to "false"),
            )
        } returns tagsExtractor
        every {
            piiDetectionService.extract(
                spark = sparkSession,
                catalog = "cat",
                fullTableName = "`cat`.`sch`.`tbl`",
                columns = listOf("id", "email", "event_date"),
            )
        } returns mapOf("email" to listOf("DETECTED:EMAIL_ADDRESS", "DETECTED:PII"))

        val result = tableMetadataExtractor.scrapeTable(
            spark = sparkSession,
            catalog = "cat",
            schema = "sch",
            tableName = "tbl",
            isTemp = false,
            useIcebergFastPath = true,
        )

        assertEquals("cat", result.catalog)
        assertEquals("sch", result.schema)
        assertEquals("tbl", result.name)
        assertEquals("MANAGED", result.tableType)
        assertFalse(result.isView)
        assertEquals("iceberg", result.provider)
        assertEquals("users table", result.description)
        assertEquals("analytics", result.owner)
        assertEquals(fastPathStats.lastModified, result.lastModified)
        assertEquals(fastPathStats.numFiles, result.numFiles)
        assertEquals(fastPathStats.totalTableNumFiles, result.totalTableNumFiles)
        assertEquals(fastPathStats.sizeInBytes, result.sizeInBytes)
        assertEquals(fastPathStats.totalTableSizeInBytes, result.totalTableSizeInBytes)
        assertEquals(fastPathStats.totalRecords, result.totalRecords)
        assertEquals(listOf("id", "email", "event_date"), result.columns.map { it.name })
        assertEquals(listOf("bigint", "string", "string"), result.columns.map { it.dataType })
        assertEquals(listOf("user id", "email address", "event partition date"), result.columns.map { it.description })
        assertEquals(listOf(0, 1, 2), result.columns.map { it.sortOrder })
        assertEquals(listOf(false, false, true), result.columns.map { it.isPartitionKey })
        assertEquals(listOf("DETECTED:EMAIL_ADDRESS", "DETECTED:PII"), result.columns.single { it.name == "email" }.tags)
        assertEquals(listOf("DETECTED:PII"), result.tags)
        verify(exactly = 0) { sparkMetadataReader.describeTable(any(), any(), any(), any()) }
    }

    @Test
    fun `scrapeTable fast path preserves empty table statistics in regular test gate`() {
        val fastPathMetadata = IcebergLoadedTableMetadata(
            tableDescription = TableDescription(
                columns = listOf(
                    ColumnMetadata("id", "bigint", null, 0, false),
                    ColumnMetadata("value", "string", null, 1, false),
                ),
                metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg"),
            ),
            tableProperties = emptyMap(),
            statistics = null,
        )
        val extractor = mockk<TableExtractor>()

        every {
            icebergMetadataReader.loadTableMetadata(sparkSession, "cat", "sch", "empty_table")
        } returns fastPathMetadata
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession,
                provider = "iceberg",
                isView = false,
                catalog = "cat",
                schema = "sch",
                table = "empty_table",
                tableProperties = emptyMap(),
            )
        } returns extractor

        val result = tableMetadataExtractor.scrapeTable(
            spark = sparkSession,
            catalog = "cat",
            schema = "sch",
            tableName = "empty_table",
            isTemp = false,
            useIcebergFastPath = true,
        )

        assertEquals(listOf("id", "value"), result.columns.map { it.name })
        assertNull(result.lastModified)
        assertNull(result.numFiles)
        assertNull(result.totalTableNumFiles)
        assertNull(result.sizeInBytes)
        assertNull(result.totalTableSizeInBytes)
        assertNull(result.totalRecords)
        verify(exactly = 0) { sparkMetadataReader.describeTable(any(), any(), any(), any()) }
    }

    @Test
    fun `scrapeTable falls back to Spark describe when iceberg fast path fails`() {
        val fallbackStats = TableStatistics(lastModified = 2000L, numFiles = 2L)
        val fallbackExtractor = object : TableExtractor, SupportTableStatistics {
            override val getTableType = "MANAGED"
            override fun extractTableStatistics() = fallbackStats
        }
        val fallbackDescription = TableDescription(
            columns = listOf(ColumnMetadata("id", "int", null, 0, false)),
            metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg", "Comment" to "from spark"),
        )

        every {
            icebergMetadataReader.loadTableMetadata(sparkSession, "cat", "sch", "tbl")
        } throws RuntimeException("boom")
        every { sparkMetadataReader.describeTable(sparkSession, "cat", "sch", "tbl") } returns fallbackDescription
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession,
                provider = "iceberg",
                isView = false,
                catalog = "cat",
                schema = "sch",
                table = "tbl",
                tableProperties = emptyMap(),
            )
        } returns fallbackExtractor

        val result = tableMetadataExtractor.scrapeTable(
            spark = sparkSession,
            catalog = "cat",
            schema = "sch",
            tableName = "tbl",
            isTemp = false,
            useIcebergFastPath = true,
        )

        assertEquals("from spark", result.description)
        assertEquals(fallbackStats.lastModified, result.lastModified)
        assertEquals(fallbackStats.numFiles, result.numFiles)
        verify(exactly = 1) { sparkMetadataReader.describeTable(sparkSession, "cat", "sch", "tbl") }
    }

    @Test
    fun `scrapeTable keeps Spark describe path when iceberg fast path is disabled`() {
        setupBasicTableExtractor(metadata = mapOf("Type" to "MANAGED", "Provider" to "iceberg"))

        tableMetadataExtractor.scrapeTable(
            spark = sparkSession,
            catalog = "test_catalog",
            schema = "test_schema",
            tableName = "test_table",
            isTemp = false,
            useIcebergFastPath = false,
        )

        verify(exactly = 0) { icebergMetadataReader.loadTableMetadata(any(), any(), any(), any()) }
        verify(exactly = 1) { sparkMetadataReader.describeTable(sparkSession, "test_catalog", "test_schema", "test_table") }
    }

    // Additional: UNKNOWN type with iceberg provider gets promoted to MANAGED
    @Test
    fun `scrapeTable promotes UNKNOWN to MANAGED when provider is iceberg`() {
        setupBasicTableExtractor(
            metadata = mapOf("Type" to "UNKNOWN", "Provider" to "iceberg")
        )

        val result = tableMetadataExtractor.scrapeTable(sparkSession, "test_catalog", "test_schema", "test_table", false)

        assertEquals("MANAGED", result.tableType)
    }
}
