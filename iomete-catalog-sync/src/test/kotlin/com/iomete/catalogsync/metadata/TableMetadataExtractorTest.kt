package com.iomete.catalogsync.metadata

import com.iomete.catalogsync.config.ApplicationConfig
import com.iomete.catalogsync.config.ExclusionRules
import com.iomete.catalogsync.extract.TableExtractor
import com.iomete.catalogsync.extract.TableExtractorFactory
import com.iomete.catalogsync.presidio.PIIDetectionService
import io.mockk.every
import io.mockk.mockk
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class TableMetadataExtractorTest {
    private lateinit var tableExtractorFactory: TableExtractorFactory
    private lateinit var piiDetectionService: PIIDetectionService
    private lateinit var applicationConfig: ApplicationConfig
    private lateinit var sparkMetadataReader: SparkMetadataReader
    private lateinit var tableMetadataExtractor: TableMetadataExtractor
    private lateinit var sparkSession: SparkSession

    @BeforeEach
    fun setup() {
        tableExtractorFactory = mockk()
        piiDetectionService = mockk()
        applicationConfig = mockk()
        sparkMetadataReader = mockk()
        sparkSession = mockk()

        every { applicationConfig.exclusionRules } returns ExclusionRules()

        tableMetadataExtractor =
            TableMetadataExtractor(
                tableExtractorFactory,
                piiDetectionService,
                applicationConfig,
                sparkMetadataReader,
            )
    }

    @Test
    fun `scrapeTable should extract table metadata correctly`() {
        // Arrange
        val catalog = "test_catalog"
        val schema = "test_schema"
        val tableName = "test_table"
        val columns =
            listOf(
                ColumnMetadata("id", "int", "User ID", 0, false),
                ColumnMetadata("email", "string", "User email", 1, false),
            )
        val tableDescription = TableDescription(columns, mapOf("Type" to "MANAGED", "Provider" to "iceberg"))
        val mockTableExtractor = mockk<TableExtractor>()

        every { sparkMetadataReader.describeTable(sparkSession, catalog, schema, tableName) } returns tableDescription
        every {
            tableExtractorFactory.extractorFor(
                spark = sparkSession,
                provider = "iceberg",
                isView = false,
                catalog = catalog,
                schema = schema,
                table = tableName,
            )
        } returns mockTableExtractor

        // Act
        val result = tableMetadataExtractor.scrapeTable(sparkSession, catalog, schema, tableName, false)

        // Assert
        assertEquals(catalog, result.catalog)
        assertEquals(schema, result.schema)
        assertEquals(tableName, result.name)
        assertEquals("MANAGED", result.tableType)
        assertEquals(2, result.columns.size)
        assertEquals("email", result.columns[1].name)
    }
}
