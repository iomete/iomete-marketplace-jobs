package com.iomete.catalogsync.extract.utils

import com.iomete.catalogsync.*
import io.mockk.*
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*

class ColumnTagExtractorTest {

    private lateinit var mockSpark: SparkSession
    private lateinit var mockPresidioClient: PresidioClient
    private lateinit var extractor: ColumnTagExtractor

    @BeforeEach
    fun setup() {
        mockSpark = mockk()
        mockPresidioClient = mockk()
        extractor = ColumnTagExtractor(spark = mockSpark, presidioClient = mockPresidioClient)

        // Enable PII detection via system property
        System.setProperty("piiDetectionEnabled", "true")
    }

    @AfterEach
    fun tearDown() {
        System.clearProperty("piiDetectionEnabled")
    }

    @Test
    fun `extract should return empty map when PII detection is disabled`() {
        System.setProperty("piiDetectionEnabled", "false")

        // Use mockkStatic to mock ConfigProvider
        mockkStatic(org.eclipse.microprofile.config.ConfigProvider::class)
        val mockConfig = mockk<org.eclipse.microprofile.config.Config>()
        every { org.eclipse.microprofile.config.ConfigProvider.getConfig() } returns mockConfig
        every { mockConfig.getOptionalValue("PII_DETECTION_ENABLED", String::class.java) } returns java.util.Optional.of("false")

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1", "col2"))

        assertEquals(emptyMap<String, List<String>>(), result)

        unmockkStatic(org.eclipse.microprofile.config.ConfigProvider::class)
    }

    @Test
    fun `extract should detect PII tags for columns with personal data`() {
        setupConfigMock()

        val sampleRow = mockSampleRow("col1", "John Doe")
        setupSqlMock(listOf(sampleRow))

        every { mockPresidioClient.analyze(match { it.text == "John Doe" }) } returns listOf(
            PresidioResponse(entityType = EntityType.PERSON, score = 0.85f)
        )

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        assertEquals(listOf("DETECTED_PERSON", "DETECTED_PII"), result["col1"])
    }

    @Test
    fun `extract should detect PCI tags for columns with financial data`() {
        setupConfigMock()

        val sampleRow = mockSampleRow("col1", "4111111111111111")
        setupSqlMock(listOf(sampleRow))

        every { mockPresidioClient.analyze(match { it.text == "4111111111111111" }) } returns listOf(
            PresidioResponse(entityType = EntityType.CREDIT_CARD, score = 0.95f)
        )

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        assertEquals(listOf("DETECTED_CREDIT_CARD", "DETECTED_PCI"), result["col1"])
    }

    @Test
    fun `extract should handle empty sample data gracefully`() {
        setupConfigMock()
        setupSqlMock(emptyList())

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        // With empty sample data, columnSampleData will be null, so detectedTags returns empty
        assertEquals(emptyList<String>(), result["col1"])
    }

    @Test
    fun `extract should handle exception from SQL query`() {
        setupConfigMock()

        every { mockSpark.sql(any()) } throws RuntimeException("SQL error")

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        // On exception, returns the partial result (empty map in this case)
        assertTrue(result.isEmpty())
    }

    @Test
    fun `extract should use highest scoring entity type`() {
        setupConfigMock()

        val sampleRow = mockSampleRow("col1", "John at john@example.com")
        setupSqlMock(listOf(sampleRow))

        every { mockPresidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.PERSON, score = 0.6f),
            PresidioResponse(entityType = EntityType.EMAIL_ADDRESS, score = 0.95f)
        )

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        // Sorted by score descending, EMAIL_ADDRESS has highest score
        assertEquals(listOf("DETECTED_EMAIL_ADDRESS", "DETECTED_PII"), result["col1"])
    }

    @Test
    fun `extract should return empty tags for null or blank sample values`() {
        setupConfigMock()

        val sampleRow = mockk<Row>()
        val schema = mockk<StructType>()
        every { sampleRow.schema() } returns schema
        every { schema.fieldIndex("col1") } returns 0
        every { sampleRow.get(0) } returns "   "
        setupSqlMock(listOf(sampleRow))

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        assertEquals(emptyList<String>(), result["col1"])
    }

    @Test
    fun `extract should prefix tags with DETECTED_`() {
        setupConfigMock()

        val sampleRow = mockSampleRow("col1", "John Doe")
        setupSqlMock(listOf(sampleRow))

        every { mockPresidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.PERSON, score = 0.9f)
        )

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        assertTrue(result["col1"]!!.all { it.startsWith("DETECTED_") })
    }

    @Test
    fun `extract should add both entity type and PII tag for PII entities`() {
        setupConfigMock()

        val sampleRow = mockSampleRow("col1", "John Doe")
        setupSqlMock(listOf(sampleRow))

        every { mockPresidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.PERSON, score = 0.9f)
        )

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        assertTrue(result["col1"]!!.contains("DETECTED_PERSON"))
        assertTrue(result["col1"]!!.contains("DETECTED_PII"))
    }

    @Test
    fun `extract should add both entity type and PCI tag for PCI entities`() {
        setupConfigMock()

        val sampleRow = mockSampleRow("col1", "DE89370400440532013000")
        setupSqlMock(listOf(sampleRow))

        every { mockPresidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.IBAN_CODE, score = 0.9f)
        )

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        assertTrue(result["col1"]!!.contains("DETECTED_IBAN_CODE"))
        assertTrue(result["col1"]!!.contains("DETECTED_PCI"))
    }

    @Test
    fun `extract should not add PII or PCI tag for non-sensitive entities`() {
        setupConfigMock()

        val sampleRow = mockSampleRow("col1", "2023-01-15")
        setupSqlMock(listOf(sampleRow))

        every { mockPresidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.DATE_TIME, score = 0.9f)
        )

        val result = extractor.extract("`c`.`s`.`t`", listOf("col1"))

        assertEquals(listOf("DETECTED_DATE_TIME"), result["col1"])
    }

    private fun setupConfigMock() {
        mockkStatic(org.eclipse.microprofile.config.ConfigProvider::class)
        val mockConfig = mockk<org.eclipse.microprofile.config.Config>()
        every { org.eclipse.microprofile.config.ConfigProvider.getConfig() } returns mockConfig
        every { mockConfig.getOptionalValue("PII_DETECTION_ENABLED", String::class.java) } returns java.util.Optional.of("false")
        // System property piiDetectionEnabled=true is set in @BeforeEach
    }

    private fun setupSqlMock(rows: List<Row>) {
        val dataset = mockk<Dataset<Row>>()
        every { mockSpark.sql(match { it.contains("TABLESAMPLE") }) } returns dataset
        every { dataset.collectAsList() } returns rows
    }

    private fun mockSampleRow(columnName: String, value: String): Row {
        val row = mockk<Row>()
        val schema = mockk<StructType>()
        every { row.schema() } returns schema
        every { schema.fieldIndex(columnName) } returns 0
        every { row.get(0) } returns value
        return row
    }
}
