package com.iomete.catalogsync.presidio

import com.iomete.catalogsync.mockRow
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

class PIIDetectionServiceTest {

    private lateinit var presidioClient: PresidioClient
    private lateinit var service: PIIDetectionService
    private lateinit var mockSparkSession: SparkSession

    @BeforeEach
    fun setup() {
        presidioClient = mockk()
        service = PIIDetectionService(presidioClient)
        mockSparkSession = mockk(relaxed = true)
    }

    @AfterEach
    fun tearDown() {
        System.clearProperty("piiDetectionEnabled")
    }

    private fun enablePiiDetection() {
        System.setProperty("piiDetectionEnabled", "true")
    }


    @Test
    fun `default disabled - returns empty map without calling Presidio`() {
        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("name"))
        assertEquals(emptyMap<String, List<String>>(), result)
        verify(exactly = 0) { presidioClient.analyze(any()) }
    }

    @Test
    fun `enabled via system property - Presidio is called`() {
        enablePiiDetection()

        val sampleRow = mockRow(mapOf("name" to "John Doe"))
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns listOf(sampleRow)

        every { presidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.PERSON, score = 0.9f)
        )

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("name"))

        assertTrue(result.containsKey("name"))
        verify(atLeast = 1) { presidioClient.analyze(any()) }
    }


    @Test
    fun `samples data and uses first non-empty distinct value`() {
        enablePiiDetection()

        val row1 = mockRow(mapOf("email" to "test@example.com"))
        val row2 = mockRow(mapOf("email" to "test@example.com"))
        val row3 = mockRow(mapOf("email" to "other@example.com"))
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns listOf(row1, row2, row3)

        every { presidioClient.analyze(match { it.text == "test@example.com" }) } returns listOf(
            PresidioResponse(entityType = EntityType.EMAIL_ADDRESS, score = 0.95f)
        )

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("email"))

        assertTrue(result["email"]!!.contains("DETECTED:EMAIL_ADDRESS"))
    }

    @Test
    fun `empty table returns empty map`() {
        enablePiiDetection()

        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns emptyList()

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("col1"))

        assertTrue(result.containsKey("col1"))
        assertEquals(emptyList<String>(), result["col1"])
    }

    @Test
    fun `Spark query failure returns empty map`() {
        enablePiiDetection()

        every { mockSparkSession.sql(any<String>()) } throws RuntimeException("Spark error")

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("col1"))

        assertEquals(emptyMap<String, List<String>>(), result)
    }


    @Test
    fun `PERSON detected - tags include DETECTED PERSON and DETECTED PII`() {
        enablePiiDetection()

        val row = mockRow(mapOf("name" to "Jane Smith"))
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns listOf(row)

        every { presidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.PERSON, score = 0.9f)
        )

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("name"))

        val tags = result["name"]!!
        assertTrue(tags.contains("DETECTED:PERSON"))
        assertTrue(tags.contains("DETECTED:PII"))
    }

    @Test
    fun `EMAIL_ADDRESS detected - tags include DETECTED EMAIL_ADDRESS and DETECTED PII`() {
        enablePiiDetection()

        val row = mockRow(mapOf("email" to "user@test.com"))
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns listOf(row)

        every { presidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.EMAIL_ADDRESS, score = 0.95f)
        )

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("email"))

        val tags = result["email"]!!
        assertTrue(tags.contains("DETECTED:EMAIL_ADDRESS"))
        assertTrue(tags.contains("DETECTED:PII"))
    }

    @Test
    fun `CREDIT_CARD detected - tags include DETECTED CREDIT_CARD and DETECTED PCI`() {
        enablePiiDetection()

        val row = mockRow(mapOf("card" to "4111111111111111"))
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns listOf(row)

        every { presidioClient.analyze(any()) } returns listOf(
            PresidioResponse(entityType = EntityType.CREDIT_CARD, score = 0.99f)
        )

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("card"))

        val tags = result["card"]!!
        assertTrue(tags.contains("DETECTED:CREDIT_CARD"))
        assertTrue(tags.contains("DETECTED:PCI"))
    }

    @Test
    fun `no entity detected returns empty tags for column`() {
        enablePiiDetection()

        val row = mockRow(mapOf("data" to "random text"))
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns listOf(row)

        every { presidioClient.analyze(any()) } returns emptyList()

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("data"))

        assertEquals(emptyList<String>(), result["data"])
    }

    @Test
    fun `Presidio API failure returns empty tags`() {
        enablePiiDetection()

        val row = mockRow(mapOf("name" to "John"))
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns listOf(row)

        every { presidioClient.analyze(any()) } throws RuntimeException("Presidio unreachable")

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("name"))

        assertEquals(emptyMap<String, List<String>>(), result)
    }

    @Test
    fun `table with fewer than 5 rows still works`() {
        enablePiiDetection()

        val row1 = mockRow(mapOf("col" to "value1"))
        val row2 = mockRow(mapOf("col" to "value2"))
        val dataset = mockk<Dataset<Row>>()
        every { mockSparkSession.sql(any<String>()) } returns dataset
        every { dataset.collectAsList() } returns listOf(row1, row2)

        every { presidioClient.analyze(any()) } returns emptyList()

        val result = service.extract(mockSparkSession, "cat", "`cat`.`sch`.`tbl`", listOf("col"))

        assertTrue(result.containsKey("col"))
    }
}
