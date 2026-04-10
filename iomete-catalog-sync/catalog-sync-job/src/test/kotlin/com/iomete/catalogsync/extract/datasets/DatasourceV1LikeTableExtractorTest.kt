package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.utils.ColumnTagExtractor
import io.mockk.*
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.internal.SessionState
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import scala.Option
import scala.collection.immutable.Map as ScalaMap

class DatasourceV1LikeTableExtractorTest {

    private lateinit var mockSpark: SparkSession
    private lateinit var mockColumnTagExtractor: ColumnTagExtractor
    private lateinit var mockSessionState: SessionState
    private lateinit var mockSessionCatalog: SessionCatalog
    private lateinit var mockCatalog: Catalog
    private lateinit var mockCatalogTable: CatalogTable
    private lateinit var mockTable: Table

    @BeforeEach
    fun setup() {
        mockSpark = mockk()
        mockColumnTagExtractor = mockk()
        mockSessionState = mockk()
        mockSessionCatalog = mockk()
        mockCatalog = mockk()
        mockCatalogTable = mockk(relaxed = true)
        mockTable = mockk(relaxed = true)

        every { mockSpark.sessionState() } returns mockSessionState
        every { mockSessionState.catalog() } returns mockSessionCatalog
        every { mockSessionCatalog.getTempViewOrPermanentTableMetadata(any<TableIdentifier>()) } returns mockCatalogTable
        every { mockSpark.catalog() } returns mockCatalog
        every { mockCatalog.getTable(any<String>(), any<String>()) } returns mockTable
    }

    private fun createExtractor(schema: String = "test_schema", tableName: String = "test_table"): DatasourceV1LikeTableExtractor {
        return DatasourceV1LikeTableExtractor(
            spark = mockSpark,
            columnTagExtractor = mockColumnTagExtractor,
            schema = schema,
            tableName = tableName
        )
    }

    @Test
    fun `extractTableStatistics should return stats when available`() {
        val mockStats = mockk<CatalogStatistics>()
        every { mockCatalogTable.stats() } returns Option.apply(mockStats)
        every { mockStats.sizeInBytes() } returns BigInt.apply(4096)
        every { mockStats.rowCount() } returns Option.apply(BigInt.apply(50))
        every { mockCatalogTable.createTime() } returns 1700000000000L

        val extractor = createExtractor()
        val result = extractor.extractTableStatistics()

        assertNotNull(result)
        assertEquals(4096L, result!!.sizeInBytes)
        assertEquals(50L, result.totalRecords)
        assertEquals(1700000000000L, result.lastModified)
        assertNull(result.numFiles)
    }

    @Test
    fun `extractTableStatistics should return null when no stats available`() {
        every { mockCatalogTable.stats() } returns Option.empty()

        val extractor = createExtractor()
        val result = extractor.extractTableStatistics()

        assertNull(result)
    }

    @Test
    fun `extractColumnStatistics should return stats for each column`() {
        val mockStats = mockk<CatalogStatistics>()
        val mockColumnStat = mockk<CatalogColumnStat>()
        val mockColStatsMap = mockk<ScalaMap<String, CatalogColumnStat>>()

        every { mockCatalogTable.stats() } returns Option.apply(mockStats)
        every { mockStats.colStats() } returns mockColStatsMap
        every { mockColStatsMap.get("col1") } returns Option.apply(mockColumnStat)
        every { mockColumnStat.distinctCount() } returns Option.apply(BigInt.apply(10))
        every { mockColumnStat.min() } returns Option.apply("0")
        every { mockColumnStat.max() } returns Option.apply("100")
        every { mockColumnStat.nullCount() } returns Option.apply(BigInt.apply(2))
        every { mockColumnStat.avgLen() } returns Option.apply(8L)
        every { mockColumnStat.maxLen() } returns Option.apply(16L)

        val extractor = createExtractor()
        val result = extractor.extractColumnStatistics(listOf("col1"))

        assertNotNull(result["col1"])
        val stats = result["col1"]!!
        assertEquals(6, stats.size)
        assertTrue(stats.any { it.name == "distinctCount" && it.statValue == "10" })
        assertTrue(stats.any { it.name == "min" && it.statValue == "0" })
        assertTrue(stats.any { it.name == "max" && it.statValue == "100" })
        assertTrue(stats.any { it.name == "nullCount" && it.statValue == "2" })
        assertTrue(stats.any { it.name == "avgLen" && it.statValue == "8" })
        assertTrue(stats.any { it.name == "maxLen" && it.statValue == "16" })
    }

    @Test
    fun `extractColumnStatistics should return empty list for column without stats`() {
        val mockStats = mockk<CatalogStatistics>()
        val mockColStatsMap = mockk<ScalaMap<String, CatalogColumnStat>>()

        every { mockCatalogTable.stats() } returns Option.apply(mockStats)
        every { mockStats.colStats() } returns mockColStatsMap
        every { mockColStatsMap.get("col_missing") } returns Option.empty()

        val extractor = createExtractor()
        val result = extractor.extractColumnStatistics(listOf("col_missing"))

        assertEquals(emptyList<Any>(), result["col_missing"])
    }

    @Test
    fun `extractColumnStatistics should handle partial stats`() {
        val mockStats = mockk<CatalogStatistics>()
        val mockColumnStat = mockk<CatalogColumnStat>()
        val mockColStatsMap = mockk<ScalaMap<String, CatalogColumnStat>>()

        every { mockCatalogTable.stats() } returns Option.apply(mockStats)
        every { mockStats.colStats() } returns mockColStatsMap
        every { mockColStatsMap.get("col1") } returns Option.apply(mockColumnStat)
        every { mockColumnStat.distinctCount() } returns Option.apply(BigInt.apply(5))
        every { mockColumnStat.min() } returns Option.empty()
        every { mockColumnStat.max() } returns Option.empty()
        every { mockColumnStat.nullCount() } returns Option.empty()
        every { mockColumnStat.avgLen() } returns Option.empty()
        every { mockColumnStat.maxLen() } returns Option.empty()

        val extractor = createExtractor()
        val result = extractor.extractColumnStatistics(listOf("col1"))

        val stats = result["col1"]!!
        assertEquals(1, stats.size)
        assertEquals("distinctCount", stats[0].name)
        assertEquals("5", stats[0].statValue)
    }

    @Test
    fun `extractColumnTags should delegate to ColumnTagExtractor`() {
        every { mockCatalogTable.stats() } returns Option.empty()

        val columns = listOf("col1", "col2")
        val expectedTags = mapOf("col1" to listOf("DETECTED_PERSON"), "col2" to emptyList())
        every { mockColumnTagExtractor.extract(any(), columns) } returns expectedTags

        val extractor = createExtractor()
        val result = extractor.extractColumnTags(columns)

        assertEquals(expectedTags, result)
        verify { mockColumnTagExtractor.extract("`test_schema`.`test_table`", columns) }
    }

    @Test
    fun `getTableType should return table type from spark catalog`() {
        every { mockTable.tableType() } returns "MANAGED"

        val extractor = createExtractor()

        assertEquals("MANAGED", extractor.getTableType)
    }
}

// Helper to create Scala BigInt from Kotlin
private object BigInt {
    fun apply(value: Long): scala.math.BigInt {
        return scala.math.BigInt.javaBigInteger2bigInt(java.math.BigInteger.valueOf(value))
    }
}
