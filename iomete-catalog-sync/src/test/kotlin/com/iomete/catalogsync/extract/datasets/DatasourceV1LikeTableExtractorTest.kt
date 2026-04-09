package com.iomete.catalogsync.extract.datasets

import io.mockk.every
import io.mockk.mockk
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.catalyst.catalog.CatalogStatistics
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.internal.SessionState
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import scala.Option
import scala.collection.immutable.`Map$`

class DatasourceV1LikeTableExtractorTest {
    private lateinit var mockSparkSession: SparkSession
    private lateinit var mockSessionState: SessionState
    private lateinit var mockSessionCatalog: SessionCatalog
    private lateinit var mockCatalog: Catalog
    private lateinit var mockCatalogTable: CatalogTable
    private lateinit var mockTable: Table

    @BeforeEach
    fun setup() {
        mockSparkSession = mockk()
        mockSessionState = mockk()
        mockSessionCatalog = mockk()
        mockCatalog = mockk()
        mockCatalogTable = mockk()
        mockTable = mockk()

        every { mockSparkSession.sessionState() } returns mockSessionState
        every { mockSessionState.catalog() } returns mockSessionCatalog
        every { mockSparkSession.catalog() } returns mockCatalog

        every {
            mockSessionCatalog.getTempViewOrPermanentTableMetadata(any<TableIdentifier>())
        } returns mockCatalogTable

        every {
            mockCatalog.getTable(any<String>(), any<String>())
        } returns mockTable
    }

    private fun createExtractor(
        schema: String = "spark_catalog.myschema",
        tableName: String = "mytable",
    ): DatasourceV1LikeTableExtractor {
        return DatasourceV1LikeTableExtractor(mockSparkSession, schema, tableName)
    }

    private fun mockCatalogStatistics(
        sizeInBytes: Long,
        rowCount: Long? = null,
        colStats: scala.collection.immutable.Map<String, CatalogColumnStat> =
            `Map$`.`MODULE$`.empty<String, CatalogColumnStat>(),
    ): CatalogStatistics {
        val stats = mockk<CatalogStatistics>()
        every { stats.sizeInBytes() } returns scala.math.`BigInt$`.`MODULE$`.apply(sizeInBytes)
        every { stats.rowCount() } returns if (rowCount != null) {
            Option.apply(scala.math.`BigInt$`.`MODULE$`.apply(rowCount))
        } else {
            Option.empty()
        }
        every { stats.colStats() } returns colStats
        return stats
    }

    private fun buildColStatsMap(
        vararg entries: Pair<String, CatalogColumnStat>,
    ): scala.collection.immutable.Map<String, CatalogColumnStat> {
        var map = `Map$`.`MODULE$`.empty<String, CatalogColumnStat>()
        for ((key, value) in entries) {
            @Suppress("UNCHECKED_CAST")
            map = map.updated(key, value)
                    as scala.collection.immutable.Map<String, CatalogColumnStat>
        }
        return map
    }

    private fun mockCatalogColumnStat(
        distinctCount: Long? = null,
        min: String? = null,
        max: String? = null,
        nullCount: Long? = null,
        avgLen: Long? = null,
        maxLen: Long? = null,
    ): CatalogColumnStat {
        val colStat = mockk<CatalogColumnStat>()
        every { colStat.distinctCount() } returns if (distinctCount != null) {
            Option.apply(scala.math.`BigInt$`.`MODULE$`.apply(distinctCount))
        } else {
            Option.empty()
        }
        every { colStat.min() } returns if (min != null) Option.apply(min) else Option.empty()
        every { colStat.max() } returns if (max != null) Option.apply(max) else Option.empty()
        every { colStat.nullCount() } returns if (nullCount != null) {
            Option.apply(scala.math.`BigInt$`.`MODULE$`.apply(nullCount))
        } else {
            Option.empty()
        }
        every { colStat.avgLen() } returns if (avgLen != null) {
            Option.apply(avgLen)
        } else {
            Option.empty()
        }
        every { colStat.maxLen() } returns if (maxLen != null) {
            Option.apply(maxLen)
        } else {
            Option.empty()
        }
        return colStat
    }

    @Test
    fun `extractTableStatistics returns correct stats when statistics present`() {
        val createTime = 1700000000000L
        val stats = mockCatalogStatistics(sizeInBytes = 4096, rowCount = 100)
        every { mockCatalogTable.stats() } returns Option.apply(stats)
        every { mockCatalogTable.createTime() } returns createTime

        val extractor = createExtractor()
        val result = extractor.extractTableStatistics()

        assertNotNull(result)
        assertEquals(4096L, result!!.sizeInBytes)
        assertEquals(100L, result.totalRecords)
        assertEquals(createTime, result.lastModified)
        assertNull(result.numFiles)
    }

    @Test
    fun `extractTableStatistics returns null when no statistics available`() {
        every { mockCatalogTable.stats() } returns Option.empty()

        val extractor = createExtractor()
        assertNull(extractor.extractTableStatistics())
    }

    @Test
    fun `constructor throws when table not found in catalog`() {
        every {
            mockSessionCatalog.getTempViewOrPermanentTableMetadata(any<TableIdentifier>())
        } throws RuntimeException("Table not found")

        assertThrows(RuntimeException::class.java) {
            createExtractor()
        }
    }

    @Test
    fun `extractColumnStatistics returns numeric stats for column`() {
        val colStat = mockCatalogColumnStat(
            distinctCount = 50,
            min = "0",
            max = "999",
            nullCount = 5,
        )
        val colStatsMap = buildColStatsMap("age" to colStat)
        val stats = mockCatalogStatistics(sizeInBytes = 1024, colStats = colStatsMap)
        every { mockCatalogTable.stats() } returns Option.apply(stats)

        val extractor = createExtractor()
        val result = extractor.extractColumnStatistics(listOf("age"))

        val ageStats = result["age"]!!
        assertEquals(4, ageStats.size)
        assertTrue(ageStats.any { it.name == "distinctCount" && it.statValue == "50" })
        assertTrue(ageStats.any { it.name == "min" && it.statValue == "0" })
        assertTrue(ageStats.any { it.name == "max" && it.statValue == "999" })
        assertTrue(ageStats.any { it.name == "nullCount" && it.statValue == "5" })
    }

    @Test
    fun `extractColumnStatistics returns string stats for column`() {
        val colStat = mockCatalogColumnStat(avgLen = 25, maxLen = 100)
        val colStatsMap = buildColStatsMap("name" to colStat)
        val stats = mockCatalogStatistics(sizeInBytes = 1024, colStats = colStatsMap)
        every { mockCatalogTable.stats() } returns Option.apply(stats)

        val extractor = createExtractor()
        val result = extractor.extractColumnStatistics(listOf("name"))

        val nameStats = result["name"]!!
        assertEquals(2, nameStats.size)
        assertTrue(nameStats.any { it.name == "avgLen" && it.statValue == "25" })
        assertTrue(nameStats.any { it.name == "maxLen" && it.statValue == "100" })
    }

    @Test
    fun `extractColumnStatistics returns empty list when column not in stats map`() {
        val stats = mockCatalogStatistics(sizeInBytes = 1024)
        every { mockCatalogTable.stats() } returns Option.apply(stats)

        val extractor = createExtractor()
        val result = extractor.extractColumnStatistics(listOf("unknown_col"))

        assertEquals(emptyList<com.iomete.catalogsync.extract.ColumnStat>(), result["unknown_col"])
    }

    @Test
    fun `extractColumnStatistics handles multiple columns`() {
        val colStatA = mockCatalogColumnStat(distinctCount = 10)
        val colStatB = mockCatalogColumnStat(avgLen = 20)
        val colStatC = mockCatalogColumnStat(nullCount = 3)
        val colStatsMap = buildColStatsMap("colA" to colStatA, "colB" to colStatB, "colC" to colStatC)
        val stats = mockCatalogStatistics(sizeInBytes = 2048, colStats = colStatsMap)
        every { mockCatalogTable.stats() } returns Option.apply(stats)

        val extractor = createExtractor()
        val result = extractor.extractColumnStatistics(listOf("colA", "colB", "colC"))

        assertEquals(3, result.size)
        assertTrue(result.containsKey("colA"))
        assertTrue(result.containsKey("colB"))
        assertTrue(result.containsKey("colC"))
        assertEquals(1, result["colA"]!!.size)
        assertEquals(1, result["colB"]!!.size)
        assertEquals(1, result["colC"]!!.size)
    }

    @Test
    fun `extractColumnStatistics returns empty list when all stat fields are None`() {
        val colStat = mockCatalogColumnStat() // all defaults to null -> Option.empty()
        val colStatsMap = buildColStatsMap("col" to colStat)
        val stats = mockCatalogStatistics(sizeInBytes = 512, colStats = colStatsMap)
        every { mockCatalogTable.stats() } returns Option.apply(stats)

        val extractor = createExtractor()
        val result = extractor.extractColumnStatistics(listOf("col"))

        assertEquals(emptyList<com.iomete.catalogsync.extract.ColumnStat>(), result["col"])
    }

    @Test
    fun `getTableType returns MANAGED when table is managed`() {
        every { mockCatalogTable.stats() } returns Option.empty()
        every { mockTable.tableType() } returns "MANAGED"

        val extractor = createExtractor()
        assertEquals("MANAGED", extractor.getTableType)
    }

    @Test
    fun `getTableType returns EXTERNAL when table is external`() {
        every { mockCatalogTable.stats() } returns Option.empty()
        every { mockTable.tableType() } returns "EXTERNAL"

        val extractor = createExtractor()
        assertEquals("EXTERNAL", extractor.getTableType)
    }
}
