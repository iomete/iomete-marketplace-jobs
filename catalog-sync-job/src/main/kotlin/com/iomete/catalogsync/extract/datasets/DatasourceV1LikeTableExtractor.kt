package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.*
import com.iomete.catalogsync.extract.utils.ColumnTagExtractor
import com.iomete.catalogsync.getOrNull
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.slf4j.LoggerFactory
import scala.Option

const val SPARK_CATALOG_PREFIX = "spark_catalog."

class DatasourceV1LikeTableExtractor(
    spark: SparkSession,
    private val columnTagExtractor: ColumnTagExtractor,
    private val schema: String,
    private val tableName: String
) : TableExtractor, SupportTableStatistics, SupportColumnStatistics, SupportColumnTags {

    private val catalog = spark.sessionState().catalog()
    private val fullName = "$schema.$tableName"
    private val catalogTable = catalog.getTempViewOrPermanentTableMetadata(
        TableIdentifier(tableName, Option.apply(schema.removePrefix(SPARK_CATALOG_PREFIX))))
    private val table = spark.catalog().getTable(schema.removePrefix(SPARK_CATALOG_PREFIX), tableName)

    override val getTableType: String
        get() = table.tableType()

    override fun extractColumnTags(columns: List<String>): Map<String, List<String>> =
        columnTagExtractor.extract(fullName, columns)

    override fun extractColumnStatistics(columns: List<String>): Map<String, List<ColumnStat>> {
        logger.info("extract statistics for {}.{}", schema, tableName)
        val result = mutableMapOf<String, List<ColumnStat>>()
        columns.forEach { columnName ->
            result[columnName] = columnStatistics(catalogTable, columnName)
        }
        return result
    }

    override fun extractTableStatistics(): TableStatistics? {
        val catalogTableStats = catalogTable.stats().getOrNull() ?: return null
        return TableStatistics(
            lastModified = catalogTable.createTime(),
            numFiles = null,
            sizeInBytes = catalogTableStats.sizeInBytes().toLong(),
            totalRecords = catalogTableStats.rowCount().getOrNull()?.toLong()
        )
    }

    private fun columnStatistics(catalogTable: CatalogTable, columnName: String): List<ColumnStat> {
        val resultStats = mutableListOf<ColumnStat>()

        val catalogColumnStatMap = catalogTable.stats().getOrNull()?.colStats() ?: return listOf()
        val catalogColumnStat = catalogColumnStatMap.get(columnName).getOrNull() ?: return listOf()

        catalogColumnStat.distinctCount().getOrNull()?.let {
            resultStats.add(ColumnStat(name = "distinctCount", statValue = it.toString()))
        }
        catalogColumnStat.min().getOrNull()?.let {
            resultStats.add(ColumnStat(name = "min", statValue = it))
        }
        catalogColumnStat.max().getOrNull()?.let {
            resultStats.add(ColumnStat(name = "max", statValue = it))
        }
        catalogColumnStat.nullCount().getOrNull()?.let {
            resultStats.add(ColumnStat(name = "nullCount", statValue = it.toString()))
        }
        catalogColumnStat.avgLen().getOrNull()?.let {
            resultStats.add(ColumnStat(name = "avgLen", statValue = it.toString()))
        }
        catalogColumnStat.maxLen().getOrNull()?.let {
            resultStats.add(ColumnStat(name = "maxLen", statValue = it.toString()))
        }

        return resultStats
    }

    companion object {
        private val logger = LoggerFactory.getLogger(DatasourceV1LikeTableExtractor::class.java)
    }
}