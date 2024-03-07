package com.iomete.catalogsync.extract.utils

import com.iomete.catalogsync.*
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


class ColumnTagExtractor(
    private val spark: SparkSession,
    private val presidioClient: PresidioClient
) {
    fun extract(
        fullTableName: String,
        columns: List<String>
    ): Map<String, List<String>> {
        logger.info("detectColumnTags for {}", fullTableName)

        val result = mutableMapOf<String, List<String>>()

        try {
            val sampleData = spark.sql("SELECT * FROM $fullTableName TABLESAMPLE (5 ROWS)")
                .collectAsList().orEmpty()

            columns.forEach { columnName ->
                val columnSampleData =
                    sampleData.map { it.get(columnName).toString() }
                        .filter { it.isNotEmpty() }
                        .distinct().firstOrNull()

                val detectedTags = detectedTags(columnSampleData)
                logger.info(
                    "table={} column={} detected-tags={} for sample data: {}",
                    fullTableName, columnName, detectedTags, columnSampleData
                )
                result[columnName] = detectedTags
            }
        } catch (ex: Exception) {
            logger.error("Error on detectColumnTags. Table: {}. Message: {}", fullTableName, ex.message)
        }

        return result
    }

    private fun detectedTags(input: String?): List<String> {
        if (input.isNullOrBlank()) {
            return emptyList()
        }

        val responseData = presidioClient.analyze(PresidioRequest(input))
        val sortedResult = responseData.sortedByDescending { it.score }
        val topResult = sortedResult.mapNotNull { it.entityType }.distinct().firstOrNull() ?: return listOf()

        val detectedTags = mutableListOf(topResult.name)

        if (PII_ENTITY_TYPES.contains(topResult)) {
            detectedTags.add("PII")
        }

        if (PCI_ENTITY_TYPES.contains(topResult)) {
            detectedTags.add("PCI")
        }

        return detectedTags.map { "DETECTED_${it.uppercase()}" }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ColumnTagExtractor::class.java)
    }

}