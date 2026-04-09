package com.iomete.catalogsync.extract.utils

import com.iomete.catalogsync.*
import org.apache.spark.sql.SparkSession
import org.eclipse.microprofile.config.ConfigProvider
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.Executors


class ColumnTagExtractor(
    private val spark: SparkSession,
    private val presidioClient: PresidioClient
) {
    private val piiDetectionEnabled: Boolean by lazy { checkPiiDetectionEnabled() }

    private val piiResultCache = ConcurrentHashMap<String, List<String>>()

    private val presidioExecutor = Executors.newFixedThreadPool(
        (System.getenv("PII_PARALLELISM")?.toIntOrNull() ?: 8).coerceIn(1, 32)
    )

    fun extract(
        fullTableName: String,
        columns: List<String>
    ): Map<String, List<String>> {
        if (!piiDetectionEnabled) {
            return emptyMap()
        }

        logger.info("detectColumnTags for {}", fullTableName)

        val result = mutableMapOf<String, List<String>>()

        try {
            val sampleData = spark.sql("SELECT * FROM $fullTableName TABLESAMPLE (5 ROWS)")
                .collectAsList().orEmpty()

            // Prepare column sample data
            val columnSamples = columns.associateWith { columnName ->
                sampleData.map { it.get(columnName).toString() }
                    .filter { it.isNotEmpty() }
                    .distinct().firstOrNull()
            }

            // Parallelize Presidio HTTP calls across columns
            val futures = columnSamples.map { (columnName, sampleValue) ->
                columnName to CompletableFuture.supplyAsync({
                    val detectedTags = detectedTags(sampleValue)
                    logger.info(
                        "table={} column={} detected-tags={} for sample data: {}",
                        fullTableName, columnName, detectedTags, sampleValue
                    )
                    detectedTags
                }, presidioExecutor)
            }

            futures.forEach { (columnName, future) ->
                result[columnName] = future.join()
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

        // Return cached result for identical sample values
        piiResultCache[input]?.let { return it }

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

        val tags = detectedTags.map { "DETECTED_${it.uppercase()}" }
        piiResultCache[input] = tags
        return tags
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ColumnTagExtractor::class.java)
    }

    private fun checkPiiDetectionEnabled(): Boolean {
        val config = ConfigProvider.getConfig()
        val envVar = config.getOptionalValue("PII_DETECTION_ENABLED", String::class.java).orElse("false")
        val systemProperty = System.getProperty("piiDetectionEnabled", "false")
        return envVar.toBoolean() || systemProperty.toBoolean()
    }
}
