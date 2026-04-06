package com.iomete.catalogsync.presidio

import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.extract.get
import jakarta.enterprise.context.ApplicationScoped
import org.apache.spark.sql.SparkSession
import org.eclipse.microprofile.config.ConfigProvider
import org.eclipse.microprofile.rest.client.inject.RestClient
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors

@ApplicationScoped
class PIIDetectionService(
    @param:RestClient private val presidioClient: PresidioClient,
) {
    fun extract(
        spark: SparkSession,
        catalog: String,
        fullTableName: String,
        columns: List<String>,
    ): Map<String, List<String>> {
        if (isPiiDetectionEnabled().not()) {
            return emptyMap()
        }

        logger.info("detectColumnTags for {}", fullTableName)

        val result = mutableMapOf<String, List<String>>()

        try {
            val sampleData =
                spark
                    .sql("SELECT * FROM $fullTableName TABLESAMPLE (5 ROWS)")
                    .collectAsList()
                    .orEmpty()

            val executor = Executors.newFixedThreadPool(columns.size.coerceIn(1, 16))
            try {
                val futures =
                    columns.map { columnName ->
                        executor.submit<Pair<String, List<String>>> {
                            val columnSampleData =
                                sampleData
                                    .map { it.get(columnName).toString() }
                                    .filter { it.isNotEmpty() }
                                    .distinct()
                                    .firstOrNull()

                            val detectedTags = detectedTags(columnSampleData)
                            logger.info(
                                "table={} column={} detected-tags={} for sample data: {}",
                                fullTableName,
                                columnName,
                                detectedTags,
                                columnSampleData,
                            )
                            columnName to detectedTags
                        }
                    }
                futures.forEach { future ->
                    val (columnName, tags) = future.get()
                    result[columnName] = tags
                }
            } finally {
                executor.shutdown()
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

        return detectedTags.map { "DETECTED:${it.uppercase()}" }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(PIIDetectionService::class.java)
    }

    private fun isPiiDetectionEnabled(): Boolean {
        val config = ConfigProvider.getConfig()

        // Check if the environment variable PII_DETECTION_ENABLED exists
        val envVar = config.getOptionalValue("PII_DETECTION_ENABLED", String::class.java).orElse("false")

        // Check if the system property piiDetectionEnabled exists
        val systemProperty = System.getProperty("piiDetectionEnabled", "false")

        // Determine the value to use
        return envVar.toBoolean() || systemProperty.toBoolean()
    }
}
