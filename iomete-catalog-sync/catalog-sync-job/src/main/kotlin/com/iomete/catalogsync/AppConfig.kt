package com.iomete.catalogsync

import com.fasterxml.jackson.module.kotlin.*
import jakarta.inject.Singleton
import org.slf4j.LoggerFactory
import java.io.File

data class CatalogConfig(
    val include: List<String> = emptyList(),
    val exclude: List<String> = emptyList()
)

data class AppConfig(
    val catalog: CatalogConfig = CatalogConfig()
)

@Singleton
class ApplicationConfigExtractor {
    private val logger = LoggerFactory.getLogger(this::class.java)
    private val mapper = jacksonObjectMapper()

    fun load(configPath: String = "config.json"): AppConfig {
        val file = File(configPath)

        return if (file.exists()) {
            logger.info("Reading JSON config from: {}", configPath)
            try {
                mapper.readValue(file)
            } catch (e: Exception) {
                logger.error("Failed to parse config file: {}", e.message)
                AppConfig()
            }
        } else {
            logger.warn("Config file not found at {}. Using default values.", configPath)
            AppConfig()
        }
    }
}

