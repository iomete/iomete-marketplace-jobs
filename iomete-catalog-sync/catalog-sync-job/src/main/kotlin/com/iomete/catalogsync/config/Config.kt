package com.iomete.catalogsync.config

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.slf4j.LoggerFactory
import java.io.File

data class IncludeExcludeOptions(
    val include: List<String> = emptyList(),
    val exclude: List<String> = emptyList(),
)

data class ApplicationConfig(
    val catalog: IncludeExcludeOptions = IncludeExcludeOptions(),
    val schemas: Map<String, IncludeExcludeOptions> = emptyMap(),
)

@ApplicationScoped
class ApplicationConfigFactory {
    private val logger = LoggerFactory.getLogger(this::class.java)
    private val mapper = jacksonObjectMapper()

    @Produces
    @ApplicationScoped
    fun applicationConfig(): ApplicationConfig {
        val configPath = "/etc/configs/application.json"
        val file = File(configPath)

        return if (file.exists()) {
            logger.info("Reading JSON config from: {}", configPath)
            try {
                mapper.readValue(file)
            } catch (e: Exception) {
                logger.error("Failed to parse config file: {}", e.message)
                ApplicationConfig()
            }
        } else {
            logger.warn("Config file not found at {}. Using default values.", configPath)
            ApplicationConfig()
        }
    }
}
