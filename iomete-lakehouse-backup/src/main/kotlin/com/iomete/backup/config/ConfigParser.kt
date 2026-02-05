package com.iomete.backup.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import java.io.File

/**
 * Exception thrown when configuration parsing fails.
 */
class ConfigParseException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/**
 * Parser for application configuration.
 * Handles JSON parsing from files or strings.
 * 
 * Note: Environment variable substitution for secrets is handled externally
 * by core/cluster service that triggers the job.
 */
object ConfigParser {
    private val logger = LoggerFactory.getLogger(ConfigParser::class.java)
    
    private val mapper = jacksonObjectMapper().apply {
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    /**
     * Parse configuration from JSON string.
     *
     * @param json The JSON configuration string
     * @return Parsed ApplicationConfig
     * @throws ConfigParseException if parsing fails
     */
    fun parse(json: String): ApplicationConfig {
        return try {
            mapper.readValue<ApplicationConfig>(json)
        } catch (e: JsonMappingException) {
            val message = buildParseErrorMessage(e)
            logger.error("Failed to parse configuration: {}", message)
            throw ConfigParseException(message, e)
        } catch (e: Exception) {
            logger.error("Failed to parse configuration: {}", e.message)
            throw ConfigParseException("Failed to parse configuration: ${e.message}", e)
        }
    }

    /**
     * Parse configuration from a file.
     *
     * @param path Path to the configuration file
     * @return Parsed ApplicationConfig
     * @throws ConfigParseException if file doesn't exist or parsing fails
     */
    fun parseFromFile(path: String): ApplicationConfig {
        val file = File(path)
        if (!file.exists()) {
            throw ConfigParseException("Configuration file not found: $path")
        }
        
        logger.info("Reading configuration from: {}", path)
        val json = file.readText()
        return parse(json)
    }

    /**
     * Build a user-friendly error message from Jackson exception.
     */
    private fun buildParseErrorMessage(e: JsonMappingException): String {
        val path = e.path.joinToString(".") { ref ->
            if (ref.index >= 0) "[${ref.index}]" else ref.fieldName ?: ""
        }
        
        return when {
            e.message?.contains("Unrecognized field") == true -> 
                "Unknown field in configuration at '$path': ${e.originalMessage}"
            e.message?.contains("Missing required") == true ->
                "Missing required field at '$path': ${e.originalMessage}"
            e.message?.contains("Cannot deserialize") == true ->
                "Invalid value at '$path': ${e.originalMessage}"
            else ->
                "Parse error at '$path': ${e.originalMessage ?: e.message}"
        }
    }
}
