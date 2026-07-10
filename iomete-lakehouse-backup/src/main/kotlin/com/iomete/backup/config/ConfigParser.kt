package com.iomete.backup.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.slf4j.LoggerFactory
import java.io.File

object ConfigParser {

    private val logger = LoggerFactory.getLogger(ConfigParser::class.java)

    private val mapper = jacksonObjectMapper().apply {
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    fun parseFromFile(path: String): ApplicationConfig {
        val file = File(path)
        if (!file.exists()) {
            throw ConfigParseException("Configuration file not found: $path")
        }

        val json = file.readText()
        return parse(json)
    }

    fun parse(json: String): ApplicationConfig {
        return try {
            mapper.readValue<ApplicationConfig>(json)
        } catch (e: JsonMappingException) {
            val message = buildParseErrorMessage(e)
            throw ConfigParseException(message, e)
        } catch (e: Exception) {
            throw ConfigParseException("Failed to parse configuration: ${e.message}", e)
        }
    }

    // is this actually required ???? #TODO
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

class ConfigParseException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)