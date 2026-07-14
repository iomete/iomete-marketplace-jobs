package com.iomete.backup.config.internal

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.fasterxml.jackson.databind.exc.InvalidNullException
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.ConfigParseException
import org.slf4j.LoggerFactory
import java.io.File

object Parser {
    private val logger = LoggerFactory.getLogger(Parser::class.java)

    private val mapper =
        jacksonObjectMapper().apply {
            configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
        }

    fun parseFromFile(path: String): ApplicationConfig {
        val file = File(path)

        logger.info("Reading configuration file: {}", path)

        if (!file.exists()) {
            throw ConfigParseException("Configuration file not found: $path")
        }

        return parse(file.readText())
    }

    fun parse(json: String): ApplicationConfig =
        try {
            mapper.readValue<ApplicationConfig>(json)
        } catch (e: MismatchedInputException) {
            logger.debug("Configuration binding failed", e)
            throw ConfigParseException(buildParseErrorMessage(e), e)
        } catch (e: JsonProcessingException) {
            logger.debug("Malformed configuration JSON", e)
            throw ConfigParseException(syntaxErrorMessage(e), e)
        } catch (e: Exception) {
            logger.debug("Configuration parsing failed", e)
            throw ConfigParseException("Failed to parse configuration", e)
        }

    private fun syntaxErrorMessage(e: JsonProcessingException): String {
        val loc = e.location ?: return "Invalid JSON: malformed configuration"
        return "Invalid JSON syntax at line ${loc.lineNr}, column ${loc.columnNr}"
    }

    private fun buildParseErrorMessage(e: MismatchedInputException): String {
        val at = pathOf(e).let { if (it.isBlank()) "" else " at '$it'" }

        return when (e) {
            is InvalidNullException -> {
                val field = pathOf(e).ifBlank { e.propertyName?.toString().orEmpty() }
                if (field.isBlank()) "Missing required field" else "Missing required field '$field'"
            }

            is InvalidTypeIdException -> {
                if (e.typeId == null) {
                    "Missing required 'type' field$at"
                } else {
                    "Unknown type '${e.typeId}'$at"
                }
            }

            is InvalidFormatException -> {
                "Invalid value '${e.value}'$at (expected ${e.targetType.simpleName})"
            }

            else -> {
                "Invalid value$at (expected ${e.targetType?.simpleName ?: "a different type"})"
            }
        }
    }

    private fun pathOf(e: JsonMappingException): String =
        buildString {
            e.path.forEach { ref ->
                when {
                    ref.index >= 0 -> {
                        append("[${ref.index}]")
                    }

                    ref.fieldName != null -> {
                        if (isNotEmpty()) append('.')
                        append(ref.fieldName)
                    }
                }
            }
        }
}
