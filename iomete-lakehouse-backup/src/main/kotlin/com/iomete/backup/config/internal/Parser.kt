package com.iomete.backup.config.internal

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.JsonMappingException
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.exc.InvalidFormatException
import com.fasterxml.jackson.databind.exc.InvalidNullException
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException
import com.fasterxml.jackson.databind.exc.MismatchedInputException
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.ConfigParseException
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException

object Parser {
    private val logger = LoggerFactory.getLogger(Parser::class.java)

    private val PLACEHOLDER = Regex("""\$\{([A-Za-z_][A-Za-z0-9_]*)}""")

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
        if (!file.isFile) {
            throw ConfigParseException("Configuration path is not a file: $path")
        }

        val content =
            try {
                file.readText()
            } catch (e: IOException) {
                logger.debug("Failed to read configuration file", e)
                throw ConfigParseException("Unable to read configuration file: $path", e)
            }

        return parse(content)
    }

    fun parse(
        json: String,
        env: Map<String, String> = System.getenv(),
    ): ApplicationConfig =
        try {
            val root = mapper.readTree(json)
            mapper.treeToValue(resolveEnvironment(root, env), ApplicationConfig::class.java)
        } catch (e: ConfigParseException) {
            throw e
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

    private fun resolveEnvironment(
        root: JsonNode,
        env: Map<String, String>,
    ): JsonNode {
        val missing = sortedSetOf<String>()
        val resolved = resolveEnvironment(root, env, missing)
        if (missing.isNotEmpty()) {
            throw ConfigParseException(
                "Undefined environment variable(s) referenced in configuration: ${missing.joinToString(", ")}",
            )
        }
        return resolved
    }

    private fun resolveEnvironment(
        node: JsonNode,
        env: Map<String, String>,
        missing: MutableSet<String>,
    ): JsonNode {
        if (node.isTextual) {
            val match = PLACEHOLDER.matchEntire(node.textValue()) ?: return node
            val name = match.groupValues[1]
            return env[name]?.let(mapper.nodeFactory::textNode) ?: node.also { missing += name }
        }

        when (node) {
            is ObjectNode -> {
                node.fieldNames().asSequence().toList().forEach { field ->
                    node.set<JsonNode>(field, resolveEnvironment(node[field], env, missing))
                }
            }

            is ArrayNode -> {
                repeat(node.size()) { index ->
                    node.set(index, resolveEnvironment(node[index], env, missing))
                }
            }
        }
        return node
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
