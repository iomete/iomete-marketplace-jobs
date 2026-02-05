package com.iomete.backup

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.iomete.backup.config.ConfigParseException
import com.iomete.backup.config.ConfigParser
import com.iomete.backup.config.ConfigUtils
import com.iomete.backup.config.ConfigValidator
import com.iomete.backup.config.ValidationResult
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

/**
 * Main application entry point for the IOMETE Lakehouse Backup job.
 *
 * This job:
 * 1. Parses configuration from JSON file
 * 2. Validates the configuration
 * 3. Initializes Spark session
 * 4. Prints the parsed configuration
 * 5. Exits cleanly
 */
object App {
    private val logger = LoggerFactory.getLogger(App::class.java)
    private val mapper = jacksonObjectMapper()

    private const val DEFAULT_CONFIG_PATH = "/etc/configs/application.json"

    @JvmStatic
    fun main(args: Array<String>) {
        logger.info("=".repeat(60))
        logger.info("IOMETE Lakehouse Backup - Starting")
        logger.info("=".repeat(60))

        try {
            run(args)
            logger.info("=".repeat(60))
            logger.info("IOMETE Lakehouse Backup - Completed Successfully")
            logger.info("=".repeat(60))
        } catch (e: Exception) {
            logger.error("=".repeat(60))
            logger.error("IOMETE Lakehouse Backup - Failed")
            logger.error("Error: {}", e.message)
            logger.error("=".repeat(60))
            exitProcess(1)
        }
    }

    private fun run(args: Array<String>) {
        // Determine config path
        val configPath = args.getOrNull(0) ?: DEFAULT_CONFIG_PATH
        logger.info("Configuration path: {}", configPath)

        // Parse configuration
        logger.info("Parsing configuration...")
        val config = try {
            ConfigParser.parseFromFile(configPath)
        } catch (e: ConfigParseException) {
            logger.error("Configuration parsing failed: {}", e.message)
            throw e
        }
        logger.info("Configuration parsed successfully")

        // Validate configuration
        logger.info("Validating configuration...")
        val validationResult = ConfigValidator.validate(config)
        if (validationResult is ValidationResult.Invalid) {
            logger.error("Configuration validation failed with {} error(s):", validationResult.errors.size)
            validationResult.errors.forEach { error ->
                logger.error("  - {}", error)
            }
            throw IllegalArgumentException("Configuration validation failed")
        }
        logger.info("Configuration validation passed")

        // Print parsed configuration (with secrets redacted)
        logger.info("-".repeat(60))
        logger.info("Parsed Configuration (secrets redacted):")
        logger.info("-".repeat(60))
        val redactedConfig = ConfigUtils.redactSecrets(config)
        val configJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(redactedConfig)
        configJson.lines().forEach { line ->
            logger.info(line)
        }
        logger.info("-".repeat(60))

        // Initialize Spark session
        logger.info("Initializing Spark session...")
        val spark = SparkSessionProvider.sparkSession
        logger.info("Spark session ready")
        logger.info("  Application ID: {}", spark.sparkContext().applicationId())

        // Stop Spark session
        logger.info("Stopping Spark session...")
        SparkSessionProvider.stop()
        logger.info("Spark session stopped")
    }
}
