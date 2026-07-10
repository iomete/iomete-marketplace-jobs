package com.iomete.backup

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.ConfigParseException
import com.iomete.backup.config.ConfigParser
import com.iomete.backup.config.ConfigUtils
import com.iomete.backup.config.ConfigValidator
import com.iomete.backup.config.ValidationResult
import com.iomete.backup.copy.HadoopConfigBuilder
import com.iomete.backup.copy.PathResolver
import com.iomete.backup.fs.FileLister
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.slf4j.LoggerFactory
import java.net.URI

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
            throw e
        }
    }

    private fun run(args: Array<String>) {
        // Determine config path
        val configPath = args.getOrNull(0) ?: DEFAULT_CONFIG_PATH
        logger.info("Configuration path: {}", configPath)

        val config = parseConfig(configPath)
        validateConfig(config)
        logRedactedConfig(config)

        // Initialize Spark session
        logger.info("Initializing Spark session...")
        val spark = SparkSessionProvider.sparkSession
        logger.info("Spark session ready")
        logger.info("  Application ID: {}", spark.sparkContext().applicationId())

        try {
            // Enumerate source files
            logger.info("-".repeat(60))
            logger.info("Enumerating source files...")

            val sourceConfMap = HadoopConfigBuilder.buildConfigMap(config.source)
            val sourceConf = HadoopConfigBuilder.toHadoopConf(sourceConfMap)
            val sourceRoot = PathResolver.resolveRootUri(config.source)

            val files = FileSystem.newInstance(URI(sourceRoot), sourceConf).use { sourceFs ->
                val fileLister = FileLister(sourceFs)
                fileLister.listRecursively(Path(sourceRoot)).toList()
            }

            logger.info("Found {} files to copy", files.size)
            val totalBytes = files.sumOf { it.size }
            logger.info("Total size: {} bytes ({} MB)", totalBytes, totalBytes / (1024 * 1024))

            if (files.isEmpty()) {
                logger.info("No files found in source. Nothing to copy.")
                return
            }
        } finally {
            // Stop Spark session
            logger.info("Stopping Spark session...")
            SparkSessionProvider.stop()
            logger.info("Spark session stopped")
        }

    }

    private fun parseConfig(configPath: String): ApplicationConfig {
        logger.info("Parsing configuration...")
        val config = try {
            ConfigParser.parseFromFile(configPath)
        } catch (e: ConfigParseException) {
            logger.error("Configuration parsing failed: {}", e.message)
            throw e
        }
        logger.info("Configuration parsed successfully")
        return config
    }

    private fun validateConfig(config: ApplicationConfig) {
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
    }

    private fun logRedactedConfig(config: ApplicationConfig) {
        logger.info("-".repeat(60))
        logger.info("Parsed Configuration (secrets redacted):")
        logger.info("-".repeat(60))
        val redactedConfig = ConfigUtils.redactSecrets(config)
        val configJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(redactedConfig)
        configJson.lines().forEach { line ->
            logger.info(line)
        }
        logger.info("-".repeat(60))
    }
}