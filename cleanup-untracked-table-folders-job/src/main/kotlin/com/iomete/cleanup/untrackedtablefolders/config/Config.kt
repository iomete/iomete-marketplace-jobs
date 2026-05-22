package com.iomete.cleanup.untrackedtablefolders.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.jboss.logging.Logger
import java.io.File

private const val APPLICATION_CONFIG_PATH = "/etc/configs/application.json"

data class ApplicationConfig(
    val catalog: String,

    val databases: List<String>,

    @field:JsonProperty("exclude_paths")
    val excludePaths: List<String> = emptyList(),

    @field:JsonProperty("exclude_database_folders")
    val excludeDatabaseFolders: List<String> = emptyList(),

    @field:JsonProperty("older_than_hours")
    val olderThanHours: Long = 24,

    @field:JsonProperty("dry_run")
    val dryRun: Boolean = true,

    @field:JsonProperty("delete_enabled")
    val deleteEnabled: Boolean = false,

    @field:JsonProperty("max_candidate_folders_per_database")
    val maxCandidateFoldersPerDatabase: Int = 10,
) {
    fun validate() {
        require(catalog.isNotBlank()) { "catalog must not be blank" }

        require(databases.isNotEmpty()) {
            "databases must contain at least one database name"
        }

        val configuredDatabaseSet = databases.toSet()

        excludeDatabaseFolders.forEach { excludedDatabaseFolder ->
            val parts = excludedDatabaseFolder.split(".")

            require(parts.size == 2 && parts[0].isNotBlank() && parts[1].isNotBlank()) {
                "exclude_database_folders entries must use database.folder format: $excludedDatabaseFolder"
            }

            val database = parts[0]
            val folder = parts[1]

            require(database in configuredDatabaseSet) {
                "exclude_database_folders database must be listed in databases: $excludedDatabaseFolder"
            }

            require('/' !in folder) {
                "exclude_database_folders folder must be an immediate child folder name without '/': $excludedDatabaseFolder"
            }
        }

        require(olderThanHours >= 0) {
            "older_than_hours must be greater than or equal to 0"
        }

        require(maxCandidateFoldersPerDatabase >= 0) {
            "max_candidate_folders_per_database must be greater than or equal to 0"
        }

        if (!dryRun) {
            require(deleteEnabled) {
                "delete_enabled must be true when dry_run is false"
            }
        }
    }
}

@ApplicationScoped
class ConfigProducer {
    private val logger = Logger.getLogger(ConfigProducer::class.java)

    @Produces
    @ApplicationScoped
    fun applicationConfig(): ApplicationConfig {
        val configFile = File(APPLICATION_CONFIG_PATH)

        if (!configFile.exists()) {
            val message =
                "Config file was not found at $APPLICATION_CONFIG_PATH. This job requires an explicit configuration file."
            logger.error(message)
            throw IllegalStateException(message)
        }

        val objectMapper = ObjectMapper()
            .registerModule(KotlinModule.Builder().build())
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)

        return objectMapper.readValue<ApplicationConfig>(configFile).also { it.validate() }
    }
}
