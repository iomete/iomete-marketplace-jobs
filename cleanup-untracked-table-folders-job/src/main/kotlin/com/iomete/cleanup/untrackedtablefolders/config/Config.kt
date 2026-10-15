package com.iomete.cleanup.untrackedtablefolders.config

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import com.iomete.cleanup.untrackedtablefolders.storage.StoragePathUtils
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

    @field:JsonProperty("collect_size_statistics")
    val collectSizeStatistics: Boolean? = null,

    // Listed for reporting only. Approval never makes a location eligible for deletion.
    @field:JsonProperty("report_only_storage_roots")
    val reportOnlyStorageRoots: Map<String, List<String>> = emptyMap(),
) {
    fun deprecationWarnings(): List<String> =
        if (collectSizeStatistics == false) {
            listOf(
                "collect_size_statistics=false is deprecated and ignored. Folder statistics are always collected " +
                    "because the same object listing is needed to establish folder age safely."
            )
        } else {
            emptyList()
        }

    fun validate() {
        require(catalog.isNotBlank()) { "catalog must not be blank" }

        require(databases.isNotEmpty()) {
            "databases must contain at least one database name"
        }

        val configuredDatabaseSet = databases.toSet()

        // An exclusion that cannot be compared with storage paths would protect nothing.
        excludePaths.forEach { excludePath ->
            require(runCatching { StoragePathUtils.isQualifiedLocation(excludePath) }.getOrDefault(false)) {
                "exclude_paths entries must include a scheme and bucket, for example s3a://bucket/path: $excludePath"
            }
        }

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

        reportOnlyStorageRoots.forEach { (database, roots) ->
            require(database in configuredDatabaseSet) {
                "report_only_storage_roots database must be listed in databases: $database"
            }

            require(roots.none { it.isBlank() }) {
                "report_only_storage_roots entries must not be blank: $database"
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

        return applicationConfigObjectMapper().readValue<ApplicationConfig>(configFile).also { config ->
            config.validate()
            config.deprecationWarnings().forEach { logger.warn(it) }
        }
    }
}

internal fun applicationConfigObjectMapper(): ObjectMapper =
    ObjectMapper()
        .registerModule(KotlinModule.Builder().build())
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)
