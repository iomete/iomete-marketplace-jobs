package com.iomete.catalogsync.config

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.slf4j.LoggerFactory
import java.io.File

@JsonIgnoreProperties(ignoreUnknown = true)
data class ApplicationConfig(
    @field:JsonProperty("inclusion_rules")
    val inclusionRules: InclusionRules = InclusionRules(),
    @field:JsonProperty("exclusion_rules")
    val exclusionRules: ExclusionRules = ExclusionRules(),
)

/**
 * Limits the sync to the listed assets. An empty list means no limit, so every catalog is synced.
 * Exclusion rules still apply to whatever is included.
 */
data class InclusionRules(
    val catalogs: CatalogInclusionRule = CatalogInclusionRule(),
)

data class CatalogInclusionRule(
    val names: List<String> = emptyList(),
)

data class ExclusionRules(
    val catalogs: CatalogExclusionRule = CatalogExclusionRule(),
    val schemas: GeneralFilter = GeneralFilter(),
    val tables: GeneralFilter = GeneralFilter(),
    @field:JsonProperty("default_rule")
    val defaultRule: DefaultRule = DefaultRule(),
)

data class CatalogExclusionRule(
    val names: List<String> = emptyList(),
    @field:JsonProperty("filter_by_properties")
    val filterByProperties: Map<String, String> = emptyMap(),
)

data class GeneralFilter(
    @field:JsonProperty("filter_by_properties")
    val filterByProperties: Map<String, String> = emptyMap(),
)

data class DefaultRule(
    @field:JsonProperty("filter_by_properties")
    val filterByProperties: Map<String, String> =
        mapOf(
            "iomete.governance.index" to "false",
        ),
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
