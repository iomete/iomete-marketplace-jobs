package com.iomete.catalogsync.config

import com.iomete.catalogsync.CoreClient.CatalogDetails
import com.iomete.catalogsync.metadata.MetadataScraper
import org.slf4j.Logger
import org.slf4j.LoggerFactory

val logger: Logger = LoggerFactory.getLogger(MetadataScraper::class.java)

class ExcludedItemException(
    message: String,
) : RuntimeException(message)

inline fun <T> ignoreExcluded(block: () -> T): T? =
    try {
        block()
    } catch (_: ExcludedItemException) {
        null
    }

fun Map<String, Any?>.matchesAnyExclusion(exclusionRules: Map<String, String>): Boolean =
    exclusionRules.any { (key, value) -> this[key] == value }

fun ExclusionRules.enforceCatalogExclusionRules(catalog: CatalogDetails) {
    throwIf({ catalog.name in catalogs.names }) {
        ExcludedItemException("Catalog `$catalog` is excluded from indexing due to name matching.")
    }

    val filters = catalogs.filterByProperties + defaultRule.filterByProperties
    throwIf({ catalog.sparkProperties.matchesAnyExclusion(filters) }) {
        ExcludedItemException("Catalog `$catalog` is excluded from indexing due to properties matching.")
    }
}

fun ExclusionRules.enforceSchemaExclusionRules(
    schema: String,
    props: Map<String, Any?>,
) = throwIf({ props.matchesAnyExclusion(schemas.filterByProperties + defaultRule.filterByProperties) }) {
    ExcludedItemException("Schema `$schema` is excluded from indexing due to properties matching.")
}

fun ExclusionRules.enforceTableExclusionRules(
    table: String,
    props: Map<String, Any?>,
) = throwIf({ props.matchesAnyExclusion(tables.filterByProperties + defaultRule.filterByProperties) }) {
    ExcludedItemException("Table `$table` is excluded from indexing due to properties matching.")
}

inline fun <T> T.throwIf(
    predicate: (T) -> Boolean,
    exception: () -> Throwable,
): T =
    also {
        if (predicate(it)) {
            logger.info(exception().localizedMessage)
            throw exception()
        }
    }
