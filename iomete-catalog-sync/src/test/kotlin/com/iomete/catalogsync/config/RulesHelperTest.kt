package com.iomete.catalogsync.config

import com.iomete.catalogsync.CoreClient
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows

class RulesHelperTest {

    @Test
    fun `enforceCatalogExclusionRules should throw exception for excluded catalog name`() {
        val exclusionRules = ExclusionRules(catalogs = CatalogExclusionRule(names = listOf("excluded_catalog")))
        val catalog = CoreClient.CatalogDetails(name = "excluded_catalog", type = emptyList())

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceCatalogExclusionRules should not throw exception for non-excluded catalog`() {
        val exclusionRules = ExclusionRules(catalogs = CatalogExclusionRule(names = listOf("excluded_catalog")))
        val catalog = CoreClient.CatalogDetails(name = "included_catalog", type = emptyList())

        assertDoesNotThrow {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceCatalogExclusionRules should throw exception for matching properties`() {
        val exclusionRules = ExclusionRules(
            catalogs = CatalogExclusionRule(
                filterByProperties = mapOf("key" to "value")
            )
        )
        val catalog = CoreClient.CatalogDetails(
            name = "test_catalog",
            type = emptyList(),
            sparkProperties = mapOf("key" to "value")
        )

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceSchemaExclusionRules should throw exception for matching properties`() {
        val exclusionRules = ExclusionRules(schemas = GeneralFilter(filterByProperties = mapOf("key" to "value")))
        val schema = "excluded_schema"
        val props = mapOf("key" to "value")

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceSchemaExclusionRules(schema, props)
        }
    }

    @Test
    fun `enforceTableExclusionRules should throw exception for matching properties`() {
        val exclusionRules = ExclusionRules(tables = GeneralFilter(filterByProperties = mapOf("key" to "value")))
        val table = "excluded_table"
        val props = mapOf("key" to "value")

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceTableExclusionRules(table, props)
        }
    }

    @Test
    fun `default rule should be applied to all enforcement functions`() {
        val exclusionRules = ExclusionRules(
            defaultRule = DefaultRule(
                filterByProperties = mapOf("iomete.governance.index" to "false")
            )
        )
        val catalog = CoreClient.CatalogDetails(
            name = "test_catalog",
            type = emptyList(),
            sparkProperties = mapOf("iomete.governance.index" to "false")
        )
        val schemaProps = mapOf("iomete.governance.index" to "false")
        val tableProps = mapOf("iomete.governance.index" to "false")

        assertThrows<ExcludedItemException>("Catalog should be excluded by default rule") {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }

        assertThrows<ExcludedItemException>("Schema should be excluded by default rule") {
            exclusionRules.enforceSchemaExclusionRules("test_schema", schemaProps)
        }

        assertThrows<ExcludedItemException>("Table should be excluded by default rule") {
            exclusionRules.enforceTableExclusionRules("test_table", tableProps)
        }
    }
}
