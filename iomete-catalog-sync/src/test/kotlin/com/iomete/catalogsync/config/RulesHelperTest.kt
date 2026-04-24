package com.iomete.catalogsync.config

import com.iomete.catalogsync.CoreClient
import org.junit.jupiter.api.Assertions.*
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

    @Test
    fun `ignoreExcluded returns value when block does not throw`() {
        val result = ignoreExcluded { "hello" }
        assertEquals("hello", result)
    }

    @Test
    fun `ignoreExcluded returns null when ExcludedItemException is thrown`() {
        val result = ignoreExcluded { throw ExcludedItemException("excluded") }
        assertNull(result)
    }

    @Test
    fun `ignoreExcluded propagates non-exclusion exceptions`() {
        assertThrows<RuntimeException> {
            ignoreExcluded { throw RuntimeException("unexpected") }
        }
    }

    @Test
    fun `enforceCatalogExclusionRules is case-sensitive for names`() {
        val exclusionRules = ExclusionRules(catalogs = CatalogExclusionRule(names = listOf("excluded_catalog")))
        val catalog = CoreClient.CatalogDetails(name = "Excluded_Catalog", type = emptyList())

        assertDoesNotThrow {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceCatalogExclusionRules with empty names list does not exclude`() {
        val exclusionRules = ExclusionRules(catalogs = CatalogExclusionRule(names = emptyList()))
        val catalog = CoreClient.CatalogDetails(name = "any_catalog", type = emptyList())

        assertDoesNotThrow {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceCatalogExclusionRules matches any name in the list`() {
        val exclusionRules = ExclusionRules(catalogs = CatalogExclusionRule(names = listOf("first", "second")))
        val catalog = CoreClient.CatalogDetails(name = "second", type = emptyList())

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceCatalogExclusionRules does not throw when property value differs`() {
        val exclusionRules = ExclusionRules(
            catalogs = CatalogExclusionRule(filterByProperties = mapOf("key" to "expected_value"))
        )
        val catalog = CoreClient.CatalogDetails(
            name = "cat",
            type = emptyList(),
            sparkProperties = mapOf("key" to "different_value")
        )

        assertDoesNotThrow {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceCatalogExclusionRules does not throw when property key is absent`() {
        val exclusionRules = ExclusionRules(
            catalogs = CatalogExclusionRule(filterByProperties = mapOf("missing_key" to "value"))
        )
        val catalog = CoreClient.CatalogDetails(
            name = "cat",
            type = emptyList(),
            sparkProperties = mapOf("other_key" to "value")
        )

        assertDoesNotThrow {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceCatalogExclusionRules throws when one of multiple filter properties matches`() {
        val exclusionRules = ExclusionRules(
            catalogs = CatalogExclusionRule(
                filterByProperties = mapOf("k1" to "v1", "k2" to "v2")
            )
        )
        val catalog = CoreClient.CatalogDetails(
            name = "cat",
            type = emptyList(),
            sparkProperties = mapOf("k1" to "v1", "k2" to "v2")
        )

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceCatalogExclusionRules throws on partial filter match due to any semantics`() {
        val exclusionRules = ExclusionRules(
            catalogs = CatalogExclusionRule(
                filterByProperties = mapOf("k1" to "v1", "k2" to "v2")
            )
        )
        val catalog = CoreClient.CatalogDetails(
            name = "cat",
            type = emptyList(),
            sparkProperties = mapOf("k1" to "v1")
        )

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceCatalogExclusionRules(catalog)
        }
    }

    @Test
    fun `enforceSchemaExclusionRules does not throw when no properties match`() {
        val exclusionRules = ExclusionRules(schemas = GeneralFilter(filterByProperties = mapOf("key" to "value")))

        assertDoesNotThrow {
            exclusionRules.enforceSchemaExclusionRules("schema1", mapOf("other" to "data"))
        }
    }

    @Test
    fun `enforceSchemaExclusionRules default rule governance index false triggers exclusion`() {
        val exclusionRules = ExclusionRules(
            defaultRule = DefaultRule(filterByProperties = mapOf("iomete.governance.index" to "false"))
        )

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceSchemaExclusionRules("schema1", mapOf("iomete.governance.index" to "false"))
        }
    }

    @Test
    fun `enforceSchemaExclusionRules governance index true does not trigger exclusion`() {
        val exclusionRules = ExclusionRules(
            defaultRule = DefaultRule(filterByProperties = mapOf("iomete.governance.index" to "false"))
        )

        assertDoesNotThrow {
            exclusionRules.enforceSchemaExclusionRules("schema1", mapOf("iomete.governance.index" to "true"))
        }
    }

    @Test
    fun `enforceTableExclusionRules does not throw when no properties match`() {
        val exclusionRules = ExclusionRules(tables = GeneralFilter(filterByProperties = mapOf("hidden" to "true")))

        assertDoesNotThrow {
            exclusionRules.enforceTableExclusionRules("tbl", mapOf("hidden" to "false"))
        }
    }

    @Test
    fun `enforceTableExclusionRules default rule excludes table`() {
        val exclusionRules = ExclusionRules(
            defaultRule = DefaultRule(filterByProperties = mapOf("iomete.governance.index" to "false"))
        )

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceTableExclusionRules("tbl", mapOf("iomete.governance.index" to "false"))
        }
    }

    @Test
    fun `enforceTableExclusionRules hidden property exclusion`() {
        val exclusionRules = ExclusionRules(tables = GeneralFilter(filterByProperties = mapOf("hidden" to "true")))

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceTableExclusionRules("tbl", mapOf("hidden" to "true"))
        }
    }

    @Test
    fun `default rule merges with catalog-specific rules - both enforced`() {
        val exclusionRules = ExclusionRules(
            catalogs = CatalogExclusionRule(filterByProperties = mapOf("custom" to "exclude")),
            defaultRule = DefaultRule(filterByProperties = mapOf("default_key" to "default_val"))
        )

        // Excluded by default rule
        assertThrows<ExcludedItemException> {
            exclusionRules.enforceCatalogExclusionRules(
                CoreClient.CatalogDetails(name = "cat", type = emptyList(), sparkProperties = mapOf("default_key" to "default_val"))
            )
        }

        // Excluded by catalog-specific rule
        assertThrows<ExcludedItemException> {
            exclusionRules.enforceCatalogExclusionRules(
                CoreClient.CatalogDetails(name = "cat", type = emptyList(), sparkProperties = mapOf("custom" to "exclude"))
            )
        }
    }

    @Test
    fun `default rule merges with schema-specific rules`() {
        val exclusionRules = ExclusionRules(
            schemas = GeneralFilter(filterByProperties = mapOf("schema_key" to "exclude")),
            defaultRule = DefaultRule(filterByProperties = mapOf("default_key" to "default_val"))
        )

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceSchemaExclusionRules("sch", mapOf("default_key" to "default_val"))
        }

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceSchemaExclusionRules("sch", mapOf("schema_key" to "exclude"))
        }
    }

    @Test
    fun `default rule merges with table-specific rules`() {
        val exclusionRules = ExclusionRules(
            tables = GeneralFilter(filterByProperties = mapOf("table_key" to "exclude")),
            defaultRule = DefaultRule(filterByProperties = mapOf("default_key" to "default_val"))
        )

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceTableExclusionRules("tbl", mapOf("default_key" to "default_val"))
        }

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceTableExclusionRules("tbl", mapOf("table_key" to "exclude"))
        }
    }

    @Test
    fun `default rule alone is still enforced`() {
        val exclusionRules = ExclusionRules(
            defaultRule = DefaultRule(filterByProperties = mapOf("governance" to "false"))
        )

        assertThrows<ExcludedItemException> {
            exclusionRules.enforceCatalogExclusionRules(
                CoreClient.CatalogDetails(name = "cat", type = emptyList(), sparkProperties = mapOf("governance" to "false"))
            )
        }
    }

    @Test
    fun `no default rule and no specific rules excludes nothing`() {
        val exclusionRules = ExclusionRules(
            defaultRule = DefaultRule(filterByProperties = emptyMap())
        )

        assertDoesNotThrow {
            exclusionRules.enforceCatalogExclusionRules(
                CoreClient.CatalogDetails(name = "cat", type = emptyList(), sparkProperties = mapOf("anything" to "value"))
            )
        }

        assertDoesNotThrow {
            exclusionRules.enforceSchemaExclusionRules("sch", mapOf("anything" to "value"))
        }

        assertDoesNotThrow {
            exclusionRules.enforceTableExclusionRules("tbl", mapOf("anything" to "value"))
        }
    }

    @Test
    fun `matchesAnyExclusion empty properties vs non-empty rules returns false`() {
        val properties: Map<String, Any?> = emptyMap()
        val rules = mapOf("key" to "value")
        assertFalse(properties.matchesAnyExclusion(rules))
    }

    @Test
    fun `matchesAnyExclusion non-empty properties vs empty rules returns false`() {
        val properties: Map<String, Any?> = mapOf("key" to "value")
        val rules: Map<String, String> = emptyMap()
        assertFalse(properties.matchesAnyExclusion(rules))
    }

    @Test
    fun `matchesAnyExclusion matching key but null value returns false`() {
        val properties: Map<String, Any?> = mapOf("key" to null)
        val rules = mapOf("key" to "value")
        assertFalse(properties.matchesAnyExclusion(rules))
    }

    @Test
    fun `matchesAnyExclusion matching key and value returns true`() {
        val properties: Map<String, Any?> = mapOf("key" to "value")
        val rules = mapOf("key" to "value")
        assertTrue(properties.matchesAnyExclusion(rules))
    }
}
