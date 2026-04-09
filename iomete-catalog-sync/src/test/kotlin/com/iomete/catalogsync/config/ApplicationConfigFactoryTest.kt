package com.iomete.catalogsync.config

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class ApplicationConfigFactoryTest {

    private val mapper = jacksonObjectMapper()

    @Test
    fun `2_1_1 - load valid JSON config`() {
        val json = """
        {
          "exclusion_rules": {
            "catalogs": {
              "names": ["sandbox_dev", "legacy_warehouse", "user_personal_spaces"],
              "filter_by_properties": {
                "iomete.governance.index": "false"
              }
            },
            "schemas": {
              "filter_by_properties": {
                "iomete.governance.index": "false"
              }
            },
            "tables": {
              "filter_by_properties": {
                "iomete.governance.index": "false",
                "hidden": "true"
              }
            }
          }
        }
        """.trimIndent()

        val config = mapper.readValue<ApplicationConfig>(json)

        val catalogs = config.exclusionRules.catalogs
        assertEquals(listOf("sandbox_dev", "legacy_warehouse", "user_personal_spaces"), catalogs.names)
        assertEquals(mapOf("iomete.governance.index" to "false"), catalogs.filterByProperties)

        assertEquals(mapOf("iomete.governance.index" to "false"), config.exclusionRules.schemas.filterByProperties)
        assertEquals(
            mapOf("iomete.governance.index" to "false", "hidden" to "true"),
            config.exclusionRules.tables.filterByProperties
        )
    }

    @Test
    fun `2_1_2 - file not found fallback returns defaults`() {
        val config = ApplicationConfig()

        assertEquals(emptyList<String>(), config.exclusionRules.catalogs.names)
        assertEquals(emptyMap<String, String>(), config.exclusionRules.catalogs.filterByProperties)
        assertEquals(emptyMap<String, String>(), config.exclusionRules.schemas.filterByProperties)
        assertEquals(emptyMap<String, String>(), config.exclusionRules.tables.filterByProperties)
        assertEquals(mapOf("iomete.governance.index" to "false"), config.exclusionRules.defaultRule.filterByProperties)
    }

    @Test
    fun `2_1_3 - malformed JSON throws exception and fallback has correct defaults`() {
        val malformedJson = """{ not valid json }"""

        assertThrows(JsonParseException::class.java) {
            mapper.readValue<ApplicationConfig>(malformedJson)
        }

        val fallback = ApplicationConfig()
        assertEquals(mapOf("iomete.governance.index" to "false"), fallback.exclusionRules.defaultRule.filterByProperties)
    }

    @Test
    fun `2_1_4 - partial config with missing fields uses defaults`() {
        val json = """
        {
          "exclusion_rules": {
            "catalogs": {
              "names": ["partial_catalog"],
              "filter_by_properties": {
                "env": "test"
              }
            }
          }
        }
        """.trimIndent()

        val config = mapper.readValue<ApplicationConfig>(json)

        assertEquals(listOf("partial_catalog"), config.exclusionRules.catalogs.names)
        assertEquals(mapOf("env" to "test"), config.exclusionRules.catalogs.filterByProperties)
        assertEquals(emptyMap<String, String>(), config.exclusionRules.schemas.filterByProperties)
        assertEquals(emptyMap<String, String>(), config.exclusionRules.tables.filterByProperties)
        assertEquals(mapOf("iomete.governance.index" to "false"), config.exclusionRules.defaultRule.filterByProperties)
    }

    @Test
    fun `2_1_5 - unknown JSON fields are ignored`() {
        val json = """
        {
          "comment": "this field should be ignored",
          "foo": "bar",
          "exclusion_rules": {
            "catalogs": {
              "names": ["cat1"]
            }
          }
        }
        """.trimIndent()

        val config = mapper.readValue<ApplicationConfig>(json)

        assertEquals(listOf("cat1"), config.exclusionRules.catalogs.names)
    }
}
