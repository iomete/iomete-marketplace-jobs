package com.iomete.catalogsync.config

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.File

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

        assertEquals(emptyList<String>(), config.inclusionRules.catalogs.names)
        assertEquals(emptyList<String>(), config.exclusionRules.catalogs.names)
        assertEquals(emptyMap<String, String>(), config.exclusionRules.catalogs.filterByProperties)
        assertEquals(emptyMap<String, String>(), config.exclusionRules.schemas.filterByProperties)
        assertEquals(emptyMap<String, String>(), config.exclusionRules.tables.filterByProperties)
        assertEquals(mapOf("iomete.governance.index" to "false"), config.exclusionRules.defaultRule.filterByProperties)
    }

    @Test
    fun `2_1_6 - load inclusion rules next to exclusion rules`() {
        val json = """
        {
          "inclusion_rules": {
            "catalogs": {
              "names": ["only_this_catalog"]
            }
          },
          "exclusion_rules": {
            "catalogs": {
              "names": ["cat1"]
            }
          }
        }
        """.trimIndent()

        val config = mapper.readValue<ApplicationConfig>(json)

        assertEquals(listOf("only_this_catalog"), config.inclusionRules.catalogs.names)
        assertEquals(listOf("cat1"), config.exclusionRules.catalogs.names)
    }

    @Test
    fun `2_1_3 - malformed JSON throws on parse`() {
        val malformedJson = """{ not valid json }"""

        assertThrows(JsonParseException::class.java) {
            mapper.readValue<ApplicationConfig>(malformedJson)
        }
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

    @Test
    fun `2_1_7 - factory fails on a config file it cannot parse`(@TempDir dir: File) {
        val file = File(dir, "application.json").apply { writeText("""{ not valid json }""") }

        assertThrows(IllegalStateException::class.java) {
            ApplicationConfigFactory().load(file)
        }
    }

    @Test
    fun `2_1_8 - factory fails on an unknown key inside a rule`(@TempDir dir: File) {
        val file = File(dir, "application.json").apply {
            writeText("""{"inclusion_rules": {"catalogs": {"name": ["only_this_catalog"]}}}""")
        }

        assertThrows(IllegalStateException::class.java) {
            ApplicationConfigFactory().load(file)
        }
    }

    @Test
    fun `2_1_9 - factory reads a valid config file and defaults a missing one`(@TempDir dir: File) {
        val file = File(dir, "application.json").apply {
            writeText("""{"inclusion_rules": {"catalogs": {"names": ["only_this_catalog"]}}}""")
        }

        assertEquals(listOf("only_this_catalog"), ApplicationConfigFactory().load(file).inclusionRules.catalogs.names)
        assertEquals(ApplicationConfig(), ApplicationConfigFactory().load(File(dir, "missing.json")))
    }

    @Test
    fun `2_1_10 - factory treats an empty config file like a missing one`(@TempDir dir: File) {
        val file = File(dir, "application.json").apply { writeText("  \n") }

        assertEquals(ApplicationConfig(), ApplicationConfigFactory().load(file))
    }
}
