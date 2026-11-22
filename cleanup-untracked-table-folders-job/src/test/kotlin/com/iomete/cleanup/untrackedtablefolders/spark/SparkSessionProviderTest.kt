package com.iomete.cleanup.untrackedtablefolders.spark

import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import io.mockk.verify
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class SparkSessionProviderTest {

    private val provider = SparkSessionProvider()

    @AfterEach
    fun clearSessions() {
        SparkSession.clearActiveSession()
        SparkSession.clearDefaultSession()
    }

    @Test
    fun `a session counts as existing only while its context is running`() {
        assertFalse(provider.sessionExists())

        val existing = mockk<SparkSession>(relaxed = true)
        SparkSession.setDefaultSession(existing)
        assertTrue(provider.sessionExists())

        every { existing.sparkContext().isStopped } returns true
        assertFalse(provider.sessionExists())
    }

    @Test
    fun `the builder asks for no substitution and unescaped string literals`() {
        val builder = mockk<SparkSession.Builder>()
        every { builder.appName(any()) } returns builder
        every { builder.enableHiveSupport() } returns builder
        every { builder.config(any<String>(), any<String>()) } returns builder

        assertSame(builder, provider.configure(builder))

        verify { builder.config(VARIABLE_SUBSTITUTE, "false") }
        verify { builder.config(ESCAPED_STRING_LITERALS, "false") }
    }

    @Test
    fun `a session that does not keep a pin is refused`() {
        listOf(VARIABLE_SUBSTITUTE, ESCAPED_STRING_LITERALS).forEach { refusedKey ->
            val settings = mutableMapOf<String, String>()
            val conf = runtimeConfig(settings)
            every { conf.set(refusedKey, any<String>()) } answers { settings[refusedKey] = "true" }

            val error = assertThrows(IllegalStateException::class.java) { provider.pinParserSettings(sessionWith(conf)) }

            assertTrue(error.message.orEmpty().contains("$refusedKey=false"), error.message)
        }
    }

    @Test
    fun `the session getOrCreate returns is pinned, and one that does not keep a pin is refused before any SQL`() {
        val settings = mutableMapOf(VARIABLE_SUBSTITUTE to "true", ESCAPED_STRING_LITERALS to "true")
        val session = sessionWith(runtimeConfig(settings))
        val refusing = runtimeConfig(mutableMapOf())
        every { refusing.set(any<String>(), any<String>()) } answers { }
        every { refusing.get(any<String>()) } returns "true"
        val refused = sessionWith(refusing)
        val builder = mockk<SparkSession.Builder>()
        every { builder.appName(any()) } returns builder
        every { builder.enableHiveSupport() } returns builder
        every { builder.config(any<String>(), any<String>()) } returns builder
        every { builder.getOrCreate() } returns session andThen refused
        mockkStatic(SparkSession::class)
        try {
            every { SparkSession.builder() } returns builder

            assertSame(session, provider.getOrCreate())
            assertEquals(PINNED_PARSER_SETTINGS, settings)

            val error = assertThrows(IllegalStateException::class.java) { provider.getOrCreate() }
            assertTrue(error.message.orEmpty().contains("$VARIABLE_SUBSTITUTE=false"), error.message)
            verify(exactly = 0) { session.sql(any<String>()) }
            verify(exactly = 0) { refused.sql(any<String>()) }
        } finally {
            unmockkStatic(SparkSession::class)
        }
    }

    private fun runtimeConfig(settings: MutableMap<String, String>): RuntimeConfig =
        mockk<RuntimeConfig>().also { conf ->
            every { conf.set(any<String>(), any<String>()) } answers { settings[firstArg()] = secondArg() }
            every { conf.get(any<String>()) } answers { settings.getValue(firstArg()) }
        }

    private fun sessionWith(conf: RuntimeConfig): SparkSession = mockk<SparkSession>().also { every { it.conf() } returns conf }
}
