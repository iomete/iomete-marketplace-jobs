package com.iomete.cleanup.untrackedtablefolders.spark

import io.mockk.every
import io.mockk.mockk
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
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
}
