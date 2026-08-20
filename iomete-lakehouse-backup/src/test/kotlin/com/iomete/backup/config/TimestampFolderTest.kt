package com.iomete.backup.config

import org.junit.jupiter.api.Test
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class TimestampFolderTest {
    private val instant = Instant.parse("2026-02-14T09:35:12Z")

    @Test
    fun `each granularity has its own folder name`() {
        assertEquals("2026-02-14-09", TimestampFolder.folderName("hourly", instant))
        assertEquals("2026-02-14", TimestampFolder.folderName("daily", instant))
        assertEquals("2026-W07", TimestampFolder.folderName("weekly", instant))
        assertEquals("2026-02", TimestampFolder.folderName("monthly", instant))
    }

    @Test
    fun `instants on either side of a period boundary land in different folders`() {
        val before = Instant.parse("2026-02-15T23:59:59Z")
        val after = Instant.parse("2026-02-16T00:00:00Z")

        assertEquals("2026-02-15-23", TimestampFolder.folderName("hourly", before))
        assertEquals("2026-02-16-00", TimestampFolder.folderName("hourly", after))
        assertEquals("2026-02-15", TimestampFolder.folderName("daily", before))
        assertEquals("2026-02-16", TimestampFolder.folderName("daily", after))
        assertEquals("2026-W07", TimestampFolder.folderName("weekly", before))
        assertEquals("2026-W08", TimestampFolder.folderName("weekly", after))
    }

    @Test
    fun `the name is derived in UTC, not in the zone of the host`() {
        // 2026-03-01T01:00+09:00 is still the previous day, month and week in UTC.
        val instant = Instant.parse("2026-02-28T16:00:00Z")

        assertEquals("2026-02-28", TimestampFolder.folderName("daily", instant))
        assertEquals("2026-02", TimestampFolder.folderName("monthly", instant))
    }

    @Test
    fun `a week folder keeps the ISO week-based year across a year boundary`() {
        assertEquals("2026-W01", TimestampFolder.folderName("weekly", Instant.parse("2025-12-31T00:00:00Z")))
    }

    @Test
    fun `an unknown granularity is rejected`() {
        assertFailsWith<IllegalArgumentException> { TimestampFolder.folderName("yearly", instant) }
    }
}
