package com.iomete.cleanup.untrackedtablefolders.audit

import java.time.Duration
import java.time.Instant
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class AuditHistoryTest {
    private val listing = Instant.parse("2026-11-23T12:00:00Z")
    private val folder = "s3a://bucket/lake/db.db/orphan"
    private val fp = "fp1:aaaa"

    private fun hoursBefore(hours: Long): Instant = listing.minus(Duration.ofHours(hours))

    private fun row(
        runId: String?,
        endedHoursAgo: Long?,
        status: String = "SUCCESS",
        seen: Map<String, String>? = mapOf(folder to fp),
        deleted: List<String>? = emptyList(),
        prev: String? = null,
        candidates: List<String>? = emptyList(),
    ) = AuditHistoryRow(runId, status, endedHoursAgo?.let(::hoursBefore), candidates, deleted, seen, prev)

    private fun confirms(vararg rows: AuditHistoryRow): Boolean = AuditHistory.Read(rows.toList()).confirms(folder, fp, listing)

    @Test
    fun `a SUCCESS row at least 72 hours old that saw the folder unchanged confirms it`() {
        assertTrue(confirms(row("r1", 73)))
        assertTrue(confirms(row("r1", 72)))
        assertTrue(confirms(row("r1", 100, status = "SUCCESS", prev = "r0")))
    }

    @Test
    fun `a row younger than 72 hours, or no SUCCESS row, confirms nothing`() {
        assertFalse(confirms(row("r1", 71)))
        assertFalse(confirms(row("r1", 100, status = "BLOCKED")))
        assertFalse(confirms(row("r1", 100, status = "FAILED")))
        assertFalse(confirms())
    }

    @Test
    fun `later rows that saw the folder unchanged keep the confirmation`() {
        assertTrue(
            confirms(
                row("r3", 1, status = "BLOCKED", prev = "r2"),
                row("r2", 30, prev = "r1"),
                row("r1", 80),
            ),
        )
    }

    @Test
    fun `a later row that saw a change, did not see the folder or saw nothing resets the confirmation`() {
        assertFalse(confirms(row("r2", 1, seen = mapOf(folder to "fp1:bbbb"), prev = "r1"), row("r1", 80)))
        assertFalse(confirms(row("r2", 1, seen = mapOf("$folder-other" to fp), prev = "r1"), row("r1", 80)))
        assertFalse(confirms(row("r2", 1, status = "BLOCKED", seen = null, prev = "r1"), row("r1", 80)))
        assertFalse(confirms(row("r2", 1, status = "SKIPPED", seen = emptyMap(), prev = "r1"), row("r1", 80)))
        assertFalse(confirms(row("r1", 80, seen = mapOf(folder to "fp1:bbbb"))))
    }

    @Test
    fun `a folder a row deleted is never confirmed by older rows`() {
        assertFalse(confirms(row("r2", 1, status = "FAILED", deleted = listOf(folder), prev = "r1"), row("r1", 80)))
        assertFalse(confirms(row("r1", 80, deleted = listOf(folder))))
    }

    @Test
    fun `a missing or broken link between rows confirms nothing`() {
        assertFalse(confirms(row("r3", 1, prev = "r9"), row("r1", 80)))
        assertFalse(confirms(row("r2", 1, prev = null), row("r1", 80)))
        assertFalse(confirms(row(null, 80)))
        assertFalse(confirms(row("r2", null, prev = "r1"), row("r1", 80)))
        assertFalse(confirms(row("r1", 80, deleted = null)))
    }

    @Test
    fun `an intent row since the confirming row that names the folder confirms nothing`() {
        val intent = { endedHoursAgo: Long?, candidates: List<String>? ->
            row("r2", endedHoursAgo, status = AUDIT_STATUS_INTENT, seen = null, candidates = candidates)
        }
        assertFalse(confirms(intent(2, listOf(folder)), row("r1", 80)))
        assertFalse(confirms(intent(80, listOf(folder)), row("r1", 80)))
        assertFalse(confirms(intent(null, listOf(folder)), row("r1", 80)))
        assertFalse(confirms(intent(2, null), row("r1", 80)))
        assertTrue(confirms(intent(2, listOf("$folder-other")), row("r1", 80)))
        assertTrue(confirms(intent(90, listOf(folder)), row("r1", 80)))
    }

    @Test
    fun `the newest final row, not an intent, is the previous run`() {
        val history =
            AuditHistory.Read(
                listOf(row("r1", 80), row("r3", 1, status = AUDIT_STATUS_INTENT), row("r2", 5, prev = "r1")),
            )
        assertEquals("r2", history.newestRunId)
        assertEquals(null, AuditHistory.Read(emptyList()).newestRunId)
        assertEquals(null, AuditHistory.NoJobIdentity.newestRunId)
    }
}
