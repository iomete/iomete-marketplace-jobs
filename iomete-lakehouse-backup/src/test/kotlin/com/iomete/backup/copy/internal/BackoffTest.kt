package com.iomete.backup.copy.internal

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class BackoffTest {
    @Test
    fun `delay is zero when base is non-positive`() {
        assertEquals(0L, fullJitterDelayMs(attempt = 5, baseMs = 0L, rnd = 1.0))
        assertEquals(0L, fullJitterDelayMs(attempt = 5, baseMs = -10L, rnd = 1.0))
    }

    @Test
    fun `delay stays within full-jitter bound`() {
        // bound = min(cap, base * 2^attempt); rnd=1.0 hits the upper edge
        assertEquals(2000L, fullJitterDelayMs(attempt = 1, baseMs = 1000L, rnd = 1.0))
        assertEquals(4000L, fullJitterDelayMs(attempt = 2, baseMs = 1000L, rnd = 1.0))
        assertEquals(0L, fullJitterDelayMs(attempt = 3, baseMs = 1000L, rnd = 0.0))
    }

    @Test
    fun `delay is capped`() {
        // base 1000 * 2^10 = 1_024_000 > cap 30_000
        assertEquals(30_000L, fullJitterDelayMs(attempt = 10, baseMs = 1000L, capMs = 30_000L, rnd = 1.0))
    }

    @Test
    fun `random draws never exceed the bound`() {
        repeat(1000) {
            val d = fullJitterDelayMs(attempt = 4, baseMs = 1000L, capMs = 30_000L)
            assertTrue(d in 0L..16_000L, "delay $d out of bound")
        }
    }
}
