package com.iomete.backup.copy.internal

import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CyclicBarrier
import kotlin.test.assertEquals
import kotlin.test.assertSame

class RateLimiterTest {
    // 1000 bytes/s: a 100-byte chunk occupies 0.1 s.
    private val chunkNanos = 100L * 1000 * 1000

    @Test
    fun `a chunk is released once it has paid for itself`() {
        val limiter = RateLimiter(1000.0)

        assertEquals(chunkNanos, limiter.reserve(100, 0))
    }

    @Test
    fun `the next chunk is released one chunk later`() {
        val limiter = RateLimiter(1000.0)

        limiter.reserve(100, 0)

        assertEquals(2 * chunkNanos, limiter.reserve(100, 0))
    }

    @Test
    fun `an idle gap is not banked as credit`() {
        val limiter = RateLimiter(1000.0)

        limiter.reserve(100, 0)

        assertEquals(10 * chunkNanos + chunkNanos, limiter.reserve(100, 10 * chunkNanos))
    }

    @Test
    fun `concurrent threads share one budget rather than two`() {
        val limiter = RateLimiter(1000.0)
        val reservations = ConcurrentLinkedQueue<Long>()
        val barrier = CyclicBarrier(4)

        val threads =
            (1..4).map {
                Thread {
                    barrier.await()
                    reservations.add(limiter.reserve(100, 0))
                }.apply { start() }
            }
        threads.forEach { it.join() }

        assertEquals((1..4).map { it * chunkNanos }.toSet(), reservations.toSet())
    }

    @Test
    fun `every copy in a JVM draws on the same limiter`() {
        assertSame(RateLimiter.shared(1000.0), RateLimiter.shared(1000.0))
    }
}
