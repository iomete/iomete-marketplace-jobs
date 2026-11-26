package com.iomete.cleanup.untrackedtablefolders.catalog

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTimeoutPreemptively
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.function.ThrowingSupplier

class CatalogReadPoolTest {
    private val pools = mutableListOf<CatalogReadPool>()

    @AfterEach
    fun close() = pools.forEach { it.close() }

    private fun pool(parallelism: Int) = CatalogReadPool(parallelism, "test-read").also { pools += it }

    @Test
    fun `results come back in item order whatever order the reads finish in`() {
        val items = (1..20).toList()

        val results = pool(8).map(items) { item ->
            Thread.sleep((20L - item) * 2)
            item * 10
        }

        assertEquals(items.map { it * 10 }, results)
    }

    @Test
    fun `no more reads run at once than the pool allows`() {
        val running = AtomicInteger()
        val peak = AtomicInteger()

        pool(4).map((1..60).toList()) {
            peak.accumulateAndGet(running.incrementAndGet(), ::maxOf)
            Thread.sleep(5)
            running.decrementAndGet()
        }

        assertTrue(peak.get() in 2..4, "peak=${peak.get()}")
    }

    @Test
    fun `a read that uses the pool again runs inline, so a full pool can't wait on itself`() {
        val pool = pool(2)

        val results =
            assertTimeoutPreemptively(
                Duration.ofSeconds(10),
                ThrowingSupplier { pool.map((1..4).toList()) { outer -> pool.map((1..3).toList()) { inner -> outer * inner } } },
            )

        assertEquals((1..4).map { outer -> (1..3).map { outer * it } }, results)
    }

    @Test
    fun `the failure of the first failing item in order is rethrown as it was thrown`() {
        val error =
            assertThrows<IllegalStateException> {
                pool(8).map((0..9).toList()) { item ->
                    when (item) {
                        3 -> {
                            Thread.sleep(100)
                            throw IllegalStateException("item 3")
                        }
                        7 -> throw IllegalArgumentException("item 7")
                        else -> item
                    }
                }
            }

        assertEquals("item 3", error.message)
    }

    @Test
    fun `a failure is rethrown only after the items already running finish, and items not started are skipped`() {
        val slowFinished = java.util.concurrent.atomic.AtomicBoolean()
        val started = AtomicInteger()

        assertThrows<IllegalStateException> {
            pool(2).map((0..9).toList()) { item ->
                when (item) {
                    0 -> throw IllegalStateException("item 0")
                    1 -> {
                        Thread.sleep(200)
                        slowFinished.set(true)
                    }
                    else -> {
                        started.incrementAndGet()
                        Thread.sleep(50)
                    }
                }
            }
        }

        assertTrue(slowFinished.get(), "returned while item 1 was still running")
        assertTrue(started.get() < 8, "started=${started.get()}")
    }

    @Test
    fun `with one thread, or one item, reads run on the calling thread`() {
        val caller = Thread.currentThread()

        assertEquals(listOf(caller, caller), pool(1).map(listOf(1, 2)) { Thread.currentThread() })
        assertEquals(listOf(caller), pool(8).map(listOf(1)) { Thread.currentThread() })
    }
}
