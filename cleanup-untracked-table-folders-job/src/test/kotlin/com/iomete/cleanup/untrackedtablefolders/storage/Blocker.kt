package com.iomete.cleanup.untrackedtablefolders.storage

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/** Holds every call that enters [block] until [release], and ignores interrupts, like a storage call that never returns. */
class Blocker {
    private val released = CountDownLatch(1)
    private val inside = AtomicInteger()
    val entered = AtomicInteger()
    val interrupted = AtomicBoolean()

    fun block() {
        entered.incrementAndGet()
        inside.incrementAndGet()
        try {
            while (true) {
                try {
                    released.await()
                    return
                } catch (e: InterruptedException) {
                    interrupted.set(true)
                }
            }
        } finally {
            inside.decrementAndGet()
        }
    }

    /** Lets every held call go on, and waits until none is still held. */
    fun release() {
        released.countDown()
        awaitUntil(Duration.ofSeconds(5)) { inside.get() == 0 }
    }
}

fun awaitUntil(
    limit: Duration,
    condition: () -> Boolean,
): Boolean {
    val deadline = System.nanoTime() + limit.toNanos()
    while (!condition()) {
        if (System.nanoTime() > deadline) return false
        Thread.sleep(5)
    }
    return true
}

fun elapsedSince(startedNanos: Long): Duration = Duration.ofNanos(System.nanoTime() - startedNanos)
