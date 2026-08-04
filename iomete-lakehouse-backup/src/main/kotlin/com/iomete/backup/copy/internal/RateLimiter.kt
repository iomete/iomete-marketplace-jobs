package com.iomete.backup.copy.internal

import java.util.concurrent.TimeUnit

private const val NANOS_PER_SECOND = 1000L * 1000 * 1000

internal class RateLimiter(
    val bytesPerSec: Double,
) {
    private var cursorNanos = Long.MIN_VALUE

    @Synchronized
    fun reserve(
        bytes: Long,
        nowNanos: Long,
    ): Long {
        cursorNanos = maxOf(cursorNanos, nowNanos) + (bytes / bytesPerSec * NANOS_PER_SECOND).toLong()
        return cursorNanos
    }

    fun acquire(bytes: Long) {
        val waitNanos = reserve(bytes, System.nanoTime()) - System.nanoTime()
        if (waitNanos > 0) TimeUnit.NANOSECONDS.sleep(waitNanos)
    }

    companion object {
        private var shared: RateLimiter? = null

        @Synchronized
        fun shared(bytesPerSec: Double): RateLimiter =
            shared?.takeIf { it.bytesPerSec == bytesPerSec } ?: RateLimiter(bytesPerSec).also { shared = it }
    }
}
