package com.iomete.backup.copy.internal

internal fun fullJitterDelayMs(
    attempt: Int,
    baseMs: Long,
    capMs: Long = 30_000L,
    rnd: Double = Math.random(),
): Long {
    if (baseMs <= 0L || attempt <= 0) return 0L
    val exp = if (attempt >= 32) capMs else minOf(capMs, baseMs shl attempt)
    return (rnd.coerceIn(0.0, 1.0) * exp).toLong()
}
