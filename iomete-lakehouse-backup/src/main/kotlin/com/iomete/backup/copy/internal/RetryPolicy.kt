package com.iomete.backup.copy.internal

import org.apache.hadoop.security.AccessControlException
import java.io.FileNotFoundException
import java.nio.file.AccessDeniedException

internal object RetryPolicy {
    const val DELAY_MS = 1000L
    const val COPY_MAX_ATTEMPTS = 3

    // More attempts than a copy: abandoning one file fails that file, while abandoning the listing
    // downgrades the whole run to a full copy.
    const val LISTING_MAX_ATTEMPTS = 5
}

// Retries every non-terminal failure, then rethrows the last one. onFailedAttempt fires for every
// failed attempt, terminal ones included.
internal fun <T> withRetries(
    maxAttempts: Int,
    retryDelayMs: Long,
    onFailedAttempt: (attempt: Int, e: Exception) -> Unit = { _, _ -> },
    block: (attempt: Int) -> T,
): T {
    for (attempt in 1..maxAttempts) {
        try {
            return block(attempt)
        } catch (e: Exception) {
            onFailedAttempt(attempt, e)

            if (isTerminal(e) || attempt == maxAttempts) throw e

            try {
                Thread.sleep(fullJitterDelayMs(attempt, retryDelayMs))
            } catch (_: InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            }
        }
    }

    error("withRetries needs maxAttempts of at least 1, got $maxAttempts")
}

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

internal fun isTerminal(e: Throwable): Boolean =
    e is InterruptedException ||
        e is FileNotFoundException ||
        e is AccessDeniedException ||
        e is AccessControlException ||
        e is IllegalArgumentException
