package com.iomete.cleanup.untrackedtablefolders.storage

import java.io.IOException
import java.time.Duration
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger

/** A monotonic time source in nanoseconds. Only the difference between two readings means anything. */
fun interface Ticker {
    fun nanos(): Long

    companion object {
        val SYSTEM: Ticker = Ticker { System.nanoTime() }
    }
}

sealed interface BoundedCall<out T> {
    data class Done<out T>(val value: T) : BoundedCall<T>

    data class Failed(val error: Throwable) : BoundedCall<Nothing>

    /** The job stopped waiting. The call itself may still be running, and a delete may still change storage. */
    data class TimedOut(val error: CallTimedOutException) : BoundedCall<Nothing>
}

class CallTimedOutException(
    val operation: String,
    val target: String,
    val waited: Duration,
    val started: Boolean,
) : IOException(
    if (started) {
        "$operation for $target did not finish within ${waited.toMillis()} ms; the job stopped waiting and the call may still complete"
    } else {
        "$operation for $target was not started because no time was left"
    },
)

/**
 * Runs blocking storage and catalog calls on worker threads and waits for each one at most a given time. Only the wait
 * is bounded: a call that times out keeps running in the background. So a block must only compute and return its
 * value. The caller applies a value it waited for and drops one it stopped waiting for.
 */
object BoundedCalls {
    private val threadCount = AtomicInteger()

    // Daemon threads, so a call that never returns cannot keep the JVM alive after the job ends.
    private val workers: ExecutorService =
        Executors.newCachedThreadPool { task ->
            Thread(task, "cleanup-bounded-call-${threadCount.incrementAndGet()}").apply { isDaemon = true }
        }

    /** With [wait] null the block runs on the caller's thread with no limit. */
    fun <T> run(
        operation: String,
        target: String,
        wait: Duration?,
        block: () -> T,
    ): BoundedCall<T> {
        if (wait == null) {
            return try {
                BoundedCall.Done(block())
            } catch (th: Throwable) {
                BoundedCall.Failed(th)
            }
        }
        if (wait <= Duration.ZERO) {
            return BoundedCall.TimedOut(CallTimedOutException(operation, target, Duration.ZERO, started = false))
        }

        val future = workers.submit(Callable { block() })
        return try {
            BoundedCall.Done(future.get(wait.toNanos(), TimeUnit.NANOSECONDS))
        } catch (th: ExecutionException) {
            BoundedCall.Failed(th.cause ?: th)
        } catch (th: TimeoutException) {
            // The interrupt may end some calls sooner. Nothing guarantees it.
            future.cancel(true)
            BoundedCall.TimedOut(CallTimedOutException(operation, target, wait, started = true))
        } catch (th: InterruptedException) {
            future.cancel(true)
            Thread.currentThread().interrupt()
            BoundedCall.TimedOut(CallTimedOutException(operation, target, wait, started = true))
        }
    }
}
