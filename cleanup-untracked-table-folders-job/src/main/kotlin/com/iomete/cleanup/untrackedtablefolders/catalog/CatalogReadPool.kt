package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/** Threads for catalog reads that clients can serve at the same time. */
const val CATALOG_READ_PARALLELISM = 8

private val SKIPPED = Any()

/**
 * A fixed number of daemon threads. [map] returns results in item order. Called from one of its own threads it runs
 * inline, so nested use can't wait on itself. The first failure in item order is rethrown once every item already
 * started has finished, so no read is still running when [map] returns; items not yet started are skipped. No running
 * call is interrupted, because an interrupted metastore call can leave its shared client in a bad state. When the
 * caller is interrupted, it stops waiting at once.
 */
class CatalogReadPool(
    val parallelism: Int,
    private val threadName: String,
) : AutoCloseable {
    private val ownThread = ThreadLocal.withInitial { false }
    private val threadCount = AtomicInteger()

    @Volatile
    private var executor: ExecutorService? = null

    fun <T, R> map(
        items: List<T>,
        work: (T) -> R,
    ): List<R> {
        if (parallelism <= 1 || items.size <= 1 || ownThread.get()) {
            return items.map(work)
        }
        val stopped = AtomicBoolean()
        val futures = items.map { item -> executor().submit(Callable { if (stopped.get()) SKIPPED else work(item) }) }
        try {
            @Suppress("UNCHECKED_CAST")
            return futures.map { it.result() as R }
        } catch (e: InterruptedException) {
            stopped.set(true)
            Thread.currentThread().interrupt()
            throw e
        } catch (e: Throwable) {
            stopped.set(true)
            futures.forEach { runCatching { it.get() } }
            throw e
        }
    }

    private fun <R> Future<R>.result(): R =
        try {
            get()
        } catch (e: ExecutionException) {
            throw e.cause ?: e
        }

    @Synchronized
    private fun executor(): ExecutorService =
        executor ?: Executors.newFixedThreadPool(parallelism) { task ->
            Thread({
                ownThread.set(true)
                task.run()
            }, "$threadName-${threadCount.incrementAndGet()}").apply { isDaemon = true }
        }.also { executor = it }

    @Synchronized
    override fun close() {
        executor?.shutdown()
        executor = null
    }
}

/** Logs how far a long read is, at most once per [intervalMs]. Safe to call from several threads. */
class ReadProgress(
    private val what: String,
    private val total: Int,
    private val intervalMs: Long = 30_000,
    private val nanoTime: () -> Long = System::nanoTime,
) {
    private val logger = SafeLogger.getLogger(ReadProgress::class.java)
    private val startedAt = nanoTime()
    private val done = AtomicInteger()
    private val lastLoggedAt = AtomicLong(startedAt)

    fun itemDone() {
        val count = done.incrementAndGet()
        val now = nanoTime()
        val last = lastLoggedAt.get()
        if (count < total && now - last >= intervalMs * 1_000_000 && lastLoggedAt.compareAndSet(last, now)) {
            logger.info("$what: $count of $total done, ${(now - startedAt) / 1_000_000_000} s so far")
        }
    }
}
