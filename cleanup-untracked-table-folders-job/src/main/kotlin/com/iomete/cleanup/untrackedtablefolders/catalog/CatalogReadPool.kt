package com.iomete.cleanup.untrackedtablefolders.catalog

import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import java.util.concurrent.Callable
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong

/** Threads for catalog reads that clients can serve at the same time. */
const val CATALOG_READ_PARALLELISM = 8

/**
 * A fixed number of daemon threads. [map] returns results in item order. Called from one of its own threads it runs
 * inline, so nested use can't wait on itself. A failure is rethrown after the items already running finish; queued
 * items are dropped, and no running call is interrupted, because an interrupted metastore call can leave its shared
 * client in a bad state.
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
        val futures = items.map { item -> executor().submit(Callable { work(item) }) }
        try {
            return futures.map { it.result() }
        } finally {
            futures.forEach { it.cancel(false) }
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
