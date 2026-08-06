package com.iomete.backup.copy.internal

import com.iomete.backup.copy.ExecutorTimings
import org.apache.spark.SparkContext
import org.apache.spark.util.LongAccumulator
import java.io.Serializable
import java.util.concurrent.TimeUnit

class CopyTimers(
    val copyTask: LongAccumulator,
    val fsInit: LongAccumulator,
    val sourceRead: LongAccumulator,
    val targetWrite: LongAccumulator,
    val throttleWait: LongAccumulator,
    val verify: LongAccumulator,
    val commit: LongAccumulator,
    val retrySleep: LongAccumulator,
) : Serializable {
    fun snapshot(): ExecutorTimings =
        ExecutorTimings(
            copyTaskMs = copyTask.value().toMillis(),
            fsInitMs = fsInit.value().toMillis(),
            sourceReadMs = sourceRead.value().toMillis(),
            targetWriteMs = targetWrite.value().toMillis(),
            throttleWaitMs = throttleWait.value().toMillis(),
            verifyMs = verify.value().toMillis(),
            commitMs = commit.value().toMillis(),
            retrySleepMs = retrySleep.value().toMillis(),
        )

    private fun Long.toMillis(): Long = TimeUnit.NANOSECONDS.toMillis(this)

    companion object {
        fun register(sc: SparkContext): CopyTimers =
            CopyTimers(
                copyTask = sc.longAccumulator("copyTaskNanos"),
                fsInit = sc.longAccumulator("fsInitNanos"),
                sourceRead = sc.longAccumulator("sourceReadNanos"),
                targetWrite = sc.longAccumulator("targetWriteNanos"),
                throttleWait = sc.longAccumulator("throttleWaitNanos"),
                verify = sc.longAccumulator("verifyNanos"),
                commit = sc.longAccumulator("commitNanos"),
                retrySleep = sc.longAccumulator("retrySleepNanos"),
            )

        /** Test seam only: serializing an unregistered accumulator into a task closure throws, so it cannot cross to an executor. */
        fun unregistered(): CopyTimers =
            CopyTimers(
                copyTask = LongAccumulator(),
                fsInit = LongAccumulator(),
                sourceRead = LongAccumulator(),
                targetWrite = LongAccumulator(),
                throttleWait = LongAccumulator(),
                verify = LongAccumulator(),
                commit = LongAccumulator(),
                retrySleep = LongAccumulator(),
            )
    }
}

internal inline fun <T> LongAccumulator.timeNanos(block: () -> T): T {
    val start = System.nanoTime()
    try {
        return block()
    } finally {
        add(System.nanoTime() - start)
    }
}
