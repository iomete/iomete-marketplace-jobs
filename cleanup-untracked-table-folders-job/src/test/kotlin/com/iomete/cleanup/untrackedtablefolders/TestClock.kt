package com.iomete.cleanup.untrackedtablefolders

import com.iomete.cleanup.untrackedtablefolders.storage.Ticker
import java.time.Duration

/** A monotonic clock that moves only when a test moves it. */
class TestClock(
    @Volatile private var nanos: Long = 0L,
) : Ticker {
    override fun nanos(): Long = nanos

    fun advance(duration: Duration) {
        nanos += duration.toNanos()
    }
}
