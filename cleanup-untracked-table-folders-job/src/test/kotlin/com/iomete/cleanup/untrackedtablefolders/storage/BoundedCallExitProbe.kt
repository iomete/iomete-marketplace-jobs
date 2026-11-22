package com.iomete.cleanup.untrackedtablefolders.storage

import java.time.Duration

/** Started in its own JVM by BoundedCallsTest: main returns while a call it stopped waiting for still runs. */
object BoundedCallExitProbe {
    @JvmStatic
    fun main(args: Array<String>) {
        val result = BoundedCalls.run("probe", "nothing", Duration.ofMillis(100)) { Blocker().block() }
        println(if (result is BoundedCall.TimedOut) "TimedOut started=${result.error.started}" else "unexpected $result")
    }
}
