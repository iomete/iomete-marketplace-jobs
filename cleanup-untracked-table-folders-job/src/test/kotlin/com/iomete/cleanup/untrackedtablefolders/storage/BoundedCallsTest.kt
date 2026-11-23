package com.iomete.cleanup.untrackedtablefolders.storage

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import java.io.File
import java.io.IOException
import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

private val LIMIT: Duration = Duration.ofMillis(300)
private val MARGIN: Duration = Duration.ofSeconds(3)

class BoundedCallsTest {

    private val blocker = Blocker()

    @AfterEach
    fun tearDown() = blocker.release()

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a call that never returns is waited for only up to the limit, on a daemon thread`() {
        val daemon = AtomicReference<Boolean>()
        val started = System.nanoTime()

        val result =
            BoundedCalls.run("read storage object status", "location=s3a://bucket/db/t/a.parquet", LIMIT) {
                daemon.set(Thread.currentThread().isDaemon)
                blocker.block()
            }

        val waited = elapsedSince(started)
        val timedOut = assertInstanceOf(BoundedCall.TimedOut::class.java, result)
        assertTrue(waited >= LIMIT && waited < LIMIT + MARGIN, waited.toString())
        assertTrue(timedOut.error.started)
        assertEquals(
            "read storage object status for location=s3a://bucket/db/t/a.parquet did not finish within 300 ms; " +
                "the job stopped waiting and the call may still complete",
            timedOut.error.message,
        )
        assertEquals(true, daemon.get())
    }

    @Test
    @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a call that never returns does not keep the JVM from exiting`() {
        val classPath =
            listOf(BoundedCalls::class.java, BoundedCallExitProbe::class.java, Unit::class.java)
                .map { File(it.protectionDomain.codeSource.location.toURI()).path }
                .distinct()
                .joinToString(File.pathSeparator)
        val java = File(File(System.getProperty("java.home"), "bin"), "java").path
        val process = ProcessBuilder(java, "-cp", classPath, BoundedCallExitProbe::class.java.name).redirectErrorStream(true).start()

        val exited = process.waitFor(20, TimeUnit.SECONDS)
        if (!exited) process.destroyForcibly().waitFor(10, TimeUnit.SECONDS)
        val output = runCatching { process.inputStream.bufferedReader().readText() }.getOrDefault("")

        assertTrue(exited, "the JVM was still running 20 s after main returned: $output")
        assertEquals(0, process.exitValue(), output)
        assertEquals("TimedOut started=true", output.trim())
    }

    @Test
    @Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
    fun `a call the job stops waiting for is interrupted`() {
        val interrupted = CountDownLatch(1)

        val result =
            BoundedCalls.run("read", "location=x", LIMIT) {
                try {
                    CountDownLatch(1).await()
                } catch (e: InterruptedException) {
                    interrupted.countDown()
                }
            }

        assertInstanceOf(BoundedCall.TimedOut::class.java, result)
        assertTrue(interrupted.await(5, TimeUnit.SECONDS))
    }

    @ParameterizedTest
    @ValueSource(longs = [0, -1])
    fun `a call with no time left is never started`(waitMillis: Long) {
        val ran = CountDownLatch(1)

        val result = BoundedCalls.run("delete storage object", "location=x", Duration.ofMillis(waitMillis)) { ran.countDown() }

        val timedOut = assertInstanceOf(BoundedCall.TimedOut::class.java, result)
        assertFalse(timedOut.error.started)
        assertEquals("delete storage object for location=x was not started because no time was left", timedOut.error.message)
        assertFalse(ran.await(200, TimeUnit.MILLISECONDS))
    }

    @Test
    fun `a call that returns or fails in time gives its value or its own error`() {
        val error = IOException("simulated failure")

        assertEquals(BoundedCall.Done(42), BoundedCalls.run("read", "location=x", MARGIN) { 42 })
        assertSame(error, assertInstanceOf(BoundedCall.Failed::class.java, BoundedCalls.run("read", "location=x", MARGIN) { throw error }).error)
    }

    @Test
    fun `without a limit the call runs on the calling thread`() {
        val caller = Thread.currentThread()
        val ranOn = AtomicReference<Thread>()
        val error = IOException("simulated failure")

        assertEquals(BoundedCall.Done(1), BoundedCalls.run("read", "location=x", null) { ranOn.set(Thread.currentThread()); 1 })
        assertSame(caller, ranOn.get())
        assertSame(error, assertInstanceOf(BoundedCall.Failed::class.java, BoundedCalls.run("read", "location=x", null) { throw error }).error)
    }

    @Test
    fun `an interrupt of the waiting thread ends the wait and stays set`() {
        Thread.currentThread().interrupt()

        val result = BoundedCalls.run("read", "location=x", MARGIN) { blocker.block() }

        assertTrue(Thread.interrupted())
        assertTrue(assertInstanceOf(BoundedCall.TimedOut::class.java, result).error.started)
    }

    @Test
    fun `the system ticker reads the monotonic clock`() {
        val before = System.nanoTime()
        val reading = Ticker.SYSTEM.nanos()
        val after = System.nanoTime()

        assertTrue(reading in before..after, "$before $reading $after")
    }
}
