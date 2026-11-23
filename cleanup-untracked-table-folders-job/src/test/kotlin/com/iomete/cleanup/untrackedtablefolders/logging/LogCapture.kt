package com.iomete.cleanup.untrackedtablefolders.logging

import io.mockk.every
import io.mockk.mockkStatic
import io.mockk.unmockkStatic
import org.jboss.logging.Logger
import java.text.MessageFormat
import java.util.concurrent.CopyOnWriteArrayList

data class CapturedLog(
    val level: Logger.Level,
    val logger: String,
    val message: String,
    val thrown: Throwable?,
)

/** A logging backend with every level enabled, including DEBUG, that records what reaches it. */
class CapturingLogger(
    name: String,
    private val records: MutableList<CapturedLog>,
) : Logger(name) {
    override fun isEnabled(level: Level?): Boolean = true

    override fun doLog(
        level: Level,
        loggerClassName: String?,
        message: Any?,
        parameters: Array<out Any?>?,
        thrown: Throwable?,
    ) {
        val text = if (parameters.isNullOrEmpty()) message.toString() else MessageFormat.format(message.toString(), *parameters)
        records += CapturedLog(level, name, text, thrown)
    }

    override fun doLogf(
        level: Level,
        loggerClassName: String?,
        format: String,
        parameters: Array<out Any?>?,
        thrown: Throwable?,
    ) {
        records += CapturedLog(level, name, String.format(format, *parameters.orEmpty()), thrown)
    }
}

/** Every jboss Logger created inside [block] records into the list it receives. Create the objects under test inside. */
fun <T> capturingLogs(block: (List<CapturedLog>) -> T): T {
    val records = CopyOnWriteArrayList<CapturedLog>()
    mockkStatic(Logger::class)
    try {
        every { Logger.getLogger(any<Class<*>>()) } answers { CapturingLogger(firstArg<Class<*>>().name, records) }
        return block(records)
    } finally {
        unmockkStatic(Logger::class)
    }
}
