package com.iomete.cleanup.untrackedtablefolders.logging

import org.jboss.logging.Logger

// The job's only logger. Messages go through SafeText, and a Throwable never reaches the logging backend, which would
// print raw messages: WARN and ERROR get a bounded summary of the cause chain, and DEBUG gets the safe stack trace.
class SafeLogger private constructor(
    private val delegate: Logger,
) {
    val isDebugEnabled: Boolean get() = delegate.isDebugEnabled

    fun debug(
        message: String,
        error: Throwable? = null,
    ) = log(Logger.Level.DEBUG, message, error)

    fun info(
        message: String,
        error: Throwable? = null,
    ) = log(Logger.Level.INFO, message, error)

    fun warn(
        message: String,
        error: Throwable? = null,
    ) = log(Logger.Level.WARN, message, error)

    fun error(
        message: String,
        error: Throwable? = null,
    ) = log(Logger.Level.ERROR, message, error)

    // One statement for a whole report, so log viewers that sort by timestamp cannot interleave its lines.
    fun infoLines(lines: List<String>) {
        delegate.info(lines.joinToString("\n") { SafeText.text(it) })
    }

    private fun log(
        level: Logger.Level,
        message: String,
        error: Throwable?,
    ) {
        val text = SafeText.text(message)
        when {
            error == null -> emit(level, text)
            level == Logger.Level.DEBUG -> debugTrace(text, error)
            else -> {
                emit(level, "$text (${SafeText.describe(error)})")
                debugTrace(text, error)
            }
        }
    }

    private fun debugTrace(
        text: String,
        error: Throwable,
    ) {
        if (delegate.isDebugEnabled) {
            delegate.debug((listOf(text) + SafeText.trace(error)).joinToString("\n"))
        }
    }

    private fun emit(
        level: Logger.Level,
        text: String,
    ) {
        when (level) {
            Logger.Level.DEBUG -> delegate.debug(text)
            Logger.Level.INFO -> delegate.info(text)
            Logger.Level.WARN -> delegate.warn(text)
            Logger.Level.ERROR -> delegate.error(text)
            else -> delegate.log(level, text)
        }
    }

    companion object {
        fun getLogger(type: Class<*>): SafeLogger = SafeLogger(Logger.getLogger(type))
    }
}
