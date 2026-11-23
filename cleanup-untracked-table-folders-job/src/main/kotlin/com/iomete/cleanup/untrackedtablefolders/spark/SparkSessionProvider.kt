package com.iomete.cleanup.untrackedtablefolders.spark

import com.iomete.cleanup.untrackedtablefolders.logging.SafeLogger
import jakarta.enterprise.context.ApplicationScoped
import org.apache.spark.sql.SparkSession

@ApplicationScoped
class SparkSessionProvider {
    private val logger = SafeLogger.getLogger(SparkSessionProvider::class.java)

    fun getOrCreate(): SparkSession {
        if (!sessionExists()) {
            logger.info("Creating SparkSession for cleanup untracked table folders job")
        }

        return pinParserSettings(configure(SparkSession.builder()).getOrCreate())
    }

    internal fun configure(builder: SparkSession.Builder): SparkSession.Builder =
        PARSER_SETTINGS.entries.fold(builder.appName("cleanup-untracked-table-folders").enableHiveSupport()) { configured, (key, value) ->
            configured.config(key, value)
        }

    // Set and read back on every use, because the session may have been created or changed elsewhere. A setting that
    // does not hold stops the job before it sends SQL text.
    internal fun pinParserSettings(session: SparkSession): SparkSession {
        val conf = session.conf()
        PARSER_SETTINGS.forEach { (key, value) ->
            conf.set(key, value)
            check(conf.get(key).trim().equals(value, ignoreCase = true)) {
                "The Spark session does not keep $key=$value, so the job's SQL text could change before it runs"
            }
        }
        return session
    }

    // The same check the builder uses before it returns an existing session instead of creating one.
    internal fun sessionExists(): Boolean =
        listOf(SparkSession.getActiveSession(), SparkSession.getDefaultSession())
            .any { it.isDefined && !it.get().sparkContext().isStopped }

    private companion object {
        // With substitution on, ${...} in a name or value is replaced before parsing. With escaped literals on, the
        // escapes in the audit values are stored as written.
        val PARSER_SETTINGS =
            mapOf(
                "spark.sql.variable.substitute" to "false",
                "spark.sql.parser.escapedStringLiterals" to "false",
            )
    }
}
