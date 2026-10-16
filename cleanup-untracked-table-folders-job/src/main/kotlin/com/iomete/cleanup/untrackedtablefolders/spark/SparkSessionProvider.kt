package com.iomete.cleanup.untrackedtablefolders.spark

import jakarta.enterprise.context.ApplicationScoped
import org.apache.spark.sql.SparkSession
import org.jboss.logging.Logger

@ApplicationScoped
class SparkSessionProvider {
    private val logger = Logger.getLogger(SparkSessionProvider::class.java)

    fun getOrCreate(): SparkSession {
        if (!sessionExists()) {
            logger.info("Creating SparkSession for cleanup untracked table folders job")
        }

        return SparkSession.builder()
            .appName("cleanup-untracked-table-folders")
            .enableHiveSupport()
            .getOrCreate()
    }

    // The same check the builder uses before it returns an existing session instead of creating one.
    internal fun sessionExists(): Boolean =
        listOf(SparkSession.getActiveSession(), SparkSession.getDefaultSession())
            .any { it.isDefined && !it.get().sparkContext().isStopped }
}
