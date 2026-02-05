package com.iomete.backup

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
 * Provider for SparkSession.
 * Creates and manages the Spark session for the backup job.
 */
object SparkSessionProvider {
    private val logger = LoggerFactory.getLogger(SparkSessionProvider::class.java)

    private const val APP_NAME = "iomete-lakehouse-backup"

    private var session: SparkSession? = null

    /**
     * Get or create the SparkSession.
     */
    val sparkSession: SparkSession
        get() {
            return session ?: run {
                logger.info("Initializing Spark session with app name: {}", APP_NAME)

                val s = SparkSession.builder()
                    .appName(APP_NAME)
                    .orCreate

                logger.info("Spark session initialized successfully")
                logger.info("  Application ID: {}", s.sparkContext().applicationId())
                logger.info("  Spark version: {}", s.version())

                session = s
                s
            }
        }

    /**
     * Stop the Spark session if it's running.
     */
    fun stop() {
        val s = session ?: run {
            logger.debug("Spark session was never initialized, nothing to stop")
            return
        }

        if (s.sparkContext().isStopped) {
            logger.debug("Spark session already stopped")
            return
        }

        logger.info("Stopping Spark session...")
        s.stop()
        logger.info("Spark session stopped")
    }
}
