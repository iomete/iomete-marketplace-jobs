package com.iomete.backup

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionProvider {
    private val logger = LoggerFactory.getLogger(SparkSessionProvider::class.java)

    private var session: SparkSession? = null

    val sparkSession: SparkSession
        get() {
            return session ?: run {
                logger.info("Initializing Spark session")

                val s = SparkSession.builder().orCreate

                logger.info("Spark session initialized successfully")
                logger.info("  Application ID: {}", s.sparkContext().applicationId())
                logger.info("  Spark version: {}", s.version())

                session = s
                s
            }
        }

    fun stop() {
        val s =
            session ?: run {
                logger.info("Spark session was never initialized, nothing to stop")
                return
            }

        if (s.sparkContext().isStopped) {
            logger.info("Spark session already stopped")
            return
        }

        logger.info("Stopping Spark session...")
        s.stop()
        logger.info("Spark session stopped")
    }
}
