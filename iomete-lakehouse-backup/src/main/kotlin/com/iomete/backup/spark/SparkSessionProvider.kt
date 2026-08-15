package com.iomete.backup.spark

import com.iomete.backup.config.internal.SparkRuntime
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object SparkSessionProvider {
    private val logger = LoggerFactory.getLogger(SparkSessionProvider::class.java)

    private var session: SparkSession? = null

    fun sparkSession(slotsPerVcpu: Int): SparkSession =
        session ?: run {
            logger.info("Initializing Spark session")

            val builder = SparkSession.builder()

            builder.config(SparkRuntime.EXECUTOR_CORES, executorCores(slotsPerVcpu).toString())

            val s = builder.orCreate

            logger.info(
                "Spark session ready (applicationId={}, version={})",
                s.sparkContext().applicationId(),
                s.version(),
            )

            session = s
            s
        }

    private fun executorCores(slotsPerVcpu: Int): Int {
        val submitted = SparkConf()
        val slots = SparkRuntime.slotsPerExecutor(submitted, slotsPerVcpu)

        logger.info(
            "Executor concurrency: {} vCPU x slotsPerVcpu {} = {} slots per executor",
            SparkRuntime.vcpuPerExecutor(submitted),
            slotsPerVcpu,
            slots,
        )

        return slots
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
