package com.iomete.backup.config.internal

import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory

private const val BYTES_PER_MB = 1024 * 1024

object SparkRuntime {
    private val logger = LoggerFactory.getLogger(SparkRuntime::class.java)

    fun bytesPerSecPerExecutor(
        capMbPerSec: Double,
        executors: Int,
    ): Double {
        val rate = capMbPerSec * BYTES_PER_MB / executors

        logger.info(
            "Bandwidth capped at {} MB/s across {} executor(s): {} bytes/s per executor",
            capMbPerSec,
            executors,
            rate.toLong(),
        )
        return rate
    }

    fun executorSetting(sparkConf: SparkConf): String =
        if (sparkConf.getBoolean("spark.dynamicAllocation.enabled", false)) {
            "spark.dynamicAllocation.maxExecutors"
        } else {
            "spark.executor.instances"
        }

    fun executorCount(sparkConf: SparkConf): Int? {
        if (sparkConf.get("spark.master", "").startsWith("local")) return 1

        return sparkConf.get(executorSetting(sparkConf), "").toIntOrNull()?.takeIf { it > 0 }
    }
}
