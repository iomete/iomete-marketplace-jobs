package com.iomete.backup.config.internal

import com.iomete.backup.config.ConfigValidationException
import org.apache.spark.SparkConf
import org.slf4j.LoggerFactory
import kotlin.math.ceil

private const val BYTES_PER_MB = 1024 * 1024

object SparkRuntime {
    const val LIMIT_CORES = "spark.kubernetes.executor.limit.cores"
    const val EXECUTOR_CORES = "spark.executor.cores"

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
        if (isLocal(sparkConf)) return 1

        return sparkConf.get(executorSetting(sparkConf), "").toIntOrNull()?.takeIf { it > 0 }
    }

    // A Kubernetes quantity: either a plain core count or millicores such as "2000m".
    fun vcpuPerExecutor(sparkConf: SparkConf): Double {
        if (isLocal(sparkConf)) return 1.0

        val raw = sparkConf.get(LIMIT_CORES, "").trim()
        val cores =
            if (raw.endsWith("m")) raw.dropLast(1).toDoubleOrNull()?.div(1000) else raw.toDoubleOrNull()

        return cores?.takeIf { it > 0 }
            ?: throw ConfigValidationException(
                listOf(
                    "spark: $LIMIT_CORES must be set to a positive core count or millicores such as \"2000m\" " +
                        "(got \"$raw\")",
                ),
            )
    }

    fun slotsPerExecutor(
        sparkConf: SparkConf,
        slotsPerVcpu: Int,
    ): Int = ceil(vcpuPerExecutor(sparkConf) * slotsPerVcpu).toInt().coerceAtLeast(1)

    private fun isLocal(sparkConf: SparkConf): Boolean = sparkConf.get("spark.master", "").startsWith("local")
}
