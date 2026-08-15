package com.iomete.backup

import com.iomete.backup.config.ConfigLoader
import com.iomete.backup.spark.SparkSessionProvider
import org.slf4j.LoggerFactory

object App {
    private val logger = LoggerFactory.getLogger(App::class.java)

    private const val DEFAULT_CONFIG_PATH = "/etc/configs/application.json"

    @JvmStatic
    fun main(args: Array<String>) {
        logger.info("IOMETE Lakehouse Backup - starting")

        try {
            val configPath = args.getOrNull(0) ?: DEFAULT_CONFIG_PATH
            logger.info("Configuration path: {}", configPath)

            val config = ConfigLoader.load(configPath)
            val spark = SparkSessionProvider.sparkSession(config.copy.slotsPerVcpu)

            try {
                val internalConfig = ConfigLoader.loadInternalConfig(config, spark.sparkContext().getConf())
                BackupJob.run(spark, config, internalConfig)
            } finally {
                SparkSessionProvider.stop()
            }

            logger.info("IOMETE Lakehouse Backup - completed successfully")
        } catch (e: Exception) {
            logger.error("IOMETE Lakehouse Backup - failed: {}", e.message, e)
            throw e
        }
    }
}
