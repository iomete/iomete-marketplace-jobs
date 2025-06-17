package com.iomete.catalogsync

import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import io.smallrye.config.ConfigMapping
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import java.util.*
import jakarta.inject.Singleton


@QuarkusMain
class App(
    private val sparkSessionProvider: SparkSessionProvider,
    private val lakehouseMetadataExtractor: LakehouseMetadataExtractor,
    private val applicationConfigExtractor: ApplicationConfigExtractor
) : QuarkusApplication {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun run(vararg args: String): Int {
        logger.info("Sync started...")
        val config = applicationConfigExtractor.load("/etc/configs/application.json")
        lakehouseMetadataExtractor.scrape(config)
        logger.info("Sync finished...")
        return 0
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) {
            Quarkus.run(App::class.java, "")
        }
    }
}

@Singleton
class SparkSessionProvider {
    // we couldn't make this exposed as a bean. Quarkus had a problem with it! This wrapping it with provider class
    val sparkSession: SparkSession = SparkSession.builder()
        .enableHiveSupport()
        // Disable ranger for catalog sync
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
        .orCreate
}

@ConfigMapping(prefix = "application")
interface ApplicationConfig {
    fun excludeSchemas(): Optional<Set<String>>
    fun catalogEndpoint(): String
}
