package com.iomete.catalogsync

import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

@QuarkusMain
class App : QuarkusApplication {
    private val logger = LoggerFactory.getLogger(this::class.java)

    @Inject
    private lateinit var metadataScraper: MetadataScraper

    override fun run(vararg args: String): Int {
        logger.info("Sync started...")
        metadataScraper.run()
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
    val sparkSession: SparkSession =
        SparkSession
            .builder()
            .enableHiveSupport()
            // Disable ranger for catalog sync
            .config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            ).orCreate
}
