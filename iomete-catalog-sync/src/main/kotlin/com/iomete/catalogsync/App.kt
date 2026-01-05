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
    private val logger = LoggerFactory.getLogger(this::class.java)

    val sparkSession: SparkSession =
        SparkSession
            .builder()
            .enableHiveSupport()
            .config(extensionsConfig)
            .orCreate

    fun getSession(catalog: CoreClient.CatalogDetails): SparkSession =
        sessions.getOrPut(catalog.name) {
            createSparkSession(catalog)
        }

    private fun createSparkSession(catalog: CoreClient.CatalogDetails): SparkSession {
        if (sparkSession.conf().contains("spark.sql.catalog.${catalog.name}")) {
            logger.info("Using default session for catalog: {}", catalog.name)
            return sparkSession
        }

        logger.info("Initializing session for catalog: {}", catalog.name)
        val session: SparkSession =
            SparkSession
                .builder()
                .config(catalog.sparkProperties)
                .config(extensionsConfig)
                .orCreate

        session.catalog().setCurrentCatalog(catalog.name)
        return session
    }

    companion object {
        private val sessions: MutableMap<String, SparkSession> = mutableMapOf()
        private val extensionsConfig: Map<String, String> =
            mapOf(
                "spark.sql.extensions" to
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions," +
                    "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
            )
    }
}
