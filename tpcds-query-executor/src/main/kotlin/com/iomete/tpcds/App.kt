package com.iomete.tpcds

import io.quarkus.runtime.Quarkus
import io.quarkus.runtime.QuarkusApplication
import io.quarkus.runtime.annotations.QuarkusMain
import io.smallrye.config.ConfigMapping
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import jakarta.inject.Singleton
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.io.BufferedReader
import java.io.InputStreamReader
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant


@QuarkusMain
class App(
    private val sparkSessionProvider: SparkSessionProvider,
    private val queryRunner: QueryRunner
) : QuarkusApplication {
    private val logger = LoggerFactory.getLogger(this::class.java)

    override fun run(vararg args: String): Int {
        logger.info("Process started...")
        queryRunner.run()
        logger.info("Process finished...")
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
class QueryRunner(
    private val sparkSessionProvider: SparkSessionProvider,
    private val applicationConfig: ApplicationConfig
) {
    private val logger = LoggerFactory.getLogger(this::class.java)

    fun run() {
        val sparkSession = sparkSessionProvider.sparkSession
        val queryId = applicationConfig.queryId()
        val resultTable = applicationConfig.resultTable()
        val cpu = applicationConfig.cpu()
        val memory = applicationConfig.memory()
        val attempt = applicationConfig.attempt()
        val flags = applicationConfig.flags()
        val db = applicationConfig.db()

        logger.info("Running db: $db")
        logger.info("Running query: $queryId")
        logger.info("Result table: $resultTable")
        logger.info("CPU: $cpu")
        logger.info("Memory: $memory")
        logger.info("Attempt: $attempt")
        logger.info("Flags: $flags")

        // Read SQL file content
        val query = readFileFromResources("queries/$queryId")

        sparkSession.sql("USE $db")
        // Start timing
        val startTime = Instant.now()

        // Execute SQL query
        val dataset: Dataset<Row> = sparkSession.sql(query)

        // Time execution completion
        val executionTime = Instant.now().epochSecond - startTime.epochSecond

        dataset.collectAsList() // Dummy iteration to force reading

        // Finish timing the whole process
        val totalTime = Instant.now().epochSecond - startTime.epochSecond

        val insertQuery = """
            INSERT INTO $resultTable (query_id, time_in_seconds, execution_time_in_seconds, cpu, memory, attempt, flags)
            VALUES ('$queryId', $totalTime, $executionTime, '$cpu', '$memory', $attempt, '$flags')
        """.trimIndent()

        sparkSession.sql(insertQuery)
    }

    private fun readFileFromResources(filePath: String): String {
        val resource = javaClass.classLoader.getResourceAsStream(filePath)
        return if (resource != null) {
            BufferedReader(InputStreamReader(resource)).use { it.readText() }
        } else {
            throw IllegalArgumentException("File not found! $filePath")
        }
    }
}


@Singleton
class SparkSessionProvider {
    // we couldn't make this exposed as a bean. Quarkus had a problem with it! This wrapping it with provider class
    val sparkSession: SparkSession = SparkSession.builder()
        .appName("TPCDS Query Executor")
        .enableHiveSupport()
        .orCreate
}

@ConfigMapping(prefix = "application")
interface ApplicationConfig {
    fun db(): String
    fun queryId(): String // Example: q1.sql It should read sql file content from resources/queries folder
    fun resultTable(): String

    fun cpu(): String
    fun memory(): String
    fun attempt(): Int
    fun flags(): String
}
