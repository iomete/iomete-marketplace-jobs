package com.iomete.backup.stats

import com.iomete.backup.config.ApplicationConfig
import com.iomete.backup.config.S3Config
import com.iomete.backup.config.StatsConfig
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.spark.sql.RuntimeConfig
import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Test
import java.time.Instant

/** A catalog problem is the backup's problem only if it is allowed to become one. It is not. */
class StatsRecorderTest {
    private val config =
        ApplicationConfig(
            source = S3Config(bucket = "src", accessKey = "k", secretKey = "s"),
            target = S3Config(bucket = "dst", accessKey = "k", secretKey = "s"),
        )

    private fun sparkThatFails(): SparkSession {
        val runtimeConfig = mockk<RuntimeConfig>()
        every { runtimeConfig.get("spark.app.name") } returns "run-1"

        val spark = mockk<SparkSession>()
        every { spark.conf() } returns runtimeConfig
        every { spark.sql(any<String>()) } throws IllegalStateException("catalog is down")
        every { spark.createDataFrame(any<List<*>>(), any()) } throws IllegalStateException("catalog is down")

        return spark
    }

    @Test
    fun `a stats write that throws is swallowed`() {
        val spark = sparkThatFails()
        val recorder = StatsRecorder(spark, config)

        recorder.claim(Instant.now())
        recorder.finalise(Instant.now(), RunProgress(), null)

        verify(atLeast = 1) { spark.sql(any<String>()) }
    }

    @Test
    fun `recording disabled touches no table`() {
        val spark = sparkThatFails()
        val recorder = StatsRecorder(spark, config.copy(stats = StatsConfig(enabled = false)))

        recorder.claim(Instant.now())
        recorder.finalise(Instant.now(), RunProgress(), RuntimeException("boom"))

        verify(exactly = 0) { spark.sql(any<String>()) }
    }
}
