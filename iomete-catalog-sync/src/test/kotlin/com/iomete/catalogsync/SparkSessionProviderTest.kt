package com.iomete.catalogsync

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class SparkSessionProviderTest {
    @Test
    fun `session uses the stock Hive session state builder so Ranger rules do not hide objects`() {
        assertEquals(
            "org.apache.spark.sql.hive.HiveSessionStateBuilder",
            SparkSessionProvider.sessionConfig["spark.sql.hive.sessionStateBuilder"],
        )
    }
}
