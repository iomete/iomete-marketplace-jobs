package com.iomete.catalogsync.integration

import org.apache.spark.sql.SparkSession

object TestDataSetup {
    fun createTestData(spark: SparkSession) {
        createTestDbTables(spark)
        createAnotherDbTables(spark)
    }

    private fun createTestDbTables(spark: SparkSession) {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS test_catalog.test_db")

        // users table: 7 columns, 5 rows across 2 inserts = 2 snapshots
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS test_catalog.test_db.users (
                id BIGINT,
                name STRING,
                age INT,
                salary DOUBLE,
                is_active BOOLEAN,
                created_at TIMESTAMP,
                email STRING
            ) USING iceberg
            """.trimIndent()
        )
        spark.sql(
            """
            INSERT INTO test_catalog.test_db.users VALUES
                (1, 'Alice', 30, 75000.0, true, TIMESTAMP '2024-01-15 10:30:00', 'alice@example.com'),
                (2, 'Bob', 25, 65000.0, true, TIMESTAMP '2024-02-20 14:00:00', 'bob@example.com'),
                (3, 'Charlie', 35, 85000.0, false, TIMESTAMP '2024-03-10 09:15:00', 'charlie@example.com')
            """.trimIndent()
        )
        spark.sql(
            """
            INSERT INTO test_catalog.test_db.users VALUES
                (4, 'Diana', 28, 70000.0, true, TIMESTAMP '2024-04-05 16:45:00', 'diana@example.com'),
                (5, 'Eve', 32, 90000.0, true, TIMESTAMP '2024-05-12 11:00:00', 'eve@example.com')
            """.trimIndent()
        )

        // events table: partitioned by event_date, 2 rows
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS test_catalog.test_db.events (
                event_id BIGINT,
                event_name STRING,
                event_date STRING
            ) USING iceberg
            PARTITIONED BY (event_date)
            """.trimIndent()
        )
        spark.sql(
            """
            INSERT INTO test_catalog.test_db.events VALUES
                (1, 'login', '2024-01-15'),
                (2, 'purchase', '2024-01-16')
            """.trimIndent()
        )

        // empty_table: no data (edge case)
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS test_catalog.test_db.empty_table (
                id BIGINT,
                value STRING
            ) USING iceberg
            """.trimIndent()
        )
    }

    private fun createAnotherDbTables(spark: SparkSession) {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS test_catalog.another_db")

        // products table: includes DECIMAL column, 2 rows
        spark.sql(
            """
            CREATE TABLE IF NOT EXISTS test_catalog.another_db.products (
                product_id BIGINT,
                product_name STRING,
                price DECIMAL(10, 2),
                quantity INT
            ) USING iceberg
            """.trimIndent()
        )
        spark.sql(
            """
            INSERT INTO test_catalog.another_db.products VALUES
                (1, 'Widget', 19.99, 100),
                (2, 'Gadget', 49.99, 50)
            """.trimIndent()
        )
    }
}
