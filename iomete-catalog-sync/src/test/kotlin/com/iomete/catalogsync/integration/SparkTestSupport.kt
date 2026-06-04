package com.iomete.catalogsync.integration

import org.apache.spark.sql.SparkSession
import java.io.File
import java.nio.file.Files

object SparkTestSupport {
    private var spark: SparkSession? = null
    private var warehouseDir: File? = null

    fun getOrCreateSpark(): SparkSession {
        if (spark != null) return spark!!

        val tmpDir = Files.createTempDirectory("iceberg-warehouse").toFile()
        warehouseDir = tmpDir

        spark = SparkSession.builder()
            .master("local[*]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.catalogImplementation", "in-memory")
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.test_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.test_catalog.type", "hadoop")
            .config("spark.sql.catalog.test_catalog.warehouse", tmpDir.absolutePath)
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()

        return spark!!
    }

    fun cleanup() {
        spark?.stop()
        spark = null
        warehouseDir?.deleteRecursively()
        warehouseDir = null
    }
}
