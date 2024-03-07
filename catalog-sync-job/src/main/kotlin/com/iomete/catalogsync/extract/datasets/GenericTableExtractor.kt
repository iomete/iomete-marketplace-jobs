package com.iomete.catalogsync.extract.datasets

import com.iomete.catalogsync.extract.TableExtractor
import org.apache.spark.sql.SparkSession

class GenericTableExtractor(
    spark: SparkSession,
    schema: String,
    tableName: String
) : TableExtractor {
    override val getTableType: String
        get() = "UNKNOWN"
}