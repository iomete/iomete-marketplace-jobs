package com.iomete.catalogsync.extract

import com.iomete.catalogsync.SparkSessionProvider
import com.iomete.catalogsync.extract.datasets.DatasourceV1LikeTableExtractor
import com.iomete.catalogsync.extract.datasets.GenericTableExtractor
import com.iomete.catalogsync.extract.datasets.IcebergTableExtractor
import com.iomete.catalogsync.extract.datasets.ViewExtractor
import jakarta.inject.Singleton

@Singleton
class TableExtractorFactory(
    sparkSessionProvider: SparkSessionProvider,
) {
    private val spark = sparkSessionProvider.sparkSession

    fun extractorFor(
        provider: String,
        isView: Boolean = false,
        catalog: String,
        schema: String,
        table: String,
    ): TableExtractor {
        if (isView) return ViewExtractor(catalog = catalog, schema = schema, table = table)

        return when (provider) {
            "iceberg" -> {
                IcebergTableExtractor(
                    spark = spark,
                    catalog = catalog,
                    schema = schema,
                    table = table,
                )
            }

            "parquet", "orc", "hive" -> {
                DatasourceV1LikeTableExtractor(
                    spark = spark,
                    schema = schema,
                    tableName = table,
                )
            }

            else -> {
                GenericTableExtractor(spark = spark, catalog = catalog, schema = schema, tableName = table)
            }
        }
    }
}
